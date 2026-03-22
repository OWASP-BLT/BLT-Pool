import json
import re
from urllib.parse import quote
from typing import Optional

from js import console, fetch
from core.github_client import github_api, create_comment, _is_human, _is_bot, _gh_headers, _ensure_label_exists
from models.leaderboard import _track_pr_opened_in_d1, _track_pr_closed_in_d1, _track_review_in_d1, _fetch_leaderboard_data
from views.comments import _post_or_update_leaderboard, _format_leaderboard_comment, _format_reviewer_leaderboard_comment, _check_and_close_excess_prs, MERGED_PR_COMMENT_MARKER, LEADERBOARD_MARKER, REVIEWER_LEADERBOARD_MARKER
from controllers.peer_review import get_valid_reviewers
from models.mentor import _fetch_mentors_config, _find_assigned_mentor_from_comments, MENTOR_ASSIGNED_LABEL

MENTOR_AUTO_PR_REVIEWER_ENABLED = False


async def handle_pull_request_opened(payload: dict, token: str, env=None) -> None:
    pr = payload["pull_request"]
    sender = payload["sender"]
    if not _is_human(sender):
        return

    # Skip bots more thoroughly
    if _is_bot(sender):
        return

    owner = payload["repository"]["owner"]["login"]
    repo = payload["repository"]["name"]
    pr_number = pr["number"]
    author_login = sender["login"]

    # Check for too many open PRs and auto-close if needed
    was_closed = await _check_and_close_excess_prs(owner, repo, pr_number, author_login, token)
    if was_closed:
        return  # Stop further processing if auto-closed

    # Track open PR counter in D1.
    await _track_pr_opened_in_d1(payload, env)

    # Post leaderboard
    if env is None:
        await _post_or_update_leaderboard(owner, repo, pr_number, author_login, token)
    else:
        await _post_or_update_leaderboard(owner, repo, pr_number, author_login, token, env)

    # If this PR is linked to a mentored issue, request the mentor as a reviewer.
    try:
        await _request_mentor_reviewer_for_pr(owner, repo, pr, token)
    except Exception as exc:
        console.error(f"[MentorPool] Mentor reviewer request failed (best-effort): {exc}")

    # When MENTOR_AUTO_PR_REVIEWER_ENABLED is True (either via the module
    # constant or the env var), also request a round-robin mentor as a reviewer
    # for every newly opened PR regardless of linked issues.
    auto_reviewer_enabled = MENTOR_AUTO_PR_REVIEWER_ENABLED or (
        env is not None
        and getattr(env, "MENTOR_AUTO_PR_REVIEWER_ENABLED", "").lower() == "true"
    )
    if auto_reviewer_enabled:
        try:
            mentors_config = await _fetch_mentors_config(env=env)
        except Exception:
            mentors_config = []
        try:
            await _assign_round_robin_mentor_reviewer(owner, repo, pr, mentors_config, token, auto_reviewer_enabled)
        except Exception as exc:
            console.error(f"[MentorPool] Round-robin reviewer failed (best-effort): {exc}")

    # Check for unresolved review conversations
    try:
        await check_unresolved_conversations(payload, token)
    except Exception as exc:
        console.error(f"[BLT] check_unresolved_conversations failed (best-effort, ignored): {exc}")

    # Label PR with number of pending checks (queued/waiting/action_required)
    await _try_label_pending_checks(owner, repo, pr, token)

async def _request_mentor_reviewer_for_pr(
    owner: str, repo: str, pr: dict, token: str
) -> None:
    """Request the assigned mentor as a reviewer if the PR is linked to a mentored issue.

    Parses the PR body for "Closes/Fixes/Resolves #N" references, fetches each linked
    issue, and — when the issue carries the ``mentor-assigned`` label — adds the mentor
    as a requested reviewer on the PR.
    """
    pr_number = pr["number"]
    pr_body = pr.get("body") or ""
    pr_author = (pr.get("user") or {}).get("login", "")

    # Extract issue numbers from common closing keywords.
    linked_issues = re.findall(
        r"(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)\s*#(\d+)",
        pr_body,
        re.IGNORECASE,
    )
    if not linked_issues:
        return

    already_requested: set = set()
    for issue_num_str in linked_issues:
        issue_number = int(issue_num_str)
        resp = await github_api(
            "GET",
            f"/repos/{owner}/{repo}/issues/{issue_number}",
            token,
        )
        if resp.status != 200:
            continue
        issue = json.loads(await resp.text())
        labels = {lb.get("name", "").lower() for lb in issue.get("labels", [])}
        if MENTOR_ASSIGNED_LABEL.lower() not in labels:
            continue

        # Find the mentor from issue comments.
        mentor_username = await _find_assigned_mentor_from_comments(
            owner, repo, issue_number, token
        )
        if not mentor_username or mentor_username.lower() == pr_author.lower():
            continue
        # Skip if this mentor was already requested for this PR (multiple linked issues
        # may reference the same mentor; avoid duplicate reviewer-request API calls).
        if mentor_username.lower() in already_requested:
            continue
        already_requested.add(mentor_username.lower())

        # Request the mentor as a reviewer on the PR.
        review_resp = await github_api(
            "POST",
            f"/repos/{owner}/{repo}/pulls/{pr_number}/requested_reviewers",
            token,
            {"reviewers": [mentor_username]},
        )
        if review_resp.status in (200, 201):
            console.log(
                f"[MentorPool] Requested @{mentor_username} as reviewer "
                f"for PR {owner}/{repo}#{pr_number} (linked issue #{issue_number})"
            )
        else:
            console.error(
                f"[MentorPool] Failed to request reviewer @{mentor_username} "
                f"for PR #{pr_number}: status={review_resp.status}"
            )

async def _assign_round_robin_mentor_reviewer(
    owner: str,
    repo: str,
    pr: dict,
    mentors_config: Optional[list],
    token: str,
    auto_reviewer_enabled: bool = False,
) -> None:
    """Auto-request one mentor as a reviewer on a newly opened PR (round-robin).

    Enabled only when ``MENTOR_AUTO_PR_REVIEWER_ENABLED`` is ``True``.
    Picks one active mentor using ``(pr_number - 1) mod pool_size`` so the
    assignment cycles predictably across consecutive PRs.  The PR author is
    never chosen as their own reviewer.
    """
    if not auto_reviewer_enabled:
        return

    pool = mentors_config if mentors_config is not None else []
    active = [
        m for m in pool
        if m.get("active", True) and m.get("github_username")
    ]
    if not active:
        return

    pr_number = pr["number"]
    pr_author = (pr.get("user") or {}).get("login", "").lower()

    # Sort by username for a stable, deterministic order.
    active.sort(key=lambda m: m["github_username"].lower())

    # Try each slot in order starting at the round-robin position until we find
    # a mentor who is not the PR author.
    for offset in range(len(active)):
        index = (pr_number - 1 + offset) % len(active)
        mentor = active[index]
        username = mentor["github_username"]
        if username.lower() == pr_author:
            continue
        # Candidate found — request this mentor and stop regardless of outcome
        # so only one reviewer is assigned per PR.
        resp = await github_api(
            "POST",
            f"/repos/{owner}/{repo}/pulls/{pr_number}/requested_reviewers",
            token,
            {"reviewers": [username]},
        )
        if resp.status in (200, 201):
            console.log(
                f"[MentorPool] Auto round-robin reviewer: requested @{username} "
                f"for {owner}/{repo}#{pr_number}"
            )
        else:
            console.error(
                f"[MentorPool] Auto round-robin reviewer: failed to request @{username} "
                f"for PR #{pr_number}: status={resp.status}"
            )
        break  # Only assign one reviewer per PR.

async def _post_merged_pr_combined_comment(
    owner: str,
    repo: str,
    pr_number: int,
    author_login: str,
    token: str,
    env=None,
    pr_reviewers: list = None,
) -> None:
    """Post a single combined comment on a merged PR containing thanks, contributor
    leaderboard, reviewer leaderboard, and a link to the BLT Pool website."""

    # ---------------------------------------------------------------------------
    # 1. Fetch leaderboard data via shared helper
    # ---------------------------------------------------------------------------
    leaderboard_data, leaderboard_note, _is_org = await _fetch_leaderboard_data(owner, repo, token, env)
    if leaderboard_data is None:
        leaderboard_data = {}

    # ---------------------------------------------------------------------------
    # 2. Build the combined comment body
    # ---------------------------------------------------------------------------
    thanks_section = (
        f"🎉 PR merged! Thanks for your contribution, @{author_login}!\n\n"
        "Your work is now part of the project. Keep contributing to "
        "[OWASP BLT-Pool](https://pool.owaspblt.org) and help make the web a safer place! 🛡️\n\n"
        "Visit [pool.owaspblt.org](https://pool.owaspblt.org) to explore the mentor pool and connect with contributors."
    )

    contributor_section = _format_leaderboard_comment(author_login, leaderboard_data, owner, leaderboard_note)
    if isinstance(contributor_section, str):
        contributor_section = contributor_section.replace(LEADERBOARD_MARKER + "\n", "")
    else:
        contributor_section = ""

    reviewer_section = _format_reviewer_leaderboard_comment(leaderboard_data, owner, pr_reviewers or [])
    if isinstance(reviewer_section, str):
        reviewer_section = reviewer_section.replace(REVIEWER_LEADERBOARD_MARKER + "\n", "")
    else:
        reviewer_section = ""

    combined_body = (
        MERGED_PR_COMMENT_MARKER + "\n"
        + thanks_section + "\n\n"
        + "---\n\n"
        + contributor_section + "\n\n"
        + "---\n\n"
        + reviewer_section
    )

    # ---------------------------------------------------------------------------
    # 3. Delete any old separate or combined comment(s), then post the new one
    # ---------------------------------------------------------------------------
    resp = await github_api("GET", f"/repos/{owner}/{repo}/issues/{pr_number}/comments?per_page=100", token)
    if resp.status == 200:
        old_comments = json.loads(await resp.text())
        for c in old_comments:
            body = c.get("body") or ""
            if any(
                marker in body
                for marker in (MERGED_PR_COMMENT_MARKER, LEADERBOARD_MARKER, REVIEWER_LEADERBOARD_MARKER)
            ):
                delete_resp = await github_api("DELETE", f"/repos/{owner}/{repo}/issues/comments/{c['id']}", token)
                if delete_resp.status not in (204, 200):
                    console.error(
                        f"[MergedPR] Failed to delete old comment {c['id']} "
                        f"for {owner}/{repo}#{pr_number}: status={delete_resp.status}"
                    )
    else:
        console.error(
            f"[MergedPR] Failed to list comments for {owner}/{repo}#{pr_number}: "
            f"status={resp.status}; posting new comment anyway"
        )

    await create_comment(owner, repo, pr_number, combined_body, token)
    console.log(f"[MergedPR] Posted combined merge comment for {owner}/{repo}#{pr_number}")

async def handle_pull_request_closed(payload: dict, token: str, env=None) -> None:
    pr = payload["pull_request"]
    author = pr.get("user", {})
    if _is_bot(author):
        return

    # Track close/merge counters for both merged and unmerged PRs.
    await _track_pr_closed_in_d1(payload, env)

    if not pr.get("merged"):
        return

    sender = payload["sender"]
    if not _is_human(sender) or _is_bot(sender):
        return

    owner = payload["repository"]["owner"]["login"]
    repo = payload["repository"]["name"]
    pr_number = pr["number"]
    author_login = pr["user"]["login"]

    # Post a single combined comment: thanks + contributor leaderboard + reviewer leaderboard
    pr_reviewers = await get_valid_reviewers(owner, repo, pr_number, author_login, token)
    await _post_merged_pr_combined_comment(owner, repo, pr_number, author_login, token, env, pr_reviewers)

async def handle_pull_request_review_submitted(payload: dict, env=None) -> None:
    """Track review credits in D1 (first two unique reviewers per PR per month)."""
    await _track_review_in_d1(payload, env)

async def check_unresolved_conversations(payload, token):
    """Add label if PR has unresolved review conversations"""
    pr = payload.get("pull_request")
    if not pr:
        return

    owner = payload["repository"]["owner"]["login"]
    repo = payload["repository"]["name"]
    number = pr["number"]

    query = """
    query($owner: String!, $repo: String!, $number: Int!) {
      repository(owner: $owner, name: $repo) {
        pullRequest(number: $number) {
          reviewThreads(first: 100) {
            nodes {
              isResolved
            }
          }
        }
      }
    }
    """

    resp = await fetch(
        "https://api.github.com/graphql",
        method="POST",
        headers=_gh_headers(token),
        body=json.dumps({
            "query": query,
            "variables": {"owner": owner, "repo": repo, "number": number},
        }),
    )

    if resp.status != 200:
        console.error(f"[BLT] GraphQL query failed: {resp.status}")
        return

    result = json.loads(await resp.text())
    pull_request = (
        result.get("data", {})
        .get("repository", {})
        .get("pullRequest")
    )
    if result.get("errors") or pull_request is None:
        console.error(f"[BLT] GraphQL reviewThreads query returned errors: {result.get('errors')}")
        return
    threads = (
        pull_request
        .get("reviewThreads", {})
        .get("nodes", [])
    )

    unresolved = any(not t.get("isResolved", True) for t in threads)

    unresolved_count = sum(not t.get("isResolved", True) for t in threads)

    # Remove any existing unresolved-conversations labels
    resp_labels = await github_api(
        "GET",
        f"/repos/{owner}/{repo}/issues/{number}/labels",
        token,
    )
    if resp_labels.status == 200:
        current_labels = json.loads(await resp_labels.text())
        for lb in current_labels:
            if lb["name"].startswith("unresolved-conversations"):
                await github_api(
                    "DELETE",
                    f"/repos/{owner}/{repo}/issues/{number}/labels/{quote(lb['name'], safe='')}",
                    token,
                )

    label = f"unresolved-conversations: {unresolved_count}"

    if unresolved:
        await _ensure_label_exists(owner, repo, label, "e74c3c", token)
    else:
        await _ensure_label_exists(owner, repo, label, "5cb85c", token)

    await github_api(
        "POST",
        f"/repos/{owner}/{repo}/issues/{number}/labels",
        token,
        {"labels": [label]},
    )

async def label_pending_checks(
    owner: str, repo: str, pr_number: int, head_sha: str, token: str
) -> None:
    """Update the 'N checks pending' label on a PR.

    Counts workflow runs for *head_sha* across the ``queued``, ``waiting``,
    and ``action_required`` statuses (all mean "waiting to be run") and
    applies a yellow label with the combined count.  Removes any pre-existing
    ``"* checks pending"`` or legacy ``"* workflow* awaiting approval"`` labels
    before adding the fresh one.  When all status queries fail the label is
    left unchanged to avoid spurious removals during API outages.
    """
    pending_count = 0
    any_succeeded = False
    for status in ("queued", "waiting", "action_required"):
        resp = await github_api(
            "GET",
            f"/repos/{owner}/{repo}/actions/runs?head_sha={head_sha}&status={status}&per_page=100",
            token,
        )
        if resp.status == 200:
            any_succeeded = True
            data = json.loads(await resp.text())
            pending_count += data.get("total_count", 0)

    if not any_succeeded:
        # Can't determine state; leave existing labels untouched.
        return

    # Remove any existing pending-checks labels (both new and legacy formats).
    resp_labels = await github_api(
        "GET",
        f"/repos/{owner}/{repo}/issues/{pr_number}/labels",
        token,
    )
    if resp_labels.status == 200:
        current_labels = json.loads(await resp_labels.text())
        for lb in current_labels:
            name = lb.get("name", "")
            is_pending = "check" in name and "pending" in name
            is_legacy = "workflow" in name and "awaiting approval" in name
            if is_pending or is_legacy:
                await github_api(
                    "DELETE",
                    f"/repos/{owner}/{repo}/issues/{pr_number}/labels/{quote(name, safe='')}",
                    token,
                )

    if pending_count > 0:
        noun = "check" if pending_count == 1 else "checks"
        label = f"{pending_count} {noun} pending"
        await _ensure_label_exists(owner, repo, label, "e4c84b", token)
        await github_api(
            "POST",
            f"/repos/{owner}/{repo}/issues/{pr_number}/labels",
            token,
            {"labels": [label]},
        )

check_workflows_awaiting_approval = label_pending_checks

async def _try_label_pending_checks(
    owner: str, repo: str, pr: dict, token: str
) -> None:
    """Best-effort wrapper: extract the head SHA from *pr* and call
    :func:`label_pending_checks`, logging any exception instead of raising.
    """
    head_sha = pr.get("head", {}).get("sha", "")
    if not head_sha:
        return
    try:
        await label_pending_checks(owner, repo, pr["number"], head_sha, token)
    except Exception as exc:
        console.error(f"[BLT] label_pending_checks failed (best-effort, ignored): {exc}")

async def handle_workflow_run(payload: dict, token: str) -> None:
    """Handle workflow_run events to update 'checks pending' labels on PRs.

    Resolves the PR(s) associated with the workflow run and calls
    :func:`label_pending_checks` for each one.  Falls back to searching open
    PRs by head SHA when the payload's ``pull_requests`` array is empty
    (e.g. fork PRs).
    """
    workflow_run = payload.get("workflow_run", {})
    owner = payload["repository"]["owner"]["login"]
    repo = payload["repository"]["name"]
    head_sha = workflow_run.get("head_sha", "")

    pr_numbers: set[int] = set()
    for pr in workflow_run.get("pull_requests", []):
        pr_numbers.add(pr["number"])

    # For fork PRs the pull_requests array is empty; fall back to a lookup
    if not pr_numbers and head_sha:
        resp = await github_api(
            "GET",
            f"/repos/{owner}/{repo}/pulls?state=open&per_page=100",
            token,
        )
        if resp.status == 200:
            pulls = json.loads(await resp.text())
            for pull in pulls:
                if pull.get("head", {}).get("sha") == head_sha:
                    pr_numbers.add(pull["number"])

    for pr_number in pr_numbers:
        await label_pending_checks(owner, repo, pr_number, head_sha, token)

async def handle_check_run(payload: dict, token: str) -> None:
    """Handle check_run events to keep 'N checks pending' labels accurate.

    Called for ``check_run.created`` and ``check_run.completed`` actions.
    Resolves the PR(s) linked to the check run's head SHA and updates the
    pending-checks label for each one.
    """
    check_run = payload.get("check_run", {})
    owner = payload["repository"]["owner"]["login"]
    repo = payload["repository"]["name"]
    head_sha = check_run.get("head_sha", "")

    pr_numbers: set[int] = set()
    for pr in check_run.get("pull_requests", []):
        pr_numbers.add(pr["number"])

    # For fork PRs the pull_requests array is empty; look up by head SHA.
    if not pr_numbers and head_sha:
        resp = await github_api(
            "GET",
            f"/repos/{owner}/{repo}/pulls?state=open&per_page=100",
            token,
        )
        if resp.status == 200:
            pulls = json.loads(await resp.text())
            for pull in pulls:
                if pull.get("head", {}).get("sha") == head_sha:
                    pr_numbers.add(pull["number"])

    for pr_number in pr_numbers:
        await label_pending_checks(owner, repo, pr_number, head_sha, token)
