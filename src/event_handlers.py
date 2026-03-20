"""GitHub webhook event handlers."""

import json
import re
import time
from typing import Optional
from urllib.parse import quote

from js import Headers, Response, console


# ---------------------------------------------------------------------------
# Event handlers
# ---------------------------------------------------------------------------


async def handle_issue_comment(payload: dict, token: str, env=None) -> None:
    comment = payload["comment"]
    issue = payload["issue"]
    if not _is_human(comment["user"]):
        return

    # Persist comments to D1 for leaderboard scoring.
    try:
        await _track_comment_in_d1(payload, env)
    except Exception as exc:
        console.error(f"[Leaderboard] Failed to persist comment event: {exc}")

    body = comment["body"].strip()
    command = _extract_command(body)
    owner = payload["repository"]["owner"]["login"]
    repo = payload["repository"]["name"]
    login = comment["user"]["login"]
    issue_number = issue["number"]
    comment_id = comment.get("id")

    # Add eyes reaction immediately to acknowledge command receipt
    if comment_id and command:
        await create_reaction(owner, repo, comment_id, "eyes", token)

    if command == ASSIGN_COMMAND:
        await _assign(owner, repo, issue, login, token)
    elif command == UNASSIGN_COMMAND:
        await _unassign(owner, repo, issue, login, token)
    elif command == LEADERBOARD_COMMAND:
        console.log(f"[Leaderboard] Command received for {owner}/{repo}#{issue_number} by @{login}")
        try:
            if env is None:
                await _post_or_update_leaderboard(owner, repo, issue_number, login, token)
            else:
                await _post_or_update_leaderboard(owner, repo, issue_number, login, token, env)
        except Exception as exc:
            console.error(f"[Leaderboard] Command failed for {owner}/{repo}#{issue_number}: {exc}")
            await _create_comment_best_effort(
                owner,
                repo,
                issue_number,
                f"@{login} I hit an error while generating the leaderboard. Please try again in a moment.",
                token,
            )
    elif command in (MENTOR_COMMAND, UNMENTOR_COMMAND, MENTOR_PAUSE_COMMAND, HANDOFF_COMMAND, REMATCH_COMMAND):
        if command == UNMENTOR_COMMAND:
            await handle_mentor_unassign(owner, repo, issue, login, token, env=env)
            return

        # Fetch mentors config once for all mentor-related commands.
        try:
            mentors_config = await _fetch_mentors_config(env=env)
        except Exception as exc:
            console.error(f"[MentorPool] Failed to fetch mentors config: {exc}")
            mentors_config = []

        if command == MENTOR_COMMAND:
            await handle_mentor_command(owner, repo, issue, login, token, mentors_config, env=env)
        elif command == MENTOR_PAUSE_COMMAND:
            await handle_mentor_pause(owner, repo, issue, login, token, mentors_config, env=env)
        elif command == HANDOFF_COMMAND:
            await handle_mentor_handoff(owner, repo, issue, login, token, mentors_config, env=env)
        elif command == REMATCH_COMMAND:
            await handle_mentor_rematch(owner, repo, issue, login, token, mentors_config, env=env)


async def _assign(
    owner: str, repo: str, issue: dict, login: str, token: str
) -> None:
    num = issue["number"]
    if issue.get("pull_request"):
        await _create_comment_best_effort(
            owner, repo, num,
            f"@{login} This command only works on issues, not pull requests.",
            token,
        )
        return
    if issue["state"] == "closed":
        await _create_comment_best_effort(
            owner, repo, num,
            f"@{login} This issue is already closed and cannot be assigned.",
            token,
        )
        return
    assignees = [a["login"] for a in issue.get("assignees", [])]
    if login in assignees:
        await _create_comment_best_effort(
            owner, repo, num,
            f"@{login} You are already assigned to this issue.",
            token,
        )
        return
    if len(assignees) >= MAX_ASSIGNEES:
        await _create_comment_best_effort(
            owner, repo, num,
            f"@{login} This issue already has the maximum number of assignees "
            f"({MAX_ASSIGNEES}). Please work on a different issue.",
            token,
        )
        return
    label_names = {lb.get("name", "").lower() for lb in issue.get("labels", [])}
    if HELP_WANTED_LABEL not in label_names:
        await create_comment(
            owner, repo, num,
            f"@{login} This issue is not yet ready for assignment. "
            f"A maintainer (such as @{TRIAGE_REVIEWER}) must first review it and add the "
            f'"{HELP_WANTED_LABEL}" label before `/assign` can be used.',
            token,
        )
        return
    await github_api(
        "POST",
        f"/repos/{owner}/{repo}/issues/{num}/assignees",
        token,
        {"assignees": [login]},
    )
    deadline = time.strftime(
        "%a, %d %b %Y %H:%M:%S UTC",
        time.gmtime(time.time() + ASSIGNMENT_DURATION_HOURS * 3600),
    )
    await _create_comment_best_effort(
        owner, repo, num,
        f"@{login} You have been assigned to this issue! 🎉\n\n"
        f"Please submit a pull request within **{ASSIGNMENT_DURATION_HOURS} hours** "
        f"(by {deadline}).\n\n"
        f"If you need more time or cannot complete the work, please comment "
        f"`{UNASSIGN_COMMAND}` so others can pick it up.\n\n"
        "Happy coding! 🚀 — [OWASP BLT-Pool](https://pool.owaspblt.org)",
        token,
    )


async def _unassign(
    owner: str, repo: str, issue: dict, login: str, token: str
) -> None:
    num = issue["number"]
    assignees = [a["login"] for a in issue.get("assignees", [])]
    if login not in assignees:
        await _create_comment_best_effort(
            owner, repo, num,
            f"@{login} You are not currently assigned to this issue.",
            token,
        )
        return
    await github_api(
        "DELETE",
        f"/repos/{owner}/{repo}/issues/{num}/assignees",
        token,
        {"assignees": [login]},
    )
    await _create_comment_best_effort(
        owner, repo, num,
        f"@{login} You have been unassigned from this issue. "
        "Thanks for letting us know! 👍\n\n"
        "The issue is now open for others to pick up.",
        token,
    )


async def handle_issue_opened(
    payload: dict, token: str, blt_api_url: str
) -> None:
    issue = payload["issue"]
    sender = payload["sender"]
    if not _is_human(sender):
        return
    owner = payload["repository"]["owner"]["login"]
    repo = payload["repository"]["name"]
    labels = [lb["name"].lower() for lb in issue.get("labels", [])]
    is_bug = any(lb in BUG_LABELS for lb in labels)
    msg = (
        f"👋 Thanks for opening this issue, @{sender['login']}!\n\n"
        "Our team will review it shortly. In the meantime:\n"
        "- If you'd like to work on this issue, comment `/assign` to get assigned.\n"
        "- Visit [OWASP BLT-Pool](https://pool.owaspblt.org) for more information about "
        "our bug bounty platform.\n"
    )
    if is_bug:
        bug_data = await report_bug_to_blt(blt_api_url, {
            "url": issue["html_url"],
            "description": issue["title"],
            "github_url": issue["html_url"],
            "label": labels[0] if labels else "bug",
        })
        if bug_data and bug_data.get("id"):
            msg += (
                "\n🐛 This issue has been automatically reported to "
                "[OWASP BLT-Pool](https://pool.owaspblt.org) "
                f"(Bug ID: #{bug_data['id']}). "
                "Thank you for helping improve security!\n"
            )
    await _create_comment_best_effort(owner, repo, issue["number"], msg, token)


async def handle_issue_labeled(
    payload: dict, token: str, blt_api_url: str, env=None
) -> None:
    issue = payload["issue"]
    label = payload.get("label") or {}
    label_name = label.get("name", "").lower()
    owner = payload["repository"]["owner"]["login"]
    repo = payload["repository"]["name"]

    # --- needs-mentor label: trigger mentor pool assignment ---
    if label_name == NEEDS_MENTOR_LABEL:
        # The contributor is the first assignee if set, otherwise the issue author.
        # Avoid using payload['sender'] because for labeled events the sender is the
        # labeler (often a maintainer or bot), not the person working on the issue.
        assignees = issue.get("assignees", [])
        contributor_login = (
            assignees[0]["login"] if assignees else (issue.get("user") or {}).get("login", "")
        )
        try:
            mentors_config = await _fetch_mentors_config(env=env)
        except Exception as exc:
            console.error(f"[MentorPool] Failed to fetch mentors config on label event: {exc}")
            mentors_config = []
        await _assign_mentor_to_issue(
            owner, repo, issue, contributor_login, token, mentors_config, env=env
        )
        return

    # --- Bug labels: report to BLT ---
    if label_name not in BUG_LABELS:
        return
    all_labels = [lb["name"].lower() for lb in issue.get("labels", [])]
    # Only report the first time a bug label is added (avoid duplicates)
    if any(lb in BUG_LABELS for lb in all_labels if lb != label_name):
        return
    bug_data = await report_bug_to_blt(blt_api_url, {
        "url": issue["html_url"],
        "description": issue["title"],
        "github_url": issue["html_url"],
        "label": label.get("name", "bug"),
    })
    if bug_data and bug_data.get("id"):
        await _create_comment_best_effort(
            owner, repo, issue["number"],
            f"🐛 This issue has been reported to [OWASP BLT-Pool](https://pool.owaspblt.org) "
            f"(Bug ID: #{bug_data['id']}) after being labeled as "
            f"`{label.get('name', 'bug')}`.",
            token,
        )


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
    author_login = (pr.get("user") or {}).get("login") or sender["login"]

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
            await _assign_round_robin_mentor_reviewer(owner, repo, pr, mentors_config, token, enabled=auto_reviewer_enabled)
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
    enabled: bool = False,
) -> None:
    """Auto-request one mentor as a reviewer on a newly opened PR (round-robin).

    Enabled when caller passes ``enabled=True``.
    Picks one active mentor using ``(pr_number - 1) mod pool_size`` so the
    assignment cycles predictably across consecutive PRs.  The PR author is
    never chosen as their own reviewer.
    """
    if not enabled:
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
    # Strip the marker from the inner section — the combined comment has its own marker.
    contributor_section = contributor_section.replace(LEADERBOARD_MARKER + "\n", "")

    reviewer_section = _format_reviewer_leaderboard_comment(leaderboard_data, owner, pr_reviewers or [])
    reviewer_section = reviewer_section.replace(REVIEWER_LEADERBOARD_MARKER + "\n", "")

    combined_body = (
        MERGED_PR_COMMENT_MARKER + "\n"
        + thanks_section + "\n\n"
        + "---\n\n"
        + contributor_section + "\n\n"
        + "---\n\n"
        + reviewer_section
    )

    # Snapshot existing marker comments before posting to avoid deleting the
    # newly-created combined comment during cleanup.
    old_comments, list_failed = await _fetch_issue_comments_paged(owner, repo, pr_number, token)
    snapshot_marker_ids = []
    if list_failed:
        console.error(
            f"[MergedPR] Failed to list comments for {owner}/{repo}#{pr_number}: "
            "status=unknown; skipping duplicate cleanup snapshot"
        )
    else:
        snapshot_marker_ids = [
            int(c.get("id") or 0)
            for c in old_comments
            if any(
                marker in (c.get("body") or "")
                for marker in (MERGED_PR_COMMENT_MARKER, LEADERBOARD_MARKER, REVIEWER_LEADERBOARD_MARKER)
            )
            and int(c.get("id") or 0) > 0
        ]

    created = await _create_comment_strict(owner, repo, pr_number, combined_body, token)
    if created is False:
        console.error(f"[MergedPR] Failed to post combined merge comment for {owner}/{repo}#{pr_number}")
        return

    # ---------------------------------------------------------------------------
    # 3. Delete any old separate or combined comment(s) after posting a new one
    # ---------------------------------------------------------------------------
    for comment_id in snapshot_marker_ids:
        delete_resp = await github_api("DELETE", f"/repos/{owner}/{repo}/issues/comments/{comment_id}", token)
        if delete_resp.status not in (204, 200):
            console.error(
                f"[MergedPR] Failed to delete old comment {comment_id} "
                f"for {owner}/{repo}#{pr_number}: status={delete_resp.status}"
            )

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


async def _ensure_label_exists(
    owner: str, repo: str, name: str, color: str, token: str
) -> None:
    """Create a label if it does not already exist, or update its colour."""
    resp = await github_api(
        "GET",
        f"/repos/{owner}/{repo}/labels/{quote(name, safe='')}",
        token,
    )
    if resp.status == 404:
        await github_api(
            "POST",
            f"/repos/{owner}/{repo}/labels",
            token,
            {"name": name, "color": color},
        )
    elif resp.status == 200:
        data = json.loads(await resp.text())
        if data.get("color") != color:
            await github_api(
                "PATCH",
                f"/repos/{owner}/{repo}/labels/{quote(name, safe='')}",
                token,
                {"color": color},
            )

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


# ---------------------------------------------------------------------------
# Workflow approval labels
# ---------------------------------------------------------------------------


# Pending checks labels
# ---------------------------------------------------------------------------


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


# Keep old name as an alias so any external callers remain compatible.
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


# ---------------------------------------------------------------------------
# Peer review enforcement
# ---------------------------------------------------------------------------

# Common bot account patterns that should not count as peer reviews
def _is_excluded_reviewer(login: str) -> bool:
    """Return True if the reviewer is a bot or automated account."""
    if not login:
        return True
    login_lower = login.lower()
    # Exact matches
    excluded_exact = {
        "coderabbitai[bot]",
        "dependabot[bot]",
        "dependabot-preview[bot]",
        "dependabot",
        "github-actions[bot]",
    }
    if login_lower in excluded_exact:
        return True
    # Pattern matches (substrings that indicate bots)
    bot_patterns = [
        "[bot]",
        "bot]",
        "copilot",
        "renovate",
        "actions-user",
        "sentry",
        "snyk",
        "sonarcloud",
        "codecov",
    ]
    return any(pattern in login_lower for pattern in bot_patterns)


async def get_valid_reviewers(owner: str, repo: str, pr_number: int, pr_author: str, token: str) -> list[str]:
    """Get list of valid approved reviewers for a PR (excluding bots and the PR author).
    
    Paginates through all reviews and tracks the latest state per reviewer.
    Only reviewers with latest state == "APPROVED" count as valid.
    """
    # Track latest state per reviewer (chronological order, last event wins)
    reviewer_latest_state = {}
    page = 1
    
    while True:
        resp = await github_api("GET", f"/repos/{owner}/{repo}/pulls/{pr_number}/reviews?per_page=100&page={page}", token)
        if resp.status != 200:
            console.error(f"[BLT] Failed to fetch reviews for PR #{pr_number}: {resp.status}")
            break
        
        reviews = json.loads(await resp.text())
        if not reviews:
            break
        
        # Process reviews in chronological order; overwrite each reviewer's state
        for review in reviews:
            reviewer_login = review.get("user", {}).get("login", "")
            state = review.get("state", "")
            if reviewer_login:
                reviewer_latest_state[reviewer_login] = state
        
        page += 1
    
    # Filter to only valid, approved reviewers
    valid_reviewers = set()
    for reviewer_login, state in reviewer_latest_state.items():
        if state != "APPROVED":
            continue
        if reviewer_login == pr_author:
            continue
        if _is_excluded_reviewer(reviewer_login):
            continue
        valid_reviewers.add(reviewer_login)
    
    return list(valid_reviewers)


async def ensure_label_exists(owner: str, repo: str, label_name: str, color: str, description: str, token: str) -> None:
    """Create or update a label to ensure it exists with the correct color/description."""
    resp = await github_api("GET", f"/repos/{owner}/{repo}/labels/{label_name}", token)
    
    if resp.status == 200:
        # Label exists, check if it needs update
        data = json.loads(await resp.text())
        if data.get("color") != color or data.get("description") != description:
            update_resp = await github_api("PATCH", f"/repos/{owner}/{repo}/labels/{label_name}", token, {
                "color": color,
                "description": description,
            })
            if update_resp.status not in (200, 201):
                error_text = await update_resp.text() if update_resp.status >= 400 else ""
                console.error(f"[BLT] Failed to update label {label_name}: {update_resp.status} {error_text}")
    elif resp.status == 404:
        # Label doesn't exist, create it
        create_resp = await github_api("POST", f"/repos/{owner}/{repo}/labels", token, {
            "name": label_name,
            "color": color,
            "description": description,
        })
        if create_resp.status not in (200, 201):
            error_text = await create_resp.text() if create_resp.status >= 400 else ""
            console.error(f"[BLT] Failed to create label {label_name}: {create_resp.status} {error_text}")


async def update_peer_review_labels(owner: str, repo: str, pr_number: int, has_review: bool, token: str) -> None:
    """Add/remove peer review labels based on whether the PR has a valid review."""
    new_label = "has-peer-review" if has_review else "needs-peer-review"
    old_label = "needs-peer-review" if has_review else "has-peer-review"
    color = "0e8a16" if has_review else "e74c3c"  # Green or Red
    description = "PR has received peer review" if has_review else "PR needs peer review"
    
    # Ensure the new label exists
    await ensure_label_exists(owner, repo, new_label, color, description, token)
    
    # Get current labels
    resp = await github_api("GET", f"/repos/{owner}/{repo}/issues/{pr_number}/labels", token)
    if resp.status != 200:
        return
    
    current_labels = json.loads(await resp.text())
    current_label_names = {label.get("name") for label in current_labels}
    
    # Remove old label if present
    if old_label in current_label_names:
        await github_api("DELETE", f"/repos/{owner}/{repo}/issues/{pr_number}/labels/{old_label}", token)
    
    # Add new label if not present
    if new_label not in current_label_names:
        await github_api("POST", f"/repos/{owner}/{repo}/issues/{pr_number}/labels", token, {"labels": [new_label]})


async def check_peer_review_and_comment(owner: str, repo: str, pr_number: int, pr_author: str, token: str) -> None:
    """Check if a PR has peer review, update labels, and post a comment if needed."""
    # Skip for excluded accounts
    if _is_excluded_reviewer(pr_author):
        return
    
    reviewers = await get_valid_reviewers(owner, repo, pr_number, pr_author, token)
    has_review = len(reviewers) > 0
    
    # Update labels
    await update_peer_review_labels(owner, repo, pr_number, has_review, token)
    
    # If no review, post a reminder comment (only once)
    if not has_review:
        # Check if we already posted the reminder (with pagination support)
        marker = "<!-- peer-review-check -->"
        already_commented = False
        page = 1
        while True:
            resp = await github_api("GET", f"/repos/{owner}/{repo}/issues/{pr_number}/comments?per_page=100&page={page}", token)
            if resp.status != 200:
                break
            comments = json.loads(await resp.text())
            if not comments:
                break
            if any(marker in comment.get("body", "") for comment in comments):
                already_commented = True
                break
            page += 1
        
        # Post comment only after searching all pages
        if not already_commented:
            body = f"""{marker}
👋 Hi @{pr_author}!

This pull request needs a peer review before it can be merged. Please request a review from a team member who is not:
- The PR author
- coderabbitai
- copilot

Once a valid peer review is submitted, this check will pass automatically. Thank you!

> ⚠️ Peer review enforcement is active."""
            await _create_comment_best_effort(owner, repo, pr_number, body, token)


async def handle_pull_request_review(payload: dict, token: str) -> None:
    """Handle pull_request_review events (submitted/dismissed) to check peer review status."""
    pr = payload["pull_request"]
    owner = payload["repository"]["owner"]["login"]
    repo = payload["repository"]["name"]
    pr_author = pr["user"]["login"]
    
    await check_peer_review_and_comment(owner, repo, pr["number"], pr_author, token)


async def handle_pull_request_for_review(payload: dict, token: str) -> None:
    """Handle pull_request events (opened/synchronize/reopened) to check peer review status."""
    pr = payload["pull_request"]
    sender = payload["sender"]
    if not _is_human(sender):
        return
    
    owner = payload["repository"]["owner"]["login"]
    repo = payload["repository"]["name"]
    pr_author = pr["user"]["login"]
    
    await check_peer_review_and_comment(owner, repo, pr["number"], pr_author, token)

    # Label PR with number of pending checks (queued/waiting/action_required)
    await _try_label_pending_checks(owner, repo, pr, token)


# ---------------------------------------------------------------------------
# Webhook dispatcher
# ---------------------------------------------------------------------------


async def handle_webhook(request, env) -> Response:
    """Verify the GitHub webhook signature and route to the correct handler."""
    body_text = await request.text()
    payload_bytes = body_text.encode("utf-8")

    # Extract header metadata immediately so every webhook invocation is logged.
    delivery_id = request.headers.get("X-GitHub-Delivery", "")
    event = request.headers.get("X-GitHub-Event", "")

    # Parse payload once up front for concise logging fields.
    payload = {}
    payload_parse_error = False
    try:
        payload = json.loads(body_text)
    except Exception:
        payload_parse_error = True

    action = payload.get("action", "") if isinstance(payload, dict) else ""
    installation_id = ((payload.get("installation") or {}).get("id") if isinstance(payload, dict) else None)
    repo_full_name = ((payload.get("repository") or {}).get("full_name") if isinstance(payload, dict) else "")
    sender_login = ((payload.get("sender") or {}).get("login") if isinstance(payload, dict) else "")
    issue_number = ((payload.get("issue") or {}).get("number") if isinstance(payload, dict) else None)
    pr_number = ((payload.get("pull_request") or {}).get("number") if isinstance(payload, dict) else None)
    item_number = issue_number or pr_number or ""

    signature = request.headers.get("X-Hub-Signature-256") or ""
    secret = (getattr(env, "WEBHOOK_SECRET", "") or "").strip()
    if not secret:
        console.error(
            "[BLT][webhook] "
            f"delivery={delivery_id or '-'} event={event or '-'} action={action or '-'} "
            f"repo={repo_full_name or '-'} sender={sender_login or '-'} item={item_number or '-'} "
            f"installation={installation_id or '-'} method={request.method} "
            "status=rejected_missing_webhook_secret"
        )
        return _json(
            {
                "error": "Webhook authentication is not configured (missing WEBHOOK_SECRET)",
                "code": "webhook_secret_missing",
            },
            503,
        )
    if not verify_signature(payload_bytes, signature, secret):
        console.log(
            "[BLT][webhook] "
            f"delivery={delivery_id or '-'} event={event or '-'} action={action or '-'} "
            f"repo={repo_full_name or '-'} sender={sender_login or '-'} item={item_number or '-'} "
            f"installation={installation_id or '-'} method={request.method} status=rejected_invalid_signature"
        )
        return _json({"error": "Invalid signature"}, 401)

    if payload_parse_error:
        console.log(
            "[BLT][webhook] "
            f"delivery={delivery_id or '-'} event={event or '-'} action=- repo=- sender=- item=- "
            f"installation=- method={request.method} status=rejected_invalid_json"
        )
        return _json({"error": "Invalid JSON"}, 400)

    console.log(
        "[BLT][webhook] "
        f"delivery={delivery_id or '-'} event={event or '-'} action={action or '-'} "
        f"repo={repo_full_name or '-'} sender={sender_login or '-'} item={item_number or '-'} "
        f"installation={installation_id or '-'} method={request.method} status=received"
    )

    app_id = getattr(env, "APP_ID", "")
    private_key = getattr(env, "PRIVATE_KEY", "")
    token = None
    if installation_id and app_id and private_key:
        token = await get_installation_token(installation_id, app_id, private_key)

    if not token:
        console.error("[BLT] Could not obtain installation token")
        return _json({"error": "Authentication failed"}, 500)

    blt_api_url = getattr(env, "BLT_API_URL", "https://blt-api.owasp-blt.workers.dev")

    try:
        if event == "issue_comment" and action == "created":
            await handle_issue_comment(payload, token, env)
        elif event == "issues":
            if action == "opened":
                await handle_issue_opened(payload, token, blt_api_url)
            elif action == "labeled":
                await handle_issue_labeled(payload, token, blt_api_url, env=env)
        elif event == "pull_request":
            if action == "opened":
                await handle_pull_request_opened(payload, token, env)
                await handle_pull_request_for_review(payload, token)
            elif action == "synchronize":
                await handle_pull_request_for_review(payload, token)
            elif action == "reopened":
                await _track_pr_reopened_in_d1(payload, env)
                await handle_pull_request_for_review(payload, token)
            elif action == "closed":
                await handle_pull_request_closed(payload, token, env)
        elif event == "pull_request_review":
            if action == "submitted":
                # Preserve existing D1 review-credit tracking
                await handle_pull_request_review_submitted(payload, env)
                # Also check peer review status
                await handle_pull_request_review(payload, token)
            elif action == "dismissed":
                await handle_pull_request_review(payload, token)
        elif event == "pull_request_review_comment":
            await check_unresolved_conversations(payload, token)
        elif event == "pull_request_review_thread":
            await check_unresolved_conversations(payload, token)
        elif event == "workflow_run":
            await handle_workflow_run(payload, token)
        elif event == "check_run" and action in ("created", "completed"):
            await handle_check_run(payload, token)

    except Exception as exc:
        console.error(f"[BLT] Webhook handler error: {exc}")
        return _json({"error": "Internal server error"}, 500)

    return _json({"ok": True})


# ---------------------------------------------------------------------------
# Landing page HTML — separated into src/index_template.py for maintainability.
# Edit templates/index.html and regenerate src/index_template.py before deploying.
# ---------------------------------------------------------------------------

_CALLBACK_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>BLT-Pool GitHub App — Installed!</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link
    rel="stylesheet"
    href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css"
    crossorigin="anonymous"
    referrerpolicy="no-referrer"
  />
</head>
<body class="min-h-screen flex items-center justify-center" style="background:#111827;color:#e5e7eb;">
  <div class="text-center rounded-xl p-12 max-w-md w-full mx-4" style="background:#1F2937;border:1px solid #374151;">
    <div class="w-16 h-16 rounded-full flex items-center justify-center mx-auto mb-6" style="background:rgba(225,1,1,0.1);">
      <i class="fa-solid fa-circle-check text-3xl" style="color:#E10101;" aria-hidden="true"></i>
    </div>
    <h1 class="text-2xl font-bold text-white mb-4">Installation complete!</h1>
    <p class="leading-relaxed mb-6" style="color:#9ca3af;">
      The BLT-Pool GitHub App has been successfully installed on your organization.<br />
      GitHub automation is now active inside BLT-Pool.
    </p>
    <a
      href="https://owaspblt.org"
      target="_blank"
      rel="noopener"
      style="color:#E10101;"
      onmouseover="this.style.textDecoration='underline'" onmouseout="this.style.textDecoration='none'"
    >
      Visit OWASP BLT <i class="fa-solid fa-arrow-right text-xs" aria-hidden="true"></i>
    </a>
  </div>
</body>
</html>
"""


