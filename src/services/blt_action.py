import json
from typing import Optional
from urllib.parse import quote
import time

ASSIGN_COMMAND = "/assign"
UNASSIGN_COMMAND = "/unassign"
APPROVE_COMMAND = "/approve"
DENY_COMMAND = "/deny"
MAX_ASSIGNEES = 1
ASSIGNMENT_DURATION_HOURS = 8
HELP_WANTED_LABEL = "help wanted"
NEEDS_APPROVAL_LABEL = "needs-approval"
NEEDS_APPROVAL_LABEL_COLOR = "e11d48"
TRIAGE_REVIEWER = "donnieblt"

UNRESOLVED_CONVERSATIONS_CHECK_NAME = "Unresolved Conversations"
UNRESOLVED_CONVERSATIONS_MARKER = "<!-- BLT-UNRESOLVED-CONVERSATIONS -->"




async def ensure_label_exists(
    owner: str, repo: str, name: str, color: str, token: str, github_api_fn
) -> None:
    """Create a label if it does not already exist, or update its colour."""
    resp = await github_api_fn(
        "GET",
        f"/repos/{owner}/{repo}/labels/{quote(name, safe='')}",
        token,
    )
    if resp.status == 404:
        await github_api_fn(
            "POST",
            f"/repos/{owner}/{repo}/labels",
            token,
            {"name": name, "color": color},
        )
    elif resp.status == 200:
        data = json.loads(await resp.text())
        if data.get("color") != color:
            await github_api_fn(
                "PATCH",
                f"/repos/{owner}/{repo}/labels/{quote(name, safe='')}",
                token,
                {"color": color},
            )


async def ensure_label_exists_with_description(
    owner: str, repo: str, label_name: str, color: str, description: str,
    token: str, github_api_fn
) -> None:
    """Create or update a label to ensure it exists with the correct color/description."""
    encoded_name = quote(label_name, safe='')
    resp = await github_api_fn("GET", f"/repos/{owner}/{repo}/labels/{encoded_name}", token)
    if resp.status == 200:
        data = json.loads(await resp.text())
        if data.get("color") != color or data.get("description") != description:
            await github_api_fn(
                "PATCH", f"/repos/{owner}/{repo}/labels/{encoded_name}", token,
                {"color": color, "description": description},
            )
    elif resp.status == 404:
        await github_api_fn(
            "POST", f"/repos/{owner}/{repo}/labels", token,
            {"name": label_name, "color": color, "description": description},
        )

async def handle_assign(
    owner: str, repo: str, issue: dict, login: str, token: str,
    github_api_fn, create_comment_fn
) -> None:
    """Handle the /assign slash command."""
    num = issue["number"]
    if issue.get("pull_request"):
        await create_comment_fn(
            owner, repo, num,
            f"@{login} This command only works on issues, not pull requests.",
            token,
        )
        return
    if issue["state"] == "closed":
        await create_comment_fn(
            owner, repo, num,
            f"@{login} This issue is already closed and cannot be assigned.",
            token,
        )
        return
    assignees = [a["login"] for a in issue.get("assignees", [])]
    if login in assignees:
        await create_comment_fn(
            owner, repo, num,
            f"@{login} You are already assigned to this issue.",
            token,
        )
        return
    if len(assignees) >= MAX_ASSIGNEES:
        await create_comment_fn(
            owner, repo, num,
            f"@{login} This issue already has the maximum number of assignees "
            f"({MAX_ASSIGNEES}). Please work on a different issue.",
            token,
        )
        return
    label_names = {lb.get("name", "").lower() for lb in issue.get("labels", [])}
    if HELP_WANTED_LABEL not in label_names:
        await create_comment_fn(
            owner, repo, num,
            f"@{login} This issue is not yet ready for assignment. "
            f"A maintainer (such as @{TRIAGE_REVIEWER}) must first review it and add the "
            f'"{HELP_WANTED_LABEL}" label before `/assign` can be used.',
            token,
        )
        await ensure_label_exists(owner, repo, NEEDS_APPROVAL_LABEL, NEEDS_APPROVAL_LABEL_COLOR, token, github_api_fn)
        await github_api_fn(
            "POST",
            f"/repos/{owner}/{repo}/issues/{num}/labels",
            token,
            {"labels": [NEEDS_APPROVAL_LABEL]},
        )
        return
    await github_api_fn(
        "POST",
        f"/repos/{owner}/{repo}/issues/{num}/assignees",
        token,
        {"assignees": [login]},
    )
    deadline = time.strftime(
        "%a, %d %b %Y %H:%M:%S UTC",
        time.gmtime(time.time() + ASSIGNMENT_DURATION_HOURS * 3600),
    )
    await create_comment_fn(
        owner, repo, num,
        f"@{login} You have been assigned to this issue! 🎉\n\n"
        f"Please submit a pull request within **{ASSIGNMENT_DURATION_HOURS} hours** "
        f"(by {deadline}).\n\n"
        f"If you need more time or cannot complete the work, please comment "
        f"`{UNASSIGN_COMMAND}` so others can pick it up.\n\n"
        "Happy coding! 🚀 — [OWASP BLT-Pool](https://pool.owaspblt.org)",
        token,
    )




async def handle_unassign(
    owner: str, repo: str, issue: dict, login: str, token: str,
    github_api_fn, create_comment_fn
) -> None:
    """Handle the /unassign slash command."""
    num = issue["number"]
    assignees = [a["login"] for a in issue.get("assignees", [])]
    if login not in assignees:
        await create_comment_fn(
            owner, repo, num,
            f"@{login} You are not currently assigned to this issue.",
            token,
        )
        return
    await github_api_fn(
        "DELETE",
        f"/repos/{owner}/{repo}/issues/{num}/assignees",
        token,
        {"assignees": [login]},
    )
    await create_comment_fn(
        owner, repo, num,
        f"@{login} You have been unassigned from this issue. "
        "Thanks for letting us know! 👍\n\n"
        "The issue is now open for others to pick up.",
        token,
    )




async def handle_approve(
    owner: str, repo: str, issue: dict, login: str, token: str,
    github_api_fn, create_comment_fn
) -> None:
    """Handle the /approve command (triage reviewer approves an issue for assignment)."""
    num = issue["number"]
    if login.lower() != TRIAGE_REVIEWER.lower():
        await create_comment_fn(
            owner, repo, num,
            f"@{login} Only @{TRIAGE_REVIEWER} can approve issues.",
            token,
        )
        return
    if issue.get("pull_request"):
        await create_comment_fn(
            owner, repo, num,
            f"@{login} The `/approve` command only works on issues, not pull requests.",
            token,
        )
        return
    if issue.get("state") == "closed":
        await create_comment_fn(
            owner, repo, num,
            f"@{login} This issue is already closed and cannot be approved.",
            token,
        )
        return
    await github_api_fn(
        "POST",
        f"/repos/{owner}/{repo}/issues/{num}/labels",
        token,
        {"labels": [HELP_WANTED_LABEL]},
    )
    opener = issue.get("user", {}).get("login", "")
    opener_assigned = False
    assignment_note = ""
    if opener:
        assignees = issue.get("assignees") or []
        assignee_logins = {
            a.get("login")
            for a in assignees
            if isinstance(a, dict) and a.get("login")
        }
        if opener in assignee_logins:
            opener_assigned = True
        elif len(assignee_logins) >= MAX_ASSIGNEES:
            assignment_note = (
                "However, this issue already has the maximum number of assignees, "
                "so the opener was not additionally assigned."
            )
        elif assignee_logins:
            assignment_note = (
                "Note: this issue already has an assignee, so the opener was not "
                "automatically assigned."
            )
        else:
            await github_api_fn(
                "POST",
                f"/repos/{owner}/{repo}/issues/{num}/assignees",
                token,
                {"assignees": [opener]},
            )
            opener_assigned = True
    if opener and opener_assigned:
        assignment_text = f"@{opener} You have been assigned — good luck! 🚀\n\n"
    elif assignment_note:
        assignment_text = assignment_note + "\n\n"
    else:
        assignment_text = ""
    await create_comment_fn(
        owner, repo, num,
        f"✅ This issue has been approved by @{login}!\n\n"
        + assignment_text
        + f'The `"{HELP_WANTED_LABEL}"` label has been added so others can also use '
        f"`/assign` to claim this issue.",
        token,
    )



async def handle_deny(
    owner: str, repo: str, issue: dict, login: str, token: str,
    github_api_fn, create_comment_fn
) -> None:
    """Handle the /deny command (triage reviewer rejects and closes an issue)."""
    num = issue["number"]
    if login.lower() != TRIAGE_REVIEWER.lower():
        await create_comment_fn(
            owner, repo, num,
            f"@{login} Only @{TRIAGE_REVIEWER} can deny issues.",
            token,
        )
        return
    if issue.get("pull_request"):
        await create_comment_fn(
            owner, repo, num,
            f"@{login} The `/deny` command only works on issues, not pull requests.",
            token,
        )
        return
    if issue["state"] == "closed":
        await create_comment_fn(
            owner, repo, num,
            f"@{login} This issue is already closed.",
            token,
        )
        return
    await create_comment_fn(
        owner, repo, num,
        f" This issue has been denied by @{login} and will be closed.\n\n"
        "If you believe this was a mistake, please open a new issue with more details.",
        token,
    )
    await github_api_fn(
        "PATCH",
        f"/repos/{owner}/{repo}/issues/{num}",
        token,
        {"state": "closed"},
    )
async def label_pending_checks(
    owner: str, repo: str, pr_number: int, head_sha: str, token: str,
    github_api_fn
) -> None:
    """Update the 'N checks pending' label on a PR."""
    pending_count = 0
    any_succeeded = False
    for status in ("queued", "waiting", "action_required"):
        resp = await github_api_fn(
            "GET",
            f"/repos/{owner}/{repo}/actions/runs?head_sha={head_sha}&status={status}&per_page=100",
            token,
        )
        if resp.status == 200:
            any_succeeded = True
            data = json.loads(await resp.text())
            pending_count += data.get("total_count", 0)
    if not any_succeeded:
        return
    resp_labels = await github_api_fn(
        "GET",
        f"/repos/{owner}/{repo}/issues/{pr_number}/labels",
        token,
    )
    if resp_labels.status == 200:
        current_labels = json.loads(await resp_labels.text())
        for lb in current_labels:
            name = lb.get("name", "")
            if ("check" in name and "pending" in name) or ("workflow" in name and "awaiting approval" in name):
                await github_api_fn(
                    "DELETE",
                    f"/repos/{owner}/{repo}/issues/{pr_number}/labels/{quote(name, safe='')}",
                    token,
                )
    if pending_count > 0:
        noun = "check" if pending_count == 1 else "checks"
        label = f"{pending_count} {noun} pending"
        await ensure_label_exists(owner, repo, label, "e4c84b", token, github_api_fn)
        await github_api_fn(
            "POST",
            f"/repos/{owner}/{repo}/issues/{pr_number}/labels",
            token,
            {"labels": [label]},
        )


async def try_label_pending_checks(
    owner: str, repo: str, pr: dict, token: str, github_api_fn
) -> None:
    """Best-effort wrapper for label_pending_checks."""
    from js import console
    head_sha = pr.get("head", {}).get("sha", "")
    if not head_sha:
        return
    try:
        await label_pending_checks(owner, repo, pr["number"], head_sha, token, github_api_fn)
    except Exception as exc:
        console.error(f"[BLT] label_pending_checks failed (best-effort, ignored): {exc}")


async def check_unresolved_conversations(
    payload: dict, token: str, github_api_fn, create_comment_fn,
    fetch_fn, gh_headers_fn, build_check_payloads_fn
) -> None:
    """Add label, create a check run, and post a comment if PR has unresolved review conversations."""
    from js import console
    pr = payload.get("pull_request")
    if not pr:
        return
    owner = payload["repository"]["owner"]["login"]
    repo = payload["repository"]["name"]
    number = pr["number"]
    head_sha = pr.get("head", {}).get("sha", "")
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
    resp = await fetch_fn(
        "https://api.github.com/graphql",
        method="POST",
        headers=gh_headers_fn(token),
        body=json.dumps({"query": query, "variables": {"owner": owner, "repo": repo, "number": number}}),
    )
    if resp.status != 200:
        console.error(f"[BLT] GraphQL query failed: {resp.status}")
        return
    result = json.loads(await resp.text())
    pull_request = result.get("data", {}).get("repository", {}).get("pullRequest")
    if result.get("errors") or pull_request is None:
        console.error(f"[BLT] GraphQL reviewThreads query returned errors: {result.get('errors')}")
        return
    threads = pull_request.get("reviewThreads", {}).get("nodes", [])
    unresolved = any(not t.get("isResolved", True) for t in threads)
    unresolved_count = sum(not t.get("isResolved", True) for t in threads)
    resp_labels = await github_api_fn("GET", f"/repos/{owner}/{repo}/issues/{number}/labels", token)
    if resp_labels.status == 200:
        current_labels = json.loads(await resp_labels.text())
        for lb in current_labels:
            if lb["name"].startswith("unresolved-conversations"):
                await github_api_fn(
                    "DELETE",
                    f"/repos/{owner}/{repo}/issues/{number}/labels/{quote(lb['name'], safe='')}",
                    token,
                )
    label = f"unresolved-conversations: {unresolved_count}"
    if unresolved:
        await ensure_label_exists(owner, repo, label, "e74c3c", token, github_api_fn)
    else:
        await ensure_label_exists(owner, repo, label, "5cb85c", token, github_api_fn)
    await github_api_fn("POST", f"/repos/{owner}/{repo}/issues/{number}/labels", token, {"labels": [label]})
    noun = "conversation" if unresolved_count == 1 else "conversations"
    if head_sha:
        if unresolved:
            check_title = f"{unresolved_count} unresolved {noun}"
            check_summary = (
                f"There {'is' if unresolved_count == 1 else 'are'} {unresolved_count} "
                f"unresolved review {noun} that must be resolved before merging."
            )
            check_conclusion = "failure"
        else:
            check_title = "All conversations resolved"
            check_summary = "All review conversations have been resolved."
            check_conclusion = "success"
        update_payload = build_check_payloads_fn(
            status="completed", title=check_title, summary=check_summary, conclusion=check_conclusion,
        )[0]
        existing_check_run_id = None
        resp_check_runs = await github_api_fn("GET", f"/repos/{owner}/{repo}/commits/{head_sha}/check-runs", token)
        if resp_check_runs.status == 200:
            resp_data = json.loads(await resp_check_runs.text())
            for check_run in resp_data.get("check_runs", []):
                if check_run.get("name") == UNRESOLVED_CONVERSATIONS_CHECK_NAME:
                    existing_check_run_id = check_run.get("id")
                    break
        if existing_check_run_id is not None:
            await github_api_fn("PATCH", f"/repos/{owner}/{repo}/check-runs/{existing_check_run_id}", token, update_payload)
        else:
            await github_api_fn("POST", f"/repos/{owner}/{repo}/check-runs", token,
                                {"name": UNRESOLVED_CONVERSATIONS_CHECK_NAME, "head_sha": head_sha, **update_payload})
    marker = UNRESOLVED_CONVERSATIONS_MARKER
    existing_comment_id = None
    page = 1
    while True:
        resp_comments = await github_api_fn(
            "GET", f"/repos/{owner}/{repo}/issues/{number}/comments?per_page=100&page={page}", token,
        )
        if resp_comments.status != 200:
            break
        comments = json.loads(await resp_comments.text())
        if not comments:
            break
        for comment in comments:
            if marker in comment.get("body", ""):
                existing_comment_id = comment["id"]
                break
        if existing_comment_id is not None:
            break
        page += 1
    if unresolved:
        comment_body = (
            f"{marker}\n"
            f"⚠️ This pull request has **{unresolved_count} unresolved review "
            f"{noun}** that must be resolved before merging."
        )
        if existing_comment_id is not None:
            await github_api_fn(
                "PATCH", f"/repos/{owner}/{repo}/issues/comments/{existing_comment_id}", token, {"body": comment_body},
            )
        else:
            await create_comment_fn(owner, repo, number, comment_body, token)
    elif existing_comment_id is not None:
        await github_api_fn("DELETE", f"/repos/{owner}/{repo}/issues/comments/{existing_comment_id}", token)


def is_excluded_reviewer(login: str) -> bool:
    """Return True if the reviewer is a bot or automated account.

    Mirrors the _is_bot() logic in worker.py — keep both in sync when adding
    new bot patterns.
    """
    if not login:
        return True
    login_lower = login.lower()
    excluded_exact = {
        "coderabbitai[bot]",
        "dependabot[bot]",
        "dependabot-preview[bot]",
        "dependabot",
        "github-actions[bot]",
    }
    if login_lower in excluded_exact:
        return True
    bot_patterns = [
        "[bot]", "bot]", "copilot", "renovate", "actions-user",
        "coderabbit", "coderabbitai", "sentry", "snyk", "sonarcloud", "codecov",
    ]
    return any(pattern in login_lower for pattern in bot_patterns)


async def get_valid_reviewers(
    owner: str, repo: str, pr_number: int, pr_author: str, token: str, github_api_fn
) -> list:
    """Get list of valid approved reviewers for a PR (excluding bots and the PR author)."""
    from js import console
    reviewer_latest_state = {}
    page = 1
    while True:
        resp = await github_api_fn(
            "GET", f"/repos/{owner}/{repo}/pulls/{pr_number}/reviews?per_page=100&page={page}", token,
        )
        if resp.status != 200:
            console.error(f"[BLT] Failed to fetch reviews for PR #{pr_number}: {resp.status}")
            break
        reviews = json.loads(await resp.text())
        if not reviews:
            break
        for review in reviews:
            reviewer_login = review.get("user", {}).get("login", "")
            state = review.get("state", "")
            if reviewer_login:
                reviewer_latest_state[reviewer_login] = state
        page += 1
    valid_reviewers = set()
    for reviewer_login, state in reviewer_latest_state.items():
        if state != "APPROVED":
            continue
        if reviewer_login == pr_author:
            continue
        if is_excluded_reviewer(reviewer_login):
            continue
        valid_reviewers.add(reviewer_login)
    return list(valid_reviewers)


async def update_peer_review_labels(
    owner: str, repo: str, pr_number: int, has_review: bool, token: str, github_api_fn
) -> None:
    """Add/remove peer review labels based on whether the PR has a valid review."""
    new_label = "has-peer-review" if has_review else "needs-peer-review"
    old_label = "needs-peer-review" if has_review else "has-peer-review"
    color = "0e8a16" if has_review else "e74c3c"
    description = "PR has received peer review" if has_review else "PR needs peer review"
    await ensure_label_exists_with_description(owner, repo, new_label, color, description, token, github_api_fn)
    resp = await github_api_fn("GET", f"/repos/{owner}/{repo}/issues/{pr_number}/labels", token)
    if resp.status != 200:
        return
    current_labels = json.loads(await resp.text())
    current_label_names = {label.get("name") for label in current_labels}
    if old_label in current_label_names:
        await github_api_fn("DELETE", f"/repos/{owner}/{repo}/issues/{pr_number}/labels/{quote(old_label, safe='')}", token)
    if new_label not in current_label_names:
        await github_api_fn("POST", f"/repos/{owner}/{repo}/issues/{pr_number}/labels", token, {"labels": [new_label]})


async def check_peer_review_and_comment(
    owner: str, repo: str, pr_number: int, pr_author: str, token: str,
    github_api_fn, create_comment_fn
) -> None:
    """Check if a PR has peer review, update labels, and post a comment if needed."""
    if is_excluded_reviewer(pr_author):
        return
    reviewers = await get_valid_reviewers(owner, repo, pr_number, pr_author, token, github_api_fn)
    has_review = len(reviewers) > 0
    await update_peer_review_labels(owner, repo, pr_number, has_review, token, github_api_fn)
    if not has_review:
        marker = "<!-- peer-review-check -->"
        already_commented = False
        page = 1
        while True:
            resp = await github_api_fn(
                "GET", f"/repos/{owner}/{repo}/issues/{pr_number}/comments?per_page=100&page={page}", token,
            )
            if resp.status != 200:
                break
            comments = json.loads(await resp.text())
            if not comments:
                break
            if any(marker in comment.get("body", "") for comment in comments):
                already_commented = True
                break
            page += 1
        if not already_commented:
            body = f"""{marker}
        
