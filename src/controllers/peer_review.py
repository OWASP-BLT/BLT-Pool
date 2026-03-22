import json
from urllib.parse import quote

from js import console
from core.github_client import github_api, create_comment, _is_human


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
            await create_comment(owner, repo, pr_number, body, token)

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

    # Label PR with number of pending checks (queued/waiting/action_required).
    # Lazy import to avoid a circular import at module level (peer_review ↔ pr_handlers).
    from controllers.pr_handlers import _try_label_pending_checks  # noqa: PLC0415
    await _try_label_pending_checks(owner, repo, pr, token)
