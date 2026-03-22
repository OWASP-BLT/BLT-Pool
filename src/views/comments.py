import json
import calendar
import time
from urllib.parse import quote
from typing import Optional

from js import console
from core.github_client import github_api, create_comment
from core.github_client import _is_bot
from models.leaderboard import _calculate_leaderboard_stats_from_d1

LEADERBOARD_MARKER = "<!-- leaderboard-bot -->"
REVIEWER_LEADERBOARD_MARKER = "<!-- reviewer-leaderboard-bot -->"
MERGED_PR_COMMENT_MARKER = "<!-- merged-pr-comment-bot -->"
MAX_OPEN_PRS_PER_AUTHOR = 15


def _parse_github_timestamp(ts_str: str) -> int:
    """Parse GitHub ISO 8601 timestamp to Unix timestamp."""
    # GitHub timestamps are like: 2024-03-05T12:34:56Z
    import re
    match = re.match(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z", ts_str)
    if match:
        year, month, day, hour, minute, second = map(int, match.groups())
        dt = time.struct_time((year, month, day, hour, minute, second, 0, 0, 0))
        return int(calendar.timegm(dt))
    return 0

def _avatar_img_tag(login: str, size: int = 20) -> str:
    """Return a fixed-size GitHub avatar image tag safe for markdown tables."""
    safe_login = quote(str(login), safe="")
    return (
        f"<img src=\"https://avatars.githubusercontent.com/{safe_login}?size={size}&v=4\" "
        f"width=\"{size}\" height=\"{size}\" alt=\"{login}\" />"
    )

def _format_leaderboard_comment(author_login: str, leaderboard_data: dict, owner: str, note: str = "") -> str:
    """Format a leaderboard comment for a specific user."""
    sorted_users = leaderboard_data["sorted"]
    start_ts = leaderboard_data["start_timestamp"]
    
    # Find author's index
    author_index = -1
    for i, user in enumerate(sorted_users):
        if user["login"] == author_login:
            author_index = i
            break
    
    # Format month display
    month_struct = time.gmtime(start_ts)
    display_month = time.strftime("%B %Y", month_struct)
    
    # Build comment
    comment = LEADERBOARD_MARKER + "\n"
    comment += "## 📊 Monthly Leaderboard\n\n"
    comment += f"Hi @{author_login}! Here's how you rank for {display_month}:\n\n"
    
    # Table header
    comment += "| Rank | User | Open PRs | PRs (merged) | PRs (closed) | Reviews | Comments | Total |\n"
    comment += "| --- | --- | --- | --- | --- | --- | --- | --- |\n"
    
    def row_for(rank: int, u: dict, bold: bool = False, medal: str = "") -> str:
        avatar = _avatar_img_tag(u["login"])
        user_cell = f"{avatar} **`@{u['login']}`** ✨" if bold else f"{avatar} `@{u['login']}`"
        rank_cell = f"{medal} {rank}" if medal else f"{rank}"
        return (f"| {rank_cell} | {user_cell} | {u['openPrs']} | {u['mergedPrs']} | "
                f"{u['closedPrs']} | {u['reviews']} | {u['comments']} | **{u['total']}** |")
    
    # Show context rows around the author
    if not sorted_users:
        # No data yet: show the requesting user with zeroes so the comment is still useful.
        avatar = _avatar_img_tag(author_login)
        comment += f"| - | {avatar} **`@{author_login}`** ✨ | 0 | 0 | 0 | 0 | 0 | **0** |\n"
        comment += "\n_No leaderboard activity has been recorded for this month yet._\n"
    elif author_index == -1:
        # Author not in leaderboard, show top 5
        for i in range(min(5, len(sorted_users))):
            medal = ["🥇", "🥈", "🥉"][i] if i < 3 else ""
            comment += row_for(i + 1, sorted_users[i], False, medal) + "\n"
    else:
        # Show author and neighbors
        if author_index > 0:
            medal = ["🥇", "🥈", "🥉"][author_index - 1] if author_index - 1 < 3 else ""
            comment += row_for(author_index, sorted_users[author_index - 1], False, medal) + "\n"
        
        medal = ["🥇", "🥈", "🥉"][author_index] if author_index < 3 else ""
        comment += row_for(author_index + 1, sorted_users[author_index], True, medal) + "\n"
        
        if author_index < len(sorted_users) - 1:
            comment += row_for(author_index + 2, sorted_users[author_index + 1]) + "\n"
    
    comment += "\n---\n"
    comment += (
        f"**Scoring this month** (across {owner} org): Open PRs (+1 each), Merged PRs (+10), "
        "Closed (not merged) (−2), Reviews (+5; first two per PR in-month), "
        "Comments (+2, excludes CodeRabbit). Run `/leaderboard` on any issue or PR to see your rank!\n"
    )
    if note:
        comment += f"\n> Note: {note}\n"
    
    return comment

def _format_reviewer_leaderboard_comment(leaderboard_data: dict, owner: str, pr_reviewers: list = None) -> str:
    """Format a reviewer leaderboard comment showing top reviewers for the month."""
    sorted_users = leaderboard_data["sorted"]
    start_ts = leaderboard_data["start_timestamp"]

    # Sort users by reviews descending, then alphabetically
    reviewer_sorted = sorted(
        [u for u in sorted_users if u["reviews"] > 0],
        key=lambda u: (-u["reviews"], u["login"].lower()),
    )

    # Format month display
    month_struct = time.gmtime(start_ts)
    display_month = time.strftime("%B %Y", month_struct)

    comment = REVIEWER_LEADERBOARD_MARKER + "\n"
    comment += "## 🔍 Reviewer Leaderboard\n\n"
    comment += f"Top reviewers for {display_month} (across the {owner} org):\n\n"

    medals = ["🥇", "🥈", "🥉"]

    def row_for(rank: int, u: dict, highlight: bool = False) -> str:
        medal = medals[rank - 1] if rank <= 3 else ""
        rank_cell = f"{medal} {rank}" if medal else f"{rank}"
        avatar = _avatar_img_tag(u["login"])
        user_cell = f"{avatar} **`@{u['login']}`** ⭐" if highlight else f"{avatar} `@{u['login']}`"
        return f"| {rank_cell} | {user_cell} | {u['reviews']} |"

    comment += "| Rank | Reviewer | Reviews this month |\n"
    comment += "| --- | --- | --- |\n"

    pr_reviewer_set = set(pr_reviewers or [])

    if not reviewer_sorted:
        comment += "| - | _No review activity recorded yet_ | 0 |\n"
    else:
        total = len(reviewer_sorted)

        # Find the highest-ranked PR reviewer to centre the window on.
        center_idx = None
        if pr_reviewer_set:
            for i, u in enumerate(reviewer_sorted):
                if u["login"] in pr_reviewer_set:
                    center_idx = i
                    break

        if center_idx is not None:
            # Build a window of up to 5 entries with the reviewer in the middle.
            start_idx = center_idx - 2
            end_idx = center_idx + 2
            # Clamp and expand to keep window size = 5 when possible.
            if start_idx < 0:
                end_idx -= start_idx  # shift right
                start_idx = 0
            if end_idx >= total:
                shift = end_idx - total + 1
                start_idx = max(0, start_idx - shift)
                end_idx = total - 1

            if start_idx > 0:
                comment += "| … | … | … |\n"
            for i in range(start_idx, end_idx + 1):
                u = reviewer_sorted[i]
                highlight = u["login"] in pr_reviewer_set
                comment += row_for(i + 1, u, highlight) + "\n"
            if end_idx < total - 1:
                comment += "| … | … | … |\n"
        else:
            # No PR reviewer identified – show top 5.
            for i, u in enumerate(reviewer_sorted[:5]):
                highlight = u["login"] in pr_reviewer_set
                comment += row_for(i + 1, u, highlight) + "\n"

    comment += "\n---\n"
    comment += (
        "Reviews earn **+5 points** each in the monthly leaderboard "
        "(first two reviewers per PR). Thank you to everyone who helps review PRs! 🙏\n"
    )
    return comment

async def _post_reviewer_leaderboard(owner: str, repo: str, pr_number: int, token: str, env=None, pr_reviewers: list = None) -> None:
    """Post or update a reviewer leaderboard comment on a merged PR."""
    leaderboard_data = None
    if env is not None:
        leaderboard_data = await _calculate_leaderboard_stats_from_d1(owner, env)
    if leaderboard_data is None:
        # Fallback: build minimal data from GitHub API is expensive; skip if unavailable.
        console.log(f"[ReviewerLeaderboard] No D1 data available for {owner}; skipping reviewer leaderboard")
        return

    comment_body = _format_reviewer_leaderboard_comment(leaderboard_data, owner, pr_reviewers)

    # Delete any existing reviewer leaderboard comment then post a fresh one
    resp = await github_api("GET", f"/repos/{owner}/{repo}/issues/{pr_number}/comments?per_page=100", token)
    if resp.status == 200:
        existing_comments = json.loads(await resp.text())
        for c in existing_comments:
            body = c.get("body") or ""
            if REVIEWER_LEADERBOARD_MARKER in body:
                delete_resp = await github_api(
                    "DELETE",
                    f"/repos/{owner}/{repo}/issues/comments/{c['id']}",
                    token,
                )
                if delete_resp.status not in (204, 200):
                    console.error(
                        f"[ReviewerLeaderboard] Failed to delete old reviewer leaderboard comment {c['id']} "
                        f"for {owner}/{repo}#{pr_number}: status={delete_resp.status}"
                    )

    await create_comment(owner, repo, pr_number, comment_body, token)
    console.log(f"[ReviewerLeaderboard] Posted reviewer leaderboard for {owner}/{repo}#{pr_number}")

async def _post_or_update_leaderboard(owner: str, repo: str, issue_number: int, author_login: str, token: str, env=None) -> None:
    """Post or update a leaderboard comment on an issue/PR."""
    console.log(f"[Leaderboard] Starting leaderboard post for {owner}/{repo}#{issue_number} by @{author_login}")

    leaderboard_data, leaderboard_note, is_org = await _fetch_leaderboard_data(owner, repo, token, env)

    if leaderboard_data is None:
        console.error(f"[Leaderboard] Owner lookup failed for {owner}; cannot post leaderboard")
        await create_comment(
            owner,
            repo,
            issue_number,
            f"@{author_login} I couldn't load leaderboard data right now (owner lookup failed). Please try again shortly.",
            token,
        )
        return
    
    # Format comment
    comment_body = _format_leaderboard_comment(author_login, leaderboard_data, owner, leaderboard_note)
    
    # Delete existing leaderboard comment(s) and old /leaderboard command comments, then create a fresh leaderboard comment.
    resp = await github_api("GET", f"/repos/{owner}/{repo}/issues/{issue_number}/comments?per_page=100", token)
    if resp.status == 200:
        comments = json.loads(await resp.text())
        for c in comments:
            body = c.get("body") or ""
            is_old_board = LEADERBOARD_MARKER in body
            is_command_comment = _extract_command(body) == LEADERBOARD_COMMAND
            if is_old_board or is_command_comment:
                delete_resp = await github_api(
                    "DELETE",
                    f"/repos/{owner}/{repo}/issues/comments/{c['id']}",
                    token,
                )
                if delete_resp.status not in (204, 200):
                    console.error(
                        f"[Leaderboard] Failed to delete old leaderboard/command comment {c['id']} "
                        f"for {owner}/{repo}#{issue_number}: status={delete_resp.status}"
                    )
    else:
        console.error(
            f"[Leaderboard] Failed to list comments for {owner}/{repo}#{issue_number}: "
            f"status={resp.status}; posting new leaderboard anyway"
        )

    await create_comment(owner, repo, issue_number, comment_body, token)
    console.log(f"[Leaderboard] Posted leaderboard comment for {owner}/{repo}#{issue_number} (requested by @{author_login})")

async def _check_and_close_excess_prs(owner: str, repo: str, pr_number: int, author_login: str, token: str) -> bool:
    """Check if author has too many open PRs and close if needed.
    
    Returns:
        True if PR was closed, False otherwise
    """
    # Search for open PRs by this author
    resp = await github_api(
        "GET",
        f"/search/issues?q=repo:{owner}/{repo}+is:pr+is:open+author:{author_login}&per_page=100",
        token
    )
    
    if resp.status != 200:
        return False
    
    data = json.loads(await resp.text())
    open_prs = data.get("items", [])
    
    # Exclude the current PR from count
    pre_existing_count = len([pr for pr in open_prs if pr["number"] != pr_number])
    
    if pre_existing_count >= MAX_OPEN_PRS_PER_AUTHOR:
        # Close the PR
        msg = (
            f"Hi @{author_login}, thanks for your contribution!\n\n"
            f"This PR is being auto-closed because you currently have {pre_existing_count} "
            f"open PRs in this repository (limit: {MAX_OPEN_PRS_PER_AUTHOR}).\n"
            "Please finish or close some existing PRs before opening new ones.\n\n"
            "If you believe this was closed in error, please contact the maintainers."
        )
        
        await create_comment(owner, repo, pr_number, msg, token)
        
        await github_api(
            "PATCH",
            f"/repos/{owner}/{repo}/pulls/{pr_number}",
            token,
            {"state": "closed"}
        )
        
        return True
    
    return False

async def _check_rank_improvement(owner: str, repo: str, pr_number: int, author_login: str, token: str) -> None:
    """Check if author's rank improved and post congratulatory message."""
    # Get org repos
    resp = await github_api("GET", f"/users/{owner}", token)
    if resp.status != 200:
        return
    
    owner_data = json.loads(await resp.text())
    is_org = owner_data.get("type") == "Organization"
    
    if is_org:
        repos = await _fetch_org_repos(owner, token)
    else:
        repos = [{"name": repo}]
    
    # Calculate 6-month window
    now = int(time.time())
    six_months_ago = now - (6 * 30 * 24 * 60 * 60)  # Approximate
    
    # Count merged PRs in 6-month window for all users
    merged_prs_per_author = {}
    
    # Limit repos to prevent subrequest errors
    for repo_obj in repos[:10]:
        repo_name = repo_obj["name"]
        resp = await github_api(
            "GET",
            f"/repos/{owner}/{repo_name}/pulls?state=closed&per_page=30&sort=updated&direction=desc",
            token
        )
        
        if resp.status == 200:
            prs = json.loads(await resp.text())
            for pr in prs:
                if pr.get("merged_at"):
                    merged_ts = _parse_github_timestamp(pr["merged_at"])
                    if merged_ts >= six_months_ago:
                        pr_author = pr.get("user")
                        if pr_author and not _is_bot(pr_author):
                            login = pr_author["login"]
                            merged_prs_per_author[login] = merged_prs_per_author.get(login, 0) + 1
    
    author_count = merged_prs_per_author.get(author_login, 0)
    
    if author_count == 0:
        return
    
    # Calculate new rank (number of users with more PRs + 1)
    new_rank = len([c for c in merged_prs_per_author.values() if c > author_count]) + 1
    
    # Calculate old rank (before this merge)
    prev_count = author_count - 1
    old_rank = None
    if prev_count > 0:
        old_rank = len([c for c in merged_prs_per_author.values() if c > prev_count]) + 1
    
    # Check if rank improved
    rank_improved = old_rank is None or new_rank < old_rank
    
    if not rank_improved:
        return
    
    # Post congratulatory message
    if old_rank is None:
        msg = (
            f"🎉 Congratulations @{author_login}! "
            f"You've entered the BLT PR leaderboard at **rank #{new_rank}** with this merged PR! "
            "Keep up the great work! 🚀"
        )
    else:
        msg = (
            f"🎉 Congratulations @{author_login}! "
            f"This merged PR has moved you up to **rank #{new_rank}** on the BLT PR leaderboard "
            f"(up from #{old_rank})! Keep up the great work! 🚀"
        )
    
    await create_comment(owner, repo, pr_number, msg, token)
