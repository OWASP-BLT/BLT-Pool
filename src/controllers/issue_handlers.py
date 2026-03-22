import json
import time
import os
from typing import Optional

from js import console
from core.github_client import github_api, create_comment, create_reaction, _is_human, report_bug_to_blt, _extract_command, _ensure_label_exists
from controllers.mentor_commands import handle_mentor_command, handle_mentor_unassign, handle_mentor_pause, handle_mentor_handoff, handle_mentor_rematch, _assign_mentor_to_issue
from models.mentor import _fetch_mentors_config, NEEDS_MENTOR_LABEL
from views.comments import _post_or_update_leaderboard
from models.leaderboard import _track_comment_in_d1

ASSIGN_COMMAND = "/assign"
UNASSIGN_COMMAND = "/unassign"
APPROVE_COMMAND = "/approve"
DENY_COMMAND = "/deny"
LEADERBOARD_COMMAND = "/leaderboard"
MENTOR_COMMAND = "/mentor"
UNMENTOR_COMMAND = "/unmentor"
MENTOR_PAUSE_COMMAND = "/mentor-pause"
HANDOFF_COMMAND = "/handoff"
REMATCH_COMMAND = "/rematch"

MAX_ASSIGNEES = 1
ASSIGNMENT_DURATION_HOURS = 8
BUG_LABELS = {"bug", "vulnerability", "security"}
HELP_WANTED_LABEL = "help wanted"
TRIAGE_REVIEWER = "donnieblt"
NEEDS_APPROVAL_LABEL = "needs-approval"
NEEDS_APPROVAL_LABEL_COLOR = "e11d48"


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
    elif command == APPROVE_COMMAND:
        await _approve(owner, repo, issue, login, token)
    elif command == DENY_COMMAND:
        await _deny(owner, repo, issue, login, token)
    elif command == LEADERBOARD_COMMAND:
        console.log(f"[Leaderboard] Command received for {owner}/{repo}#{issue_number} by @{login}")
        # Best effort: remove the triggering command comment to keep threads clean.
        if env is not None and comment_id:
            delete_cmd_resp = await github_api(
                "DELETE",
                f"/repos/{owner}/{repo}/issues/comments/{comment_id}",
                token,
            )
            if delete_cmd_resp.status not in (204, 200):
                console.error(
                    f"[Leaderboard] Failed to delete triggering command comment {comment_id} "
                    f"for {owner}/{repo}#{issue_number}: status={delete_cmd_resp.status}"
                )
        try:
            if env is None:
                await _post_or_update_leaderboard(owner, repo, issue_number, login, token)
            else:
                await _post_or_update_leaderboard(owner, repo, issue_number, login, token, env)
        except Exception as exc:
            console.error(f"[Leaderboard] Command failed for {owner}/{repo}#{issue_number}: {exc}")
            await create_comment(
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
        await create_comment(
            owner, repo, num,
            f"@{login} This command only works on issues, not pull requests.",
            token,
        )
        return
    if issue["state"] == "closed":
        await create_comment(
            owner, repo, num,
            f"@{login} This issue is already closed and cannot be assigned.",
            token,
        )
        return
    assignees = [a["login"] for a in issue.get("assignees", [])]
    if login in assignees:
        await create_comment(
            owner, repo, num,
            f"@{login} You are already assigned to this issue.",
            token,
        )
        return
    if len(assignees) >= MAX_ASSIGNEES:
        await create_comment(
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
        await _ensure_label_exists(owner, repo, NEEDS_APPROVAL_LABEL, NEEDS_APPROVAL_LABEL_COLOR, token)
        await github_api(
            "POST",
            f"/repos/{owner}/{repo}/issues/{num}/labels",
            token,
            {"labels": [NEEDS_APPROVAL_LABEL]},
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
    await create_comment(
        owner, repo, num,
        f"@{login} You have been assigned to this issue! 🎉\n\n"
        f"Please submit a pull request within **{ASSIGNMENT_DURATION_HOURS} hours** "
        f"(by {deadline}).\n\n"
        f"If you need more time or cannot complete the work, please comment "
        f"`{UNASSIGN_COMMAND}` so others can pick it up.\n\n"
        "Happy coding! 🚀 — [OWASP BLT-Pool](https://pool.owaspblt.org)",
        token,
    )


async def _approve(
    owner: str, repo: str, issue: dict, login: str, token: str
) -> None:
    """Handle the ``/approve`` command (triage reviewer approves an issue for assignment).

    Only TRIAGE_REVIEWER is authorised. Adds the 'help wanted' label and assigns the opener.
    """
    num = issue["number"]
    if login.lower() != TRIAGE_REVIEWER.lower():
        await create_comment(
            owner, repo, num,
            f"@{login} Only @{TRIAGE_REVIEWER} can approve issues.",
            token,
        )
        return
    # Do not process /approve on pull requests or closed issues.
    if issue.get("pull_request") or issue.get("state") == "closed":
        return
    # Add the "help wanted" label so /assign can be used.
    await github_api(
        "POST",
        f"/repos/{owner}/{repo}/issues/{num}/labels",
        token,
        {"labels": [HELP_WANTED_LABEL]},
    )
    # Assign the opener, respecting MAX_ASSIGNEES.
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
            await github_api(
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
    await create_comment(
        owner, repo, num,
        f"✅ This issue has been approved by @{login}!\n\n"
        + assignment_text
        + f'The `"{HELP_WANTED_LABEL}"` label has been added so others can also use '
        f"`/assign` to claim this issue.",
        token,
    )


async def _deny(
    owner: str, repo: str, issue: dict, login: str, token: str
) -> None:
    """Handle the ``/deny`` command (triage reviewer rejects an issue).

    Only TRIAGE_REVIEWER is authorised. Closes the issue with an explanatory comment.
    """
    num = issue["number"]
    if login.lower() != TRIAGE_REVIEWER.lower():
        await create_comment(
            owner, repo, num,
            f"@{login} Only @{TRIAGE_REVIEWER} can deny issues.",
            token,
        )
        return
    if issue.get("pull_request"):
        await create_comment(
            owner, repo, num,
            f"@{login} The `/deny` command only works on issues, not pull requests.",
            token,
        )
        return
    if issue["state"] == "closed":
        await create_comment(
            owner, repo, num,
            f"@{login} This issue is already closed.",
            token,
        )
        return
    await create_comment(
        owner, repo, num,
        f"❌ This issue has been denied by @{login} and will be closed.\n\n"
        "If you believe this was a mistake, please open a new issue with more details.",
        token,
    )
    await github_api(
        "PATCH",
        f"/repos/{owner}/{repo}/issues/{num}",
        token,
        {"state": "closed"},
    )


async def _unassign(
    owner: str, repo: str, issue: dict, login: str, token: str
) -> None:
    num = issue["number"]
    assignees = [a["login"] for a in issue.get("assignees", [])]
    if login not in assignees:
        await create_comment(
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
    await create_comment(
        owner, repo, num,
        f"@{login} You have been unassigned from this issue. "
        "Thanks for letting us know! 👍\n\n"
        "The issue is now open for others to pick up.",
        token,
    )

_NO_WELCOME_REPOS_YML_PATH = os.path.join(os.path.dirname(__file__), "no_welcome_repos.yml")
_NO_WELCOME_REPOS_CACHE: Optional[list] = None

def _load_no_welcome_repos(path: str = _NO_WELCOME_REPOS_YML_PATH) -> list:
    """Return the list of repository names that should not receive the new-issue welcome message.

    Reads ``src/no_welcome_repos.yml`` which has the format::

        repos:
          - RepoName
          - AnotherRepo

    The result is cached in memory after the first read.
    """
    global _NO_WELCOME_REPOS_CACHE
    if path == _NO_WELCOME_REPOS_YML_PATH and _NO_WELCOME_REPOS_CACHE is not None:
        return _NO_WELCOME_REPOS_CACHE
    try:
        with open(path, "r", encoding="utf-8") as fh:
            content = fh.read()
    except OSError:
        if path == _NO_WELCOME_REPOS_YML_PATH:
            _NO_WELCOME_REPOS_CACHE = []
        return []
    repos: list = []
    in_repos_section = False
    for raw_line in content.splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped == "repos:":
            in_repos_section = True
            continue
        # If we hit another top-level key (non-indented line ending with ":"),
        # we are no longer in the "repos" section.
        if not line.startswith(" ") and stripped.endswith(":"):
            in_repos_section = False
            continue
        if in_repos_section and stripped.startswith("- "):
            repos.append(stripped[2:].strip())
    if path == _NO_WELCOME_REPOS_YML_PATH:
        _NO_WELCOME_REPOS_CACHE = repos
    return repos

async def handle_issue_opened(
    payload: dict, token: str, blt_api_url: str
) -> None:
    issue = payload["issue"]
    sender = payload["sender"]
    if not _is_human(sender):
        return
    owner = payload["repository"]["owner"]["login"]
    repo = payload["repository"]["name"]
    no_welcome_repos = {r.lower() for r in _load_no_welcome_repos()}
    if repo.lower() in no_welcome_repos:
        return
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
    await create_comment(owner, repo, issue["number"], msg, token)

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
        await create_comment(
            owner, repo, issue["number"],
            f"🐛 This issue has been reported to [OWASP BLT-Pool](https://pool.owaspblt.org) "
            f"(Bug ID: #{bug_data['id']}) after being labeled as "
            f"`{label.get('name', 'bug')}`.",
            token,
        )


