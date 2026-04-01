import json
import time
from typing import Optional

from js import console
from core.github_client import github_api, create_comment, _is_maintainer, _ensure_label_exists
from core.db import _d1_binding, _d1_run
from models.mentor import _find_assigned_mentor_from_comments, _select_mentor, _get_last_human_activity_ts, _is_security_issue, _fetch_mentors_config, MENTOR_ASSIGNED_LABEL, MENTOR_STALE_DAYS, _SECONDS_PER_DAY, NEEDS_MENTOR_LABEL, MENTOR_LABEL_COLOR, MENTOR_ASSIGNED_LABEL_COLOR
from models.assignment import _d1_record_mentor_assignment, _d1_remove_mentor_assignment
from models.leaderboard import _ensure_leaderboard_schema


async def _assign_mentor_to_issue(
    owner: str,
    repo: str,
    issue: dict,
    contributor_login: str,
    token: str,
    mentors_config: Optional[list] = None,
    exclude: Optional[str] = None,
    env=None,
) -> bool:
    """Assign a mentor from the pool to an issue.

    Steps:
    1. Reject security-sensitive issues.
    2. Skip if the issue already has the ``mentor-assigned`` label.
    3. Select a mentor via capacity-aware round-robin (D1 load map preferred).
    4. Ensure the ``needs-mentor`` and ``mentor-assigned`` labels exist, then apply
       ``mentor-assigned`` to the issue.
    5. Add the mentor as a GitHub assignee.
    6. Post a welcome comment with a hidden ``blt-mentor-assigned`` marker.
    7. Record the assignment in D1 ``mentor_assignments`` table.

    Returns ``True`` on success, ``False`` otherwise.
    """
    issue_number = issue["number"]

    if _is_security_issue(issue):
        console.log(
            f"[MentorPool] Skipping security issue {owner}/{repo}#{issue_number}"
        )
        return False

    current_labels = {lb.get("name", "").lower() for lb in issue.get("labels", [])}
    if MENTOR_ASSIGNED_LABEL.lower() in current_labels:
        console.log(
            f"[MentorPool] Mentor already assigned to {owner}/{repo}#{issue_number}"
        )
        return False

    issue_label_names = [lb.get("name", "") for lb in issue.get("labels", [])]
    mentor = await _select_mentor(
        owner, token, issue_label_names, mentors_config, exclude=exclude, env=env
    )

    if mentor is None:
        await create_comment(
            owner,
            repo,
            issue_number,
            "👋 A mentor was requested for this issue, but all mentors are currently "
            "at capacity. Please check back soon or ask for guidance in the "
            "[OWASP Slack](https://owasp.slack.com/archives/C0DKR6LAW).",
            token,
        )
        return False

    mentor_username = mentor["github_username"]

    # Ensure labels exist in the repo before applying them.
    await _ensure_label_exists(owner, repo, NEEDS_MENTOR_LABEL, MENTOR_LABEL_COLOR, token)
    await _ensure_label_exists(
        owner, repo, MENTOR_ASSIGNED_LABEL, MENTOR_ASSIGNED_LABEL_COLOR, token
    )

    # Apply mentor-assigned label.
    await github_api(
        "POST",
        f"/repos/{owner}/{repo}/issues/{issue_number}/labels",
        token,
        {"labels": [MENTOR_ASSIGNED_LABEL]},
    )

    specialties_info = ""
    if mentor.get("specialties"):
        specialties_info = f" (specialties: {', '.join(mentor['specialties'])})"

    contributor_mention = f"@{contributor_login}" if contributor_login else "the contributor"
    body = (
        f"<!-- blt-mentor-assigned: @{mentor_username} -->\n"
        f"🎓 A mentor has been assigned to this issue!\n\n"
        f"**Mentor:** @{mentor_username}{specialties_info}\n"
        f"**Contributor:** {contributor_mention}\n\n"
        f"@{mentor_username} — please provide guidance and support. "
        f"Use `/handoff` if you need to transfer mentorship.\n\n"
        f"{contributor_mention} — @{mentor_username} will help you through this. "
        "Feel free to ask questions here. Use `/rematch` if you need a different mentor.\n\n"
        "Happy coding! 🚀 — [OWASP BLT-Pool](https://pool.owaspblt.org)"
    )
    await create_comment(owner, repo, issue_number, body, token)
    console.log(
        f"[MentorPool] Assigned @{mentor_username} as mentor for {owner}/{repo}#{issue_number}"
    )

    # Record assignment in D1 so _get_mentor_load_map can use D1 instead of GitHub API.
    db = _d1_binding(env)
    if db:
        try:
            await _ensure_leaderboard_schema(db)
            await _d1_record_mentor_assignment(db, owner, mentor_username, repo, issue_number, mentee_login=contributor_login or "")
        except Exception as exc:
            console.error(f"[MentorPool] Failed to record assignment in D1 (best-effort): {exc}")

    return True

async def handle_mentor_command(
    owner: str,
    repo: str,
    issue: dict,
    login: str,
    token: str,
    mentors_config: Optional[list] = None,
    env=None,
) -> None:
    """Handle the ``/mentor`` slash command (contributor requests mentorship)."""
    issue_number = issue["number"]
    current_labels = {lb.get("name", "").lower() for lb in issue.get("labels", [])}
    if MENTOR_ASSIGNED_LABEL.lower() in current_labels:
        await create_comment(
            owner,
            repo,
            issue_number,
            f"@{login} This issue already has a mentor assigned. "
            "Use `/rematch` if you'd like a different mentor.",
            token,
        )
        return
    await _assign_mentor_to_issue(owner, repo, issue, login, token, mentors_config, env=env)

async def handle_mentor_unassign(
    owner: str,
    repo: str,
    issue: dict,
    login: str,
    token: str,
    env=None,
) -> None:
    """Handle the ``/unmentor`` slash command (undo an accidental /mentor request).

    Removes the mentor assignment from the issue by:
    - Removing the ``mentor-assigned`` label.
    - Removing the mentor from GitHub assignees.
    - Deleting the D1 assignment record.
    - Posting a confirmation comment.

    The issue author, the currently assigned mentor, or any repo maintainer
    (admin or maintain permission) may use this command.
    """
    issue_number = issue["number"]
    current_labels = {lb.get("name", "").lower() for lb in issue.get("labels", [])}
    if MENTOR_ASSIGNED_LABEL.lower() not in current_labels:
        await create_comment(
            owner,
            repo,
            issue_number,
            f"@{login} This issue does not have a mentor assigned. "
            "Use `/mentor` to request one.",
            token,
        )
        return

    # Identify the currently assigned mentor from hidden comment marker.
    current_mentor = await _find_assigned_mentor_from_comments(
        owner, repo, issue_number, token
    )

    # Permission check: allow the issue author, the assigned mentor, or any
    # repo maintainer (admin/maintain) to unmentor.  The maintainer check calls
    # the GitHub API so we skip it when one of the cheaper conditions already
    # grants access.
    issue_author = (issue.get("user") or {}).get("login", "")
    is_issue_author = login.lower() == issue_author.lower()
    is_assigned_mentor = current_mentor and login.lower() == current_mentor.lower()
    if not is_issue_author and not is_assigned_mentor:
        is_repo_maintainer = await _is_maintainer(owner, repo, login, token)
        if not is_repo_maintainer:
            await create_comment(
                owner,
                repo,
                issue_number,
                f"@{login} Only the issue author, the assigned mentor, or a repo maintainer "
                "can remove a mentor assignment. "
                "Use `/rematch` if you'd like a different mentor.\n\n"
                "— [OWASP BLT-Pool](https://pool.owaspblt.org)",
                token,
            )
            return

    # Remove the mentor-assigned label (best-effort).
    try:
        await github_api(
            "DELETE",
            f"/repos/{owner}/{repo}/issues/{issue_number}/labels/{MENTOR_ASSIGNED_LABEL}",
            token,
        )
    except Exception as exc:
        console.error(f"[MentorPool] Failed to remove mentor-assigned label (best-effort): {exc}")

    # Remove the mentor from GitHub assignees (best-effort).
    if current_mentor:
        try:
            await github_api(
                "DELETE",
                f"/repos/{owner}/{repo}/issues/{issue_number}/assignees",
                token,
                {"assignees": [current_mentor]},
            )
        except Exception as exc:
            console.error(f"[MentorPool] Failed to remove mentor assignee (best-effort): {exc}")

    # Remove D1 assignment record (best-effort).
    db = _d1_binding(env)
    if db:
        try:
            await _d1_remove_mentor_assignment(db, owner, repo, issue_number)
        except Exception as exc:
            console.error(f"[MentorPool] Failed to remove D1 assignment record (best-effort): {exc}")

    mentor_mention = f"@{current_mentor} " if current_mentor else ""
    await create_comment(
        owner,
        repo,
        issue_number,
        f"<!-- blt-mentor-unassigned -->\n"
        f"@{login} The mentor assignment has been cancelled. {mentor_mention}"
        "The issue is now open for mentorship again — use `/mentor` to request a new mentor.\n\n"
        "— [OWASP BLT-Pool](https://pool.owaspblt.org)",
        token,
    )
    console.log(
        f"[MentorPool] Mentor assignment cancelled by @{login} for {owner}/{repo}#{issue_number}"
    )

async def handle_mentor_pause(
    owner: str,
    repo: str,
    issue: dict,
    login: str,
    token: str,
    mentors_config: Optional[list] = None,
    env=None,
) -> None:
    """Handle the ``/mentor-pause`` slash command (mentor opts out of new assignments).

    Because mentor state is stored in D1, this handler acknowledges the request
    and pauses the mentor by updating their ``active`` flag in the database.
    """
    pool = mentors_config if mentors_config is not None else []
    # Only active mentors can pause; inactive ones already aren't receiving assignments.
    mentor_usernames = {
        m.get("github_username", "").lower()
        for m in pool
        if m.get("github_username") and m.get("active", True)
    }
    if login.lower() not in mentor_usernames:
        await create_comment(
            owner,
            repo,
            issue["number"],
            f"@{login} The `/mentor-pause` command is only available to active mentors.",
            token,
        )
        return
    await create_comment(
        owner,
        repo,
        issue["number"],
        f"@{login} Your pause request has been noted. 🙏\n\n"
        "Your availability has been paused in the mentor pool. "
        "The system will not select you for new assignments until you resume.\n\n"
        "Contact a maintainer if you need to resume your availability.",
        token,
    )
    # Persist the pause to D1 so _select_mentor() respects it immediately.
    db = _d1_binding(env)
    if db:
        try:
            await _d1_run(
                db,
                "UPDATE mentors SET active = 0 WHERE lower(github_username) = lower(?)",
                (login,),
            )
            console.log(f"[MentorPool] Paused mentor @{login} in D1")
        except Exception as exc:
            console.error(f"[MentorPool] Failed to persist pause for @{login} in D1 (best-effort): {exc}")
    else:
        console.error(f"[MentorPool] No D1 binding available; pause for @{login} was not persisted.")

async def handle_mentor_handoff(
    owner: str,
    repo: str,
    issue: dict,
    login: str,
    token: str,
    mentors_config: Optional[list] = None,
    env=None,
) -> None:
    """Handle the ``/handoff`` slash command (mentor transfers mentorship to a new mentor)."""
    issue_number = issue["number"]
    pool = mentors_config if mentors_config is not None else []
    mentor_usernames = {
        m.get("github_username", "").lower()
        for m in pool
        if m.get("github_username")
    }
    # First gate: check that the commenter is in the mentor pool at all (any entry).
    # The second gate below verifies they are specifically the *assigned* mentor for
    # this issue.  Having two separate gates gives a clearer error message to
    # non-mentor users vs. mentor-pool members who are not assigned here.
    if login.lower() not in mentor_usernames:
        await create_comment(
            owner,
            repo,
            issue_number,
            f"@{login} The `/handoff` command is only available to assigned mentors.",
            token,
        )
        return

    current_mentor = await _find_assigned_mentor_from_comments(
        owner, repo, issue_number, token
    )
    # Require a confirmed current mentor before proceeding; if the marker is missing
    # (API failure or marker never posted) we cannot safely authorize the handoff.
    if not current_mentor:
        await create_comment(
            owner,
            repo,
            issue_number,
            f"@{login} Unable to confirm the currently assigned mentor for this issue. "
            "Please contact a maintainer for assistance with the handoff.",
            token,
        )
        return
    if current_mentor.lower() != login.lower():
        await create_comment(
            owner,
            repo,
            issue_number,
            f"@{login} You are not the currently assigned mentor for this issue "
            f"(@{current_mentor} is). Only the assigned mentor can use `/handoff`.",
            token,
        )
        return

    # Determine contributor login from existing assignees (skip mentor usernames).
    contributor = None
    for assignee in issue.get("assignees", []):
        a_login = assignee.get("login", "")
        if a_login.lower() not in mentor_usernames and a_login.lower() != login.lower():
            contributor = a_login
            break

    # Build a temporary issue view with the mentor-assigned label stripped so the
    # assignment check inside _assign_mentor_to_issue does not abort early.
    updated_issue = {
        **issue,
        "labels": [
            lb for lb in issue.get("labels", [])
            if lb.get("name", "").lower() != MENTOR_ASSIGNED_LABEL.lower()
        ],
    }

    # Select and assign the replacement mentor BEFORE removing current state so
    # that if no mentor is available the issue is not left in an unmentored state.
    assigned = await _assign_mentor_to_issue(
        owner, repo, updated_issue, contributor or "", token, pool, exclude=login, env=env
    )
    if not assigned:
        await create_comment(
            owner,
            repo,
            issue_number,
            f"@{login} Handoff request noted, but no other mentor is currently available. "
            "Please reach out on [OWASP Slack](https://owasp.slack.com/archives/C0DKR6LAW) "
            "for assistance.",
            token,
        )
        return

    # Replacement assigned successfully — the outgoing mentor's label was already
    # replaced by _assign_mentor_to_issue; no assignee record to clean up.
    console.log(
        f"[MentorPool] Handoff from @{login} completed for {owner}/{repo}#{issue_number}"
    )

async def handle_mentor_rematch(
    owner: str,
    repo: str,
    issue: dict,
    login: str,
    token: str,
    mentors_config: Optional[list] = None,
    env=None,
) -> None:
    """Handle the ``/rematch`` slash command (contributor requests a different mentor)."""
    issue_number = issue["number"]
    pool = mentors_config if mentors_config is not None else []
    current_labels = {lb.get("name", "").lower() for lb in issue.get("labels", [])}
    if MENTOR_ASSIGNED_LABEL.lower() not in current_labels:
        await create_comment(
            owner,
            repo,
            issue_number,
            f"@{login} This issue does not have a mentor assigned yet. "
            "Use `/mentor` to request one.",
            token,
        )
        return

    current_mentor = await _find_assigned_mentor_from_comments(
        owner, repo, issue_number, token
    )

    # Authorization gate: only the issue author, the assigned mentor, or a repo
    # maintainer may force a rematch (mirrors the check in handle_mentor_unassign).
    issue_author = (issue.get("user") or {}).get("login", "")
    is_issue_author = login.lower() == issue_author.lower()
    is_assigned_mentor = current_mentor and login.lower() == current_mentor.lower()
    if not is_issue_author and not is_assigned_mentor:
        is_repo_maintainer = await _is_maintainer(owner, repo, login, token)
        if not is_repo_maintainer:
            await create_comment(
                owner,
                repo,
                issue_number,
                f"@{login} Only the issue author, the assigned mentor, or a repo maintainer "
                "can request a rematch.\n\n"
                "— [OWASP BLT-Pool](https://pool.owaspblt.org)",
                token,
            )
            return

    # Build a temporary issue view with the mentor-assigned label stripped so the
    # assignment check inside _assign_mentor_to_issue does not abort early.
    updated_issue = {
        **issue,
        "labels": [
            lb for lb in issue.get("labels", [])
            if lb.get("name", "").lower() != MENTOR_ASSIGNED_LABEL.lower()
        ],
    }

    # Attempt to assign a replacement mentor BEFORE removing old state so that
    # if no mentor is available the issue stays in a mentored state.
    assigned = await _assign_mentor_to_issue(
        owner,
        repo,
        updated_issue,
        login,
        token,
        pool,
        exclude=current_mentor,
        env=env,
    )
    if not assigned:
        # _assign_mentor_to_issue already posted a "no mentor available" comment.
        console.log(
            f"[MentorPool] Rematch for @{login} on {owner}/{repo}#{issue_number} "
            "aborted: no replacement mentor available"
        )
        return

    # Replacement assigned — _assign_mentor_to_issue already applied the label
    # and posted the assignment comment.  No old assignee or label to clean up.
    console.log(
        f"[MentorPool] Rematch completed for @{login} on {owner}/{repo}#{issue_number}"
    )

async def _check_stale_mentor_assignments(owner: str, repo: str, token: str, env=None) -> None:
    """Unassign mentors from issues that have been inactive for MENTOR_STALE_DAYS days.

    Iterates over open issues that carry the ``mentor-assigned`` label.  When the
    issue's ``updated_at`` timestamp is older than the stale threshold the mentor is
    unassigned, the ``mentor-assigned`` label is removed, and an explanatory comment
    is posted.
    """
    try:
        stale_threshold = MENTOR_STALE_DAYS * _SECONDS_PER_DAY
        current_time = time.time()
        per_page = 100
        max_pages = 10  # Conservative limit to avoid excessive subrequests.
        page = 1

        while page <= max_pages:
            resp = await github_api(
                "GET",
                f"/repos/{owner}/{repo}/issues"
                f"?state=open&labels={MENTOR_ASSIGNED_LABEL}&per_page={per_page}&page={page}",
                token,
            )
            if resp.status != 200:
                break

            issues = json.loads(await resp.text())
            if not issues:
                break

            for issue in issues:
                if "pull_request" in issue:
                    continue
                issue_number = issue["number"]
                # Use the last human (non-bot) comment timestamp as the activity
                # signal so that bot-posted comments (e.g. mentor assignment
                # notices) don't reset the stale clock.
                last_human_ts = await _get_last_human_activity_ts(
                    owner, repo, issue_number, issue, token
                )
                if not last_human_ts:
                    continue
                if current_time - last_human_ts <= stale_threshold:
                    continue

                # Issue is stale — identify the mentor from the hidden comment marker.
                current_mentor = await _find_assigned_mentor_from_comments(
                    owner, repo, issue_number, token
                )

                # Remove the mentor-assigned label.
                await github_api(
                    "DELETE",
                    f"/repos/{owner}/{repo}/issues/{issue_number}/labels/{MENTOR_ASSIGNED_LABEL}",
                    token,
                )

                # Free the mentor's capacity slot in D1 (best-effort).
                db = _d1_binding(env)
                if db:
                    try:
                        await _d1_remove_mentor_assignment(db, owner, repo, issue_number)
                    except Exception as exc:
                        console.error(
                            f"[MentorPool] Failed to remove stale D1 assignment for "
                            f"{owner}/{repo}#{issue_number} (best-effort): {exc}"
                        )

                days_elapsed = int((current_time - last_human_ts) / _SECONDS_PER_DAY)
                mentor_mention = f"@{current_mentor} " if current_mentor else ""
                await create_comment(
                    owner,
                    repo,
                    issue_number,
                    f"{mentor_mention}This issue has had no activity for {days_elapsed} days "
                    f"so the mentor assignment has been automatically released. "
                    "The issue remains open — use `/mentor` to request a new mentor when work resumes.\n\n"
                    "— [OWASP BLT-Pool](https://pool.owaspblt.org)",
                    token,
                )
                console.log(
                    f"[MentorPool] Released stale mentor assignment on {owner}/{repo}#{issue_number}"
                )

            if len(issues) < per_page:
                break

            page += 1
    except Exception as exc:
        console.error(f"[MentorPool] Error checking stale mentors in {owner}/{repo}: {exc}")
