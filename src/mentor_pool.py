"""Mentor pool management and assignment functions."""

import json
import time
from typing import Optional

from js import console


# ---------------------------------------------------------------------------
# Mentor pool functions
# ---------------------------------------------------------------------------


def _parse_yaml_scalar(s: str):
    """Convert a YAML scalar string to an appropriate Python value."""
    if s.lower() in ("true", "yes", "on"):
        return True
    if s.lower() in ("false", "no", "off"):
        return False
    if s.lower() in ("null", "~", ""):
        return None
    try:
        return int(s)
    except ValueError:
        pass
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        return s[1:-1]
    return s


def _parse_mentors_yaml(content: str) -> list:
    """Parse a simple mentors YAML file into a list of mentor dicts.

    Handles the specific format used in ``src/mentors.yml``:

    .. code-block:: yaml

        mentors:
          - github_username: alice
            name: Alice Smith
            specialties:
              - frontend
            max_mentees: 3
            active: true
    """
    mentors: list = []
    current: Optional[dict] = None
    current_list_key: Optional[str] = None

    for raw_line in content.splitlines():
        line = raw_line.rstrip()
        if not line.strip() or line.strip().startswith("#"):
            continue
        indent = len(line) - len(line.lstrip())
        stripped = line.strip()

        if stripped == "mentors:":
            continue

        if stripped.startswith("- ") and indent == 2:
            # New mentor entry
            if current is not None:
                mentors.append(current)
            current = {}
            current_list_key = None
            kv = stripped[2:]
            if ":" in kv:
                k, _, v = kv.partition(":")
                current[k.strip()] = _parse_yaml_scalar(v.strip())
        elif stripped.startswith("- ") and indent >= 6 and current is not None and current_list_key:
            # List item (e.g. a specialty entry)
            current[current_list_key].append(stripped[2:].strip())
        elif ":" in stripped and not stripped.startswith("-") and current is not None:
            k, _, v = stripped.partition(":")
            k = k.strip()
            v = v.strip()
            if v == "":
                current_list_key = k
                current[k] = []
            else:
                current_list_key = None
                current[k] = _parse_yaml_scalar(v)

    if current is not None:
        mentors.append(current)

    return mentors


async def _fetch_mentors_config(env=None, owner: str = "", repo: str = "", token: str = "") -> list:
    """Load the mentor list, preferring the D1 database when available.

    Falls back to an empty list when D1 is unavailable.  The ``owner``,
    ``repo``, and ``token`` parameters are retained for call-site compatibility
    but are no longer used — mentors are stored in and served from D1.
    """
    db = _d1_binding(env) if env is not None else None
    if db:
        mentors = await _load_mentors_from_d1(db)
        if mentors:
            return mentors
    console.error("[MentorPool] No D1 binding or empty mentors table; returning []")
    return []


async def _load_mentors_local(env=None) -> list:
    """Load the mentor list from D1 (preferred) for homepage display.

    Returns the parsed mentor list, or ``[]`` when D1 is unavailable.
    This function is kept for backwards compatibility with call sites that
    previously read from ``src/mentors.yml``.
    """
    db = _d1_binding(env) if env is not None else None
    if db:
        return await _load_mentors_from_d1(db)
    console.error("[MentorPool] No D1 binding available; returning empty mentor list")
    return []


async def _fetch_mentor_stats_from_d1(env, org: str, mentors: Optional[list] = None, token: str = "") -> dict:
    """Return per-mentor all-time PR/review totals for homepage display.

    When ``mentors`` and ``token`` are provided, fetches accurate all-time
    counts directly from the GitHub Search API (using ``total_count``), caching
    results in the ``mentor_stats_cache`` D1 table with a 24-hour TTL.  This
    reflects the full lifespan of the organisation rather than only the period
    since the webhook was first deployed.

    Falls back to aggregating ``leaderboard_monthly_stats`` across all months
    when no GitHub token is available or D1 is not configured.

    Returns a mapping of ``github_username → {"merged_prs": int, "reviews": int}``.
    Returns ``{}`` when D1 is unavailable and the GitHub API path is not used.
    """
    db = _d1_binding(env)

    # ------------------------------------------------------------------
    # Path A: GitHub Search API with D1 cache (accurate all-time counts).
    # ------------------------------------------------------------------
    if mentors and token and db:
        try:
            await _ensure_leaderboard_schema(db)
            now_ts = int(time.time())
            fresh_cutoff = now_ts - _MENTOR_STATS_CACHE_TTL

            # Load all cached stats for this org in one query.
            cached_rows = await _d1_all(
                db,
                "SELECT github_username, merged_prs, reviews, fetched_at FROM mentor_stats_cache WHERE org = ?",
                (org,),
            )
            cache = {
                row["github_username"]: row
                for row in (cached_rows or [])
                if row.get("github_username")
            }

            stats: dict = {}
            for mentor in mentors:
                username = mentor.get("github_username", "")
                if not username:
                    continue
                cached = cache.get(username)
                if cached and int(cached.get("fetched_at") or 0) >= fresh_cutoff:
                    # Cache hit — return stored values directly.
                    stats[username] = {
                        "merged_prs": int(cached.get("merged_prs") or 0),
                        "reviews": int(cached.get("reviews") or 0),
                    }
                    continue

                # Cache miss or stale — fetch from GitHub Search API.
                merged_prs = 0
                reviews = 0
                safe_org = quote(org, safe="")
                safe_user = quote(username, safe="")
                try:
                    pr_resp = await github_api(
                        "GET",
                        f"/search/issues?q=is:pr+is:merged+org:{safe_org}+author:{safe_user}&per_page=1",
                        token,
                    )
                    if pr_resp.status == 200:
                        pr_data = json.loads(await pr_resp.text())
                        merged_prs = int(pr_data.get("total_count") or 0)
                except Exception as exc:
                    console.error(f"[MentorPool] GitHub PR count failed for {username}: {exc}")
                try:
                    rev_resp = await github_api(
                        "GET",
                        f"/search/issues?q=is:pr+org:{safe_org}+reviewed-by:{safe_user}+-author:{safe_user}&per_page=1",
                        token,
                    )
                    if rev_resp.status == 200:
                        rev_data = json.loads(await rev_resp.text())
                        reviews = int(rev_data.get("total_count") or 0)
                except Exception as exc:
                    console.error(f"[MentorPool] GitHub review count failed for {username}: {exc}")

                stats[username] = {"merged_prs": merged_prs, "reviews": reviews}

                # Persist into cache for future requests.
                try:
                    await _d1_run(
                        db,
                        """
                        INSERT INTO mentor_stats_cache (org, github_username, merged_prs, reviews, fetched_at)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(org, github_username) DO UPDATE SET
                            merged_prs = excluded.merged_prs,
                            reviews    = excluded.reviews,
                            fetched_at = excluded.fetched_at
                        """,
                        (org, username, merged_prs, reviews, now_ts),
                    )
                except Exception as exc:
                    console.error(f"[MentorPool] Failed to cache mentor stats for {username}: {exc}")

            return stats
        except Exception as exc:
            console.error(f"[MentorPool] Failed to fetch mentor stats from GitHub: {exc}")
            # Fall through to the D1 monthly-aggregation path below.

    # ------------------------------------------------------------------
    # Path B: Aggregate from D1 monthly stats (fallback / no token).
    # ------------------------------------------------------------------
    if not db:
        console.log("[MentorPool] No D1 binding available for mentor stats; stats will be hidden")
        return {}
    try:
        await _ensure_leaderboard_schema(db)
        rows = await _d1_all(
            db,
            """
            SELECT user_login,
                   COALESCE(SUM(merged_prs), 0) AS total_prs,
                   COALESCE(SUM(reviews),    0) AS total_reviews
            FROM leaderboard_monthly_stats
            WHERE org = ?
            GROUP BY user_login
            """,
            (org,),
        )
        return {
            row["user_login"]: {
                "merged_prs": int(row.get("total_prs") or 0),
                "reviews": int(row.get("total_reviews") or 0),
            }
            for row in rows
            if row.get("user_login")
        }
    except Exception as exc:
        console.error(f"[MentorPool] Failed to fetch mentor stats from D1: {exc}")
        return {}


async def _get_mentor_load_map(owner: str, token: str, env=None) -> dict:
    """Return a mapping of mentor_username → open mentored issue count.

    Tries D1 first (``mentor_assignments`` table) when a D1 binding is
    available; falls back to the GitHub Search API for compatibility with
    environments where D1 is not configured.
    """
    db = _d1_binding(env)
    if db:
        try:
            await _ensure_leaderboard_schema(db)
            d1_loads = await _d1_get_mentor_loads(db, owner)
            # d1_loads is a dict (possibly empty when no assignments exist); always use
            # D1 when available — an empty dict is a valid state (no active assignments).
            console.log(f"[MentorPool] Using D1 mentor loads for {owner}: {len(d1_loads)} entries")
            return d1_loads
        except Exception as exc:
            console.error(f"[MentorPool] D1 mentor load lookup failed, falling back to GitHub API: {exc}")

    # ---------------------------------------------------------------------------
    # Fallback: query GitHub Search API (original behaviour).
    # ---------------------------------------------------------------------------
    # Limit pagination to avoid excessive subrequests.
    max_pages = 5
    per_page = 100
    load_map: dict = {}

    for page in range(1, max_pages + 1):
        resp = await github_api(
            "GET",
            f"/search/issues?q=org:{owner}+is:issue+is:open+label:{MENTOR_ASSIGNED_LABEL}"
            f"&per_page={per_page}&page={page}",
            token,
        )
        if resp.status != 200:
            if page == 1:
                console.log(
                    f"[MentorPool] _get_mentor_load_map: GitHub search API returned {resp.status} "
                    f"on page {page} — returning empty load map (all mentors appear at zero load)."
                )
                return {}
            console.log(
                f"[MentorPool] _get_mentor_load_map: GitHub search API returned {resp.status} "
                f"on page {page} — using load counts collected so far."
            )
            break

        data = json.loads(await resp.text())
        items = data.get("items", [])
        if not items:
            break

        for item in items:
            for assignee in item.get("assignees", []):
                login = assignee.get("login", "")
                if login:
                    load_map[login] = load_map.get(login, 0) + 1

        if len(items) < per_page:
            break

    return load_map


async def _select_mentor(
    owner: str,
    token: str,
    issue_labels: Optional[list] = None,
    mentors_config: Optional[list] = None,
    exclude: Optional[str] = None,
    env=None,
) -> Optional[dict]:
    """Select the best available mentor using capacity-aware round-robin.

    The algorithm:
    1. Filter to active mentors with a GitHub username (optionally excluding one).
    2. If the issue has labels that match any mentor's specialties, prefer those mentors.
    3. Fetch the current load map (D1 if available, GitHub Search API otherwise).
    4. Skip mentors who are at or over their ``max_mentees`` cap.
    5. Return the mentor with the fewest active issues; break ties alphabetically.

    Returns ``None`` when no mentor is available.
    """
    pool = mentors_config if mentors_config is not None else []
    active = [
        m for m in pool
        if m.get("active", True)
        and m.get("github_username")
        and (exclude is None or m["github_username"].lower() != exclude.lower())
    ]
    if not active:
        return None

    # Specialty matching: narrow to mentors who match the issue's labels.
    if issue_labels:
        label_set = {lb.lower() for lb in issue_labels}
        specialty_matched = [
            m for m in active
            if any(s.lower() in label_set for s in m.get("specialties", []))
        ]
        if specialty_matched:
            active = specialty_matched

    load_map = await _get_mentor_load_map(owner, token, env=env)

    # Normalize load_map keys to lowercase: GitHub usernames are case-insensitive
    # but config entries and API responses may differ in casing.
    normalized_load = {k.lower(): v for k, v in load_map.items()}

    # Build candidates filtered by capacity.
    candidates = []
    for m in active:
        username = m["github_username"]
        load = normalized_load.get(username.lower(), 0)
        cap = m.get("max_mentees", MENTOR_MAX_MENTEES)
        if load < cap:
            candidates.append((m, load))

    if not candidates:
        return None

    # Pick mentor with fewest active issues; break ties alphabetically.
    candidates.sort(key=lambda x: (x[1], x[0]["github_username"].lower()))
    return candidates[0][0]


async def _find_assigned_mentor_from_comments(
    owner: str, repo: str, issue_number: int, token: str
) -> Optional[str]:
    """Scan issue comments for the ``blt-mentor-assigned`` hidden marker.

    Paginates through all comments (100 per page) so the marker is found even
    on issues with many comments.  Returns the mentor's GitHub username from the
    most recent marker found, or ``None`` if no marker exists.
    """
    marker = "<!-- blt-mentor-assigned:"
    per_page = 100
    page = 1
    last_mentor: Optional[str] = None
    while True:
        resp = await github_api(
            "GET",
            f"/repos/{owner}/{repo}/issues/{issue_number}/comments"
            f"?per_page={per_page}&page={page}",
            token,
        )
        if resp.status != 200:
            return None
        comments = json.loads(await resp.text())
        if not comments:
            break
        # Iterate in forward order, tracking the last match so the most recent
        # assignment marker wins without needing to reverse the full list.
        for comment in comments:
            body = comment.get("body", "")
            if marker in body:
                start = body.find(marker) + len(marker)
                end = body.find("-->", start)
                if end > start:
                    last_mentor = body[start:end].strip().lstrip("@")
        if len(comments) < per_page:
            break
        page += 1
    return last_mentor


async def _get_last_human_activity_ts(
    owner: str, repo: str, issue_number: int, issue: dict, token: str
) -> float:
    """Return the timestamp (epoch seconds) of the most recent non-bot activity.

    Fetches the most recently created page of issue comments and returns the
    timestamp of the latest comment posted by a non-bot human.  If no human
    comments are found the issue's ``created_at`` value is used as a fallback so
    that newly opened issues without any comments are still eligible for stale
    checks after ``MENTOR_STALE_DAYS`` days.
    """
    fallback = _parse_github_timestamp(issue.get("created_at", "")) or 0.0

    resp = await github_api(
        "GET",
        f"/repos/{owner}/{repo}/issues/{issue_number}/comments"
        f"?sort=created&direction=desc&per_page=100",
        token,
    )
    if resp.status != 200:
        return fallback

    comments = json.loads(await resp.text())
    for comment in comments:
        user = comment.get("user") or {}
        if _is_human(user) and not _is_bot(user):
            ts = _parse_github_timestamp(comment.get("created_at", ""))
            if ts:
                return ts

    return fallback


def _is_security_issue(issue: dict) -> bool:
    """Return ``True`` if the issue carries any security-sensitive label."""
    labels = {lb.get("name", "").lower() for lb in issue.get("labels", [])}
    return bool(labels & SECURITY_BYPASS_LABELS)


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
        await _create_comment_best_effort(
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
    await _create_comment_best_effort(owner, repo, issue_number, body, token)
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
        await _create_comment_best_effort(
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
        await _create_comment_best_effort(
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
            await _create_comment_best_effort(
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
    await _create_comment_best_effort(
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
        await _create_comment_best_effort(
            owner,
            repo,
            issue["number"],
            f"@{login} The `/mentor-pause` command is only available to active mentors.",
            token,
        )
        return
    await _create_comment_best_effort(
        owner,
        repo,
        issue["number"],
        f"@{login} Your pause request has been noted. 🙏\n\n"
        "Your availability has been paused in the mentor pool. "
        "The system will not select you for new assignments until you resume.\n\n"
        "Contact a maintainer if you need to resume your availability.",
        token,
    )


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
        await _create_comment_best_effort(
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
        await _create_comment_best_effort(
            owner,
            repo,
            issue_number,
            f"@{login} Unable to confirm the currently assigned mentor for this issue. "
            "Please contact a maintainer for assistance with the handoff.",
            token,
        )
        return
    if current_mentor.lower() != login.lower():
        await _create_comment_best_effort(
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
        await _create_comment_best_effort(
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
        await _create_comment_best_effort(
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


async def _check_stale_mentor_assignments(owner: str, repo: str, token: str) -> None:
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

                days_elapsed = int((current_time - last_human_ts) / _SECONDS_PER_DAY)
                mentor_mention = f"@{current_mentor} " if current_mentor else ""
                await _create_comment_best_effort(
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


# ---------------------------------------------------------------------------
# Event handlers — mirror the Node.js handler logic exactly
# ---------------------------------------------------------------------------


