"""models/leaderboard.py — Leaderboard D1 tracking and stats calculation.

Handles:
- Leaderboard schema creation
- Incremental and full stats tracking
- D1 backfills for historical PRs
- Stats calculation (aggregating counts from D1 or live API)
"""

import json
import time
from typing import Optional

from js import console

from core.db import _d1_all, _d1_binding, _d1_first, _d1_run, _month_key, _month_window
from core.github_client import _is_bot, github_api


# ---------------------------------------------------------------------------
# Timestamp helper
# ---------------------------------------------------------------------------


def _parse_github_timestamp(ts_str: str) -> int:
    """Parse GitHub ISO 8601 timestamp to Unix timestamp."""
    import calendar
    import re
    match = re.match(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z", str(ts_str))
    if match:
        year, month, day, hour, minute, second = map(int, match.groups())
        dt = time.struct_time((year, month, day, hour, minute, second, 0, 0, 0))
        return int(calendar.timegm(dt))
    return 0


# ---------------------------------------------------------------------------
# Schema setup
# ---------------------------------------------------------------------------


async def _ensure_leaderboard_schema(db) -> None:
    """Create leaderboard tables if they do not exist."""
    await _d1_run(db, """
        CREATE TABLE IF NOT EXISTS leaderboard_monthly_stats (
            org TEXT NOT NULL,
            month_key TEXT NOT NULL,
            user_login TEXT NOT NULL,
            merged_prs INTEGER NOT NULL DEFAULT 0,
            closed_prs INTEGER NOT NULL DEFAULT 0,
            reviews INTEGER NOT NULL DEFAULT 0,
            comments INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (org, month_key, user_login)
        )
    """)
    await _d1_run(db, """
        CREATE TABLE IF NOT EXISTS leaderboard_open_prs (
            org TEXT NOT NULL,
            user_login TEXT NOT NULL,
            open_prs INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (org, user_login)
        )
    """)
    await _d1_run(db, """
        CREATE TABLE IF NOT EXISTS leaderboard_pr_state (
            org TEXT NOT NULL,
            repo TEXT NOT NULL,
            pr_number INTEGER NOT NULL,
            author_login TEXT NOT NULL,
            state TEXT NOT NULL,
            merged INTEGER NOT NULL DEFAULT 0,
            closed_at INTEGER,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (org, repo, pr_number)
        )
    """)
    await _d1_run(db, """
        CREATE TABLE IF NOT EXISTS leaderboard_review_credits (
            org TEXT NOT NULL,
            repo TEXT NOT NULL,
            pr_number INTEGER NOT NULL,
            month_key TEXT NOT NULL,
            reviewer_login TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            PRIMARY KEY (org, repo, pr_number, month_key, reviewer_login)
        )
    """)
    await _d1_run(db, """
        CREATE TABLE IF NOT EXISTS leaderboard_backfill_state (
            org TEXT NOT NULL,
            month_key TEXT NOT NULL,
            next_page INTEGER NOT NULL DEFAULT 1,
            completed INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (org, month_key)
        )
    """)
    await _d1_run(db, """
        CREATE TABLE IF NOT EXISTS leaderboard_backfill_repo_done (
            org TEXT NOT NULL,
            month_key TEXT NOT NULL,
            repo TEXT NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (org, month_key, repo)
        )
    """)
    await _d1_run(db, """
        CREATE TABLE IF NOT EXISTS mentor_assignments (
            org TEXT NOT NULL,
            mentor_login TEXT NOT NULL,
            issue_repo TEXT NOT NULL,
            issue_number INTEGER NOT NULL,
            assigned_at INTEGER NOT NULL,
            mentee_login TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (org, issue_repo, issue_number)
        )
    """)
    try:
        await _d1_run(db, "ALTER TABLE mentor_assignments ADD COLUMN mentee_login TEXT NOT NULL DEFAULT ''")
    except Exception:
        pass
    await _d1_run(db, """
        CREATE TABLE IF NOT EXISTS mentors (
            github_username TEXT NOT NULL PRIMARY KEY,
            name TEXT NOT NULL,
            specialties TEXT NOT NULL DEFAULT '[]',
            max_mentees INTEGER NOT NULL DEFAULT 3,
            active INTEGER NOT NULL DEFAULT 1,
            timezone TEXT NOT NULL DEFAULT '',
            referred_by TEXT NOT NULL DEFAULT ''
        )
    """)
    await _d1_run(db, """
        CREATE TABLE IF NOT EXISTS mentor_stats_cache (
            org TEXT NOT NULL,
            github_username TEXT NOT NULL,
            merged_prs INTEGER NOT NULL DEFAULT 0,
            reviews INTEGER NOT NULL DEFAULT 0,
            fetched_at INTEGER NOT NULL,
            PRIMARY KEY (org, github_username)
        )
    """)


# ---------------------------------------------------------------------------
# Incremental tracking
# ---------------------------------------------------------------------------


async def _d1_get_user_comment_totals(db, org: str, logins: list) -> dict:
    if not logins:
        return {}
    try:
        placeholders = ",".join("?" for _ in logins)
        rows = await _d1_all(
            db,
            f"""
            SELECT user_login, COALESCE(SUM(comments), 0) AS total_comments
            FROM leaderboard_monthly_stats
            WHERE org = ? AND user_login IN ({placeholders})
            GROUP BY user_login
            """,
            (org, *logins),
        )
        return {
            row["user_login"]: int(row.get("total_comments") or 0)
            for row in rows
            if row.get("user_login")
        }
    except Exception as exc:
        console.error(f"[D1] Failed to get user comment totals: {exc}")
        return {}


async def _d1_inc_open_pr(db, org: str, user_login: str, delta: int) -> None:
    now = int(time.time())
    try:
        await _d1_run(
            db,
            """
            INSERT INTO leaderboard_open_prs (org, user_login, open_prs, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(org, user_login) DO UPDATE SET
                open_prs = CASE
                    WHEN leaderboard_open_prs.open_prs + excluded.open_prs < 0 THEN 0
                    ELSE leaderboard_open_prs.open_prs + excluded.open_prs
                END,
                updated_at = excluded.updated_at
            """,
            (org, user_login, delta, now),
        )
        console.log(f"[D1] Inserted/updated open PR count org={org} user={user_login} count={delta}")
    except Exception as e:
        console.error(f"[D1] Failed to update open PRs org={org} user={user_login}: {e}")


async def _d1_inc_monthly(db, org: str, month_key: str, user_login: str, field: str, delta: int = 1) -> None:
    now = int(time.time())
    if field not in {"merged_prs", "closed_prs", "reviews", "comments"}:
        return
    try:
        await _d1_run(
            db,
            f"""
            INSERT INTO leaderboard_monthly_stats (org, month_key, user_login, {field}, updated_at)
            VALUES (?, ?, ?, CASE WHEN ? < 0 THEN 0 ELSE ? END, ?)
            ON CONFLICT(org, month_key, user_login) DO UPDATE SET
                {field} = CASE
                    WHEN leaderboard_monthly_stats.{field} + ? < 0 THEN 0
                    ELSE leaderboard_monthly_stats.{field} + ?
                END,
                updated_at = excluded.updated_at
            """,
            (org, month_key, user_login, delta, delta, now, delta, delta),
        )
        console.log(f"[D1] Updated {field} org={org} month={month_key} user={user_login} +{delta}")
    except Exception as e:
        console.error(f"[D1] Failed to update {field} org={org} month={month_key} user={user_login}: {e}")


async def _track_pr_opened_in_d1(payload: dict, env) -> None:
    db = _d1_binding(env)
    if not db:
        return
    pr = payload.get("pull_request") or {}
    author = pr.get("user") or {}
    if _is_bot(author):
        return
    org = (payload.get("repository") or {}).get("owner", {}).get("login", "")
    repo = (payload.get("repository") or {}).get("name", "")
    pr_number = pr.get("number")
    author_login = author.get("login", "")
    if not (org and repo and pr_number and author_login):
        return

    await _ensure_leaderboard_schema(db)
    existing = await _d1_first(
        db,
        "SELECT state FROM leaderboard_pr_state WHERE org = ? AND repo = ? AND pr_number = ?",
        (org, repo, pr_number),
    )
    if not existing or existing.get("state") != "open":
        await _d1_inc_open_pr(db, org, author_login, 1)

    now = int(time.time())
    await _d1_run(
        db,
        """
        INSERT INTO leaderboard_pr_state (org, repo, pr_number, author_login, state, merged, closed_at, updated_at)
        VALUES (?, ?, ?, ?, 'open', 0, NULL, ?)
        ON CONFLICT(org, repo, pr_number) DO UPDATE SET
            author_login = excluded.author_login,
            state = 'open',
            merged = 0,
            closed_at = NULL,
            updated_at = excluded.updated_at
        """,
        (org, repo, pr_number, author_login, now),
    )


async def _track_pr_closed_in_d1(payload: dict, env) -> None:
    db = _d1_binding(env)
    if not db:
        return
    pr = payload.get("pull_request") or {}
    author = pr.get("user") or {}
    if _is_bot(author):
        return
    org = (payload.get("repository") or {}).get("owner", {}).get("login", "")
    repo = (payload.get("repository") or {}).get("name", "")
    pr_number = pr.get("number")
    author_login = author.get("login", "")
    closed_at = pr.get("closed_at")
    merged_at = pr.get("merged_at")
    merged = bool(pr.get("merged"))
    closed_ts = _parse_github_timestamp(closed_at) if closed_at else int(time.time())
    if not (org and repo and pr_number and author_login):
        return

    await _ensure_leaderboard_schema(db)
    existing = await _d1_first(
        db,
        "SELECT state, merged, closed_at FROM leaderboard_pr_state WHERE org = ? AND repo = ? AND pr_number = ?",
        (org, repo, pr_number),
    )

    if existing and existing.get("state") == "closed" and int(existing.get("merged") or 0) == int(merged):
        existing_closed_at = int(existing.get("closed_at") or 0)
        if existing_closed_at == int(closed_ts or 0):
            return

    if existing and existing.get("state") == "open":
        await _d1_inc_open_pr(db, org, author_login, -1)

    event_ts = _parse_github_timestamp(merged_at) if merged and merged_at else closed_ts
    mk = _month_key(event_ts)
    if merged:
        await _d1_inc_monthly(db, org, mk, author_login, "merged_prs", 1)
    else:
        await _d1_inc_monthly(db, org, mk, author_login, "closed_prs", 1)

    now = int(time.time())
    await _d1_run(
        db,
        """
        INSERT INTO leaderboard_pr_state (org, repo, pr_number, author_login, state, merged, closed_at, updated_at)
        VALUES (?, ?, ?, ?, 'closed', ?, ?, ?)
        ON CONFLICT(org, repo, pr_number) DO UPDATE SET
            author_login = excluded.author_login,
            state = 'closed',
            merged = excluded.merged,
            closed_at = excluded.closed_at,
            updated_at = excluded.updated_at
        """,
        (org, repo, pr_number, author_login, 1 if merged else 0, closed_ts, now),
    )


async def _track_pr_reopened_in_d1(payload: dict, env) -> None:
    db = _d1_binding(env)
    if not db:
        return

    pr = payload.get("pull_request") or {}
    author = pr.get("user") or {}
    if _is_bot(author):
        return

    org = (payload.get("repository") or {}).get("owner", {}).get("login", "")
    repo = (payload.get("repository") or {}).get("name", "")
    pr_number = pr.get("number")
    author_login = author.get("login", "")
    if not (org and repo and pr_number and author_login):
        return

    await _ensure_leaderboard_schema(db)
    existing = await _d1_first(
        db,
        "SELECT state, merged, closed_at FROM leaderboard_pr_state WHERE org = ? AND repo = ? AND pr_number = ?",
        (org, repo, pr_number),
    )

    if existing and existing.get("state") == "closed":
        prev_merged = int(existing.get("merged") or 0)
        prev_closed_at = int(existing.get("closed_at") or 0)
        prev_mk = _month_key(prev_closed_at) if prev_closed_at else _month_key()
        field = "merged_prs" if prev_merged else "closed_prs"
        await _d1_inc_monthly(db, org, prev_mk, author_login, field, -1)

    if not existing or existing.get("state") != "open":
        await _d1_inc_open_pr(db, org, author_login, 1)

    now = int(time.time())
    await _d1_run(
        db,
        """
        INSERT INTO leaderboard_pr_state (org, repo, pr_number, author_login, state, merged, closed_at, updated_at)
        VALUES (?, ?, ?, ?, 'open', 0, NULL, ?)
        ON CONFLICT(org, repo, pr_number) DO UPDATE SET
            author_login = excluded.author_login,
            state = 'open',
            merged = 0,
            closed_at = NULL,
            updated_at = excluded.updated_at
        """,
        (org, repo, pr_number, author_login, now),
    )


async def _track_comment_in_d1(payload: dict, env) -> None:
    from core.github_client import _is_coderabbit_ping, _extract_command

    db = _d1_binding(env)
    if not db:
        return
    comment = payload.get("comment") or {}
    user = comment.get("user") or {}
    if _is_bot(user):
        return
    body = comment.get("body", "")
    if _is_coderabbit_ping(body):
        return
    if _extract_command(body):
        return
    org = (payload.get("repository") or {}).get("owner", {}).get("login", "")
    login = user.get("login", "")
    created_at = comment.get("created_at")
    if not (org and login):
        return

    await _ensure_leaderboard_schema(db)
    mk = _month_key(_parse_github_timestamp(created_at) if created_at else int(time.time()))
    await _d1_inc_monthly(db, org, mk, login, "comments", 1)


async def _track_review_in_d1(payload: dict, env) -> None:
    db = _d1_binding(env)
    if not db:
        return
    review = payload.get("review") or {}
    reviewer = review.get("user") or {}
    if _is_bot(reviewer):
        return
    pr = payload.get("pull_request") or {}
    org = (payload.get("repository") or {}).get("owner", {}).get("login", "")
    repo = (payload.get("repository") or {}).get("name", "")
    pr_number = pr.get("number")
    reviewer_login = reviewer.get("login", "")
    submitted_at = review.get("submitted_at")
    if not (org and repo and pr_number and reviewer_login):
        return

    await _ensure_leaderboard_schema(db)
    mk = _month_key(_parse_github_timestamp(submitted_at) if submitted_at else int(time.time()))

    exists = await _d1_first(
        db,
        """
        SELECT 1 FROM leaderboard_review_credits
        WHERE org = ? AND repo = ? AND pr_number = ? AND month_key = ? AND reviewer_login = ?
        """,
        (org, repo, pr_number, mk, reviewer_login),
    )
    if exists:
        return

    cnt_row = await _d1_first(
        db,
        """
        SELECT COUNT(*) AS cnt FROM leaderboard_review_credits
        WHERE org = ? AND repo = ? AND pr_number = ? AND month_key = ?
        """,
        (org, repo, pr_number, mk),
    )
    already = int((cnt_row or {}).get("cnt") or 0)
    if already >= 2:
        return

    await _d1_run(
        db,
        """
        INSERT INTO leaderboard_review_credits (org, repo, pr_number, month_key, reviewer_login, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (org, repo, pr_number, mk, reviewer_login, int(time.time())),
    )
    await _d1_inc_monthly(db, org, mk, reviewer_login, "reviews", 1)


# ---------------------------------------------------------------------------
# Stats computation and fetching
# ---------------------------------------------------------------------------


async def _calculate_leaderboard_stats_from_d1(owner: str, env) -> Optional[dict]:
    db = _d1_binding(env)
    if not db:
        return None

    await _ensure_leaderboard_schema(db)
    mk = _month_key()
    start_timestamp, end_timestamp = _month_window(mk)

    monthly_rows = await _d1_all(
        db,
        "SELECT user_login, merged_prs, closed_prs, reviews, comments FROM leaderboard_monthly_stats WHERE org = ? AND month_key = ?",
        (owner, mk),
    )
    open_rows = await _d1_all(
        db,
        "SELECT user_login, open_prs FROM leaderboard_open_prs WHERE org = ?",
        (owner,),
    )

    user_stats = {}

    def ensure(login: str):
        if login not in user_stats:
            user_stats[login] = {
                "openPrs": 0,
                "mergedPrs": 0,
                "closedPrs": 0,
                "reviews": 0,
                "comments": 0,
                "total": 0,
            }

    for row in monthly_rows:
        login = row.get("user_login")
        if not login:
            continue
        ensure(login)
        user_stats[login]["mergedPrs"] = int(row.get("merged_prs") or 0)
        user_stats[login]["closedPrs"] = int(row.get("closed_prs") or 0)
        user_stats[login]["reviews"] = int(row.get("reviews") or 0)
        user_stats[login]["comments"] = int(row.get("comments") or 0)

    for row in open_rows:
        login = row.get("user_login")
        if not login:
            continue
        ensure(login)
        user_stats[login]["openPrs"] = int(row.get("open_prs") or 0)

    for login in user_stats:
        s = user_stats[login]
        s["total"] = (s["openPrs"] * 1) + (s["mergedPrs"] * 10) + (s["closedPrs"] * -2) + (s["reviews"] * 5) + (s["comments"] * 2)

    sorted_users = sorted(
        [{"login": login, **stats} for login, stats in user_stats.items()],
        key=lambda u: (-u["total"], -u["mergedPrs"], -u["reviews"], u["login"].lower())
    )

    return {
        "users": user_stats,
        "sorted": sorted_users,
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp,
    }


async def _calculate_leaderboard_stats(owner: str, repos: list, token: str, window_months: int = 1) -> dict:
    """Calculate leaderboard stats across ALL repositories using GitHub Search API."""
    now_seconds = int(time.time())
    import time as _t
    now = _t.gmtime(now_seconds)
    
    start_of_month = _t.struct_time((now.tm_year, now.tm_mon, 1, 0, 0, 0, 0, 0, 0))
    import calendar
    start_timestamp = int(calendar.timegm(start_of_month))
    
    if now.tm_mon == 12:
        end_month = 1
        end_year = now.tm_year + 1
    else:
        end_month = now.tm_mon + 1
        end_year = now.tm_year
    end_of_month = _t.struct_time((end_year, end_month, 1, 0, 0, 0, 0, 0, 0))
    end_timestamp = int(calendar.timegm(end_of_month)) - 1
    
    start_date = _t.strftime("%Y-%m-%d", _t.gmtime(start_timestamp))
    end_date = _t.strftime("%Y-%m-%d", _t.gmtime(end_timestamp))
    
    user_stats = {}
    def ensure_user(login: str):
        if login not in user_stats:
            user_stats[login] = {"openPrs": 0, "mergedPrs": 0, "closedPrs": 0, "reviews": 0, "comments": 0, "total": 0}
            
    page = 1
    while page <= 3:
        resp = await github_api("GET", f"/search/issues?q=is:pr+is:open+org:{owner}&per_page=100&page={page}", token)
        if resp.status != 200: break
        data = json.loads(await resp.text())
        items = data.get("items", [])
        if not items: break
        for pr in items:
            if pr.get("user") and not _is_bot(pr["user"]):
                login = pr["user"]["login"]
                ensure_user(login)
                user_stats[login]["openPrs"] += 1
        if len(items) < 100: break
        page += 1
    
    page = 1
    while page <= 3:
        resp = await github_api("GET", f"/search/issues?q=is:pr+is:merged+org:{owner}+merged:{start_date}..{end_date}&per_page=100&page={page}", token)
        if resp.status != 200: break
        data = json.loads(await resp.text())
        items = data.get("items", [])
        if not items: break
        for pr in items:
            if pr.get("user") and not _is_bot(pr["user"]):
                login = pr["user"]["login"]
                ensure_user(login)
                user_stats[login]["mergedPrs"] += 1
        if len(items) < 100: break
        page += 1
    
    page = 1
    while page <= 3:
        resp = await github_api("GET", f"/search/issues?q=is:pr+is:closed+is:unmerged+org:{owner}+closed:{start_date}..{end_date}&per_page=100&page={page}", token)
        if resp.status != 200: break
        data = json.loads(await resp.text())
        items = data.get("items", [])
        if not items: break
        for pr in items:
            if pr.get("user") and not _is_bot(pr["user"]):
                login = pr["user"]["login"]
                ensure_user(login)
                user_stats[login]["closedPrs"] += 1
        if len(items) < 100: break
        page += 1
    
    max_review_calls = 15
    review_calls_used = 0
    page = 1
    sampled_prs = []
    while page <= 2 and len(sampled_prs) < max_review_calls:
        resp = await github_api("GET", f"/search/issues?q=is:pr+is:merged+org:{owner}+merged:{start_date}..{end_date}&per_page=50&page={page}&sort=updated", token)
        if resp.status != 200: break
        data = json.loads(await resp.text())
        items = data.get("items", [])
        if not items: break
        for pr in items:
            if len(sampled_prs) >= max_review_calls: break
            repo_url = pr.get("repository_url", "")
            if repo_url:
                parts = repo_url.split("/")
                if len(parts) >= 2:
                    repo_name = parts[-1]
                    pr_number = pr.get("number")
                    if repo_name and pr_number:
                        sampled_prs.append((repo_name, pr_number))
        if len(items) < 50: break
        page += 1
    
    for repo_name, pr_number in sampled_prs:
        if review_calls_used >= max_review_calls: break
        resp_reviews = await github_api("GET", f"/repos/{owner}/{repo_name}/pulls/{pr_number}/reviews", token)
        review_calls_used += 1
        if resp_reviews.status == 200:
            reviews = json.loads(await resp_reviews.text())
            pr_review_count = {}
            for review in reviews:
                if review.get("user") and not _is_bot(review["user"]):
                    submitted_at = review.get("submitted_at")
                    if submitted_at:
                        review_ts = _parse_github_timestamp(submitted_at)
                        if start_timestamp <= review_ts <= end_timestamp:
                            login = review["user"]["login"]
                            pr_review_count[login] = pr_review_count.get(login, 0) + 1
            for login in list(pr_review_count.keys())[:2]:
                ensure_user(login)
                user_stats[login]["reviews"] += 1
    
    for login in user_stats:
        s = user_stats[login]
        s["total"] = (s["openPrs"] * 1) + (s["mergedPrs"] * 10) + (s["closedPrs"] * -2) + (s["reviews"] * 5) + (s["comments"] * 2)
    
    sorted_users = sorted(
        [{"login": login, **stats} for login, stats in user_stats.items()],
        key=lambda u: (-u["total"], -u["mergedPrs"], -u["reviews"], u["login"].lower())
    )
    
    return {
        "users": user_stats,
        "sorted": sorted_users,
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp
    }


async def _fetch_leaderboard_data(owner: str, repo: str, token: str, env=None) -> tuple:
    """Fetch leaderboard data for *owner*, running D1 backfill when available."""
    leaderboard_note = ""
    owner_data = None
    is_org = False

    owner_resp = await github_api("GET", f"/users/{owner}", token)
    if owner_resp.status == 200:
        owner_data = json.loads(await owner_resp.text())
        is_org = owner_data.get("type") == "Organization"

    leaderboard_data = await _calculate_leaderboard_stats_from_d1(owner, env)

    if leaderboard_data is not None and is_org:
        seeded_current = await _backfill_repo_month_if_needed(owner, repo, token, env)
        if seeded_current:
            leaderboard_data = await _calculate_leaderboard_stats_from_d1(owner, env) or leaderboard_data

    if leaderboard_data is not None and is_org:
        db = _d1_binding(env)
        if db:
            month_key = _month_key()
            state = await _get_backfill_state(db, owner, month_key)
            if not state.get("completed"):
                backfill_result = await _run_incremental_backfill(owner, token, env)
                if backfill_result:
                    leaderboard_data = await _calculate_leaderboard_stats_from_d1(owner, env) or leaderboard_data
                    if backfill_result.get("completed"):
                        leaderboard_note = f"Backfill completed in this request; seeded {backfill_result.get('processed', 0)} repos in the final chunk."
                    elif backfill_result.get("ran"):
                        leaderboard_note = f"Backfill in progress: seeded {backfill_result.get('processed', 0)} repos in this run; next page {backfill_result.get('next_page', '?')}. Run `/leaderboard` again to continue filling historical data."
                    else:
                        leaderboard_note = "Backfill did not progress this run; leaderboard still updates from new webhook events."
                else:
                    leaderboard_note = "Backfill state unavailable; leaderboard still updates from new webhook events."

    if leaderboard_data is None:
        if owner_data is None:
            resp = await github_api("GET", f"/users/{owner}", token)
            if resp.status == 200:
                owner_data = json.loads(await resp.text())
                is_org = owner_data.get("type") == "Organization"
        if is_org:
            repos_resp = await github_api("GET", f"/orgs/{owner}/repos?sort=pushed&direction=desc&per_page=10", token)
            if repos_resp.status == 200:
                repos = json.loads(await repos_resp.text())
            else:
                repos = []
        else:
            repos = [{"name": repo}]
        leaderboard_data = await _calculate_leaderboard_stats(owner, repos, token)

    return leaderboard_data, leaderboard_note, is_org


# ---------------------------------------------------------------------------
# Backfill operations
# ---------------------------------------------------------------------------


async def _get_backfill_state(db, owner: str, month_key: str) -> dict:
    row = await _d1_first(
        db,
        "SELECT next_page, completed FROM leaderboard_backfill_state WHERE org = ? AND month_key = ?",
        (owner, month_key),
    )
    if row:
        return {"next_page": int(row.get("next_page") or 1), "completed": bool(int(row.get("completed") or 0))}
    return {"next_page": 1, "completed": False}


async def _set_backfill_state(db, owner: str, month_key: str, next_page: int, completed: bool) -> None:
    try:
        await _d1_run(
            db,
            """
            INSERT INTO leaderboard_backfill_state (org, month_key, next_page, completed, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(org, month_key) DO UPDATE SET
                next_page = excluded.next_page,
                completed = excluded.completed,
                updated_at = excluded.updated_at
            """,
            (owner, month_key, next_page, 1 if completed else 0, int(time.time())),
        )
    except Exception as e:
        console.error(f"[Backfill] Failed to update state: {e}")


async def _run_incremental_backfill(owner: str, token: str, env, repos_per_request: int = 5) -> Optional[dict]:
    db = _d1_binding(env)
    if not db:
        return None

    await _ensure_leaderboard_schema(db)
    month_key = _month_key()
    start_ts, end_ts = _month_window(month_key)
    start_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(start_ts))

    state = await _get_backfill_state(db, owner, month_key)
    if state["completed"]:
        return {"ran": False, "completed": True, "processed": 0, "next_page": state["next_page"]}

    page = state["next_page"]
    repos_resp = await github_api("GET", f"/orgs/{owner}/repos?sort=full_name&direction=asc&per_page={repos_per_request}&page={page}", token)
    if repos_resp.status != 200:
        return {"ran": False, "completed": False, "processed": 0, "next_page": page}

    repos = json.loads(await repos_resp.text())
    if not repos:
        await _set_backfill_state(db, owner, month_key, page, True)
        return {"ran": False, "completed": True, "processed": 0, "next_page": page}

    processed = 0
    for repo_obj in repos:
        repo_name = repo_obj.get("name")
        if not repo_name:
            continue
        seeded = await _backfill_repo_month_if_needed(owner, repo_name, token, env, month_key, start_ts, end_ts)
        if seeded:
            processed += 1

    done = len(repos) < repos_per_request
    await _set_backfill_state(db, owner, month_key, page + 1, done)
    return {"ran": True, "completed": done, "processed": processed, "next_page": page + 1, "month_key": month_key, "since": start_iso}


async def _backfill_repo_month_if_needed(
    owner: str, repo_name: str, token: str, env,
    month_key: Optional[str] = None, start_ts: Optional[int] = None, end_ts: Optional[int] = None,
) -> bool:
    db = _d1_binding(env)
    if not db:
        return False

    await _ensure_leaderboard_schema(db)
    mk = month_key or _month_key()
    if start_ts is None or end_ts is None:
        start_ts, end_ts = _month_window(mk)

    already = await _d1_first(db, "SELECT 1 FROM leaderboard_backfill_repo_done WHERE org = ? AND month_key = ? AND repo = ?", (owner, mk, repo_name))
    if already:
        return False

    tracked_rows = await _d1_all(db, "SELECT pr_number, state FROM leaderboard_pr_state WHERE org = ? AND repo = ?", (owner, repo_name))
    already_tracked_state = {int(row["pr_number"]): row.get("state", "") for row in (tracked_rows or [])}
    already_tracked = set(already_tracked_state.keys())

    now_ts = int(time.time())

    open_resp = await github_api("GET", f"/repos/{owner}/{repo_name}/pulls?state=open&per_page=100", token)
    if open_resp.status == 200:
        open_prs = json.loads(await open_resp.text())
        open_by_user = {}
        for pr in open_prs:
            user = pr.get("user") or {}
            if _is_bot(user):
                continue
            login = user.get("login")
            pr_number = pr.get("number")
            if not login or not pr_number:
                continue
            if pr_number in already_tracked:
                continue
            open_by_user[login] = open_by_user.get(login, 0) + 1
            await _d1_run(db, """
                INSERT INTO leaderboard_pr_state (org, repo, pr_number, author_login, state, merged, closed_at, updated_at)
                VALUES (?, ?, ?, ?, 'open', 0, NULL, ?)
                ON CONFLICT(org, repo, pr_number) DO NOTHING
            """, (owner, repo_name, pr_number, login, now_ts))
            already_tracked.add(pr_number)
            already_tracked_state[pr_number] = "open"
        for login, cnt in open_by_user.items():
            await _d1_inc_open_pr(db, owner, login, cnt)

    merged_count = 0
    closed_count = 0
    closed_page = 1
    merged_prs_for_review = []
    MAX_REVIEW_BACKFILL = 20
    
    while closed_page <= 3:
        closed_resp = await github_api("GET", f"/repos/{owner}/{repo_name}/pulls?state=closed&per_page=100&sort=updated&direction=desc&page={closed_page}", token)
        if closed_resp.status != 200:
            break
        closed_prs = json.loads(await closed_resp.text())
        if not closed_prs:
            break
        for pr in closed_prs:
            user = pr.get("user") or {}
            if _is_bot(user):
                continue
            login = user.get("login")
            pr_number = pr.get("number")
            if not login or not pr_number:
                continue
            tracked_state = already_tracked_state.get(pr_number)
            if tracked_state == "closed":
                continue
            if tracked_state == "open":
                await _d1_inc_open_pr(db, owner, login, -1)
            merged_at = pr.get("merged_at")
            closed_at = pr.get("closed_at")
            if merged_at:
                merged_ts = _parse_github_timestamp(merged_at)
                if start_ts <= merged_ts <= end_ts:
                    merged_count += 1
                    await _d1_inc_monthly(db, owner, mk, login, "merged_prs", 1)
                    pr_closed_ts = _parse_github_timestamp(closed_at) if closed_at else merged_ts
                    await _d1_run(db, """
                        INSERT INTO leaderboard_pr_state (org, repo, pr_number, author_login, state, merged, closed_at, updated_at)
                        VALUES (?, ?, ?, ?, 'closed', 1, ?, ?)
                        ON CONFLICT(org, repo, pr_number) DO UPDATE SET
                            state = 'closed', merged = 1, closed_at = excluded.closed_at, updated_at = excluded.updated_at
                    """, (owner, repo_name, pr_number, login, pr_closed_ts, now_ts))
                    already_tracked.add(pr_number)
                    already_tracked_state[pr_number] = "closed"
                    if len(merged_prs_for_review) < MAX_REVIEW_BACKFILL:
                        merged_prs_for_review.append((pr_number, login))
            elif closed_at:
                closed_ts_val = _parse_github_timestamp(closed_at)
                if start_ts <= closed_ts_val <= end_ts:
                    closed_count += 1
                    await _d1_inc_monthly(db, owner, mk, login, "closed_prs", 1)
                    await _d1_run(db, """
                        INSERT INTO leaderboard_pr_state (org, repo, pr_number, author_login, state, merged, closed_at, updated_at)
                        VALUES (?, ?, ?, ?, 'closed', 0, ?, ?)
                        ON CONFLICT(org, repo, pr_number) DO UPDATE SET
                            state = 'closed', merged = 0, closed_at = excluded.closed_at, updated_at = excluded.updated_at
                    """, (owner, repo_name, pr_number, login, closed_ts_val, now_ts))
                    already_tracked.add(pr_number)
                    already_tracked_state[pr_number] = "closed"
        if len(closed_prs) < 100:
            break
        closed_page += 1

    if len(merged_prs_for_review) < MAX_REVIEW_BACKFILL:
        tracked_merged_rows = await _d1_all(db, "SELECT pr_number, author_login FROM leaderboard_pr_state WHERE org = ? AND repo = ? AND merged = 1", (owner, repo_name))
        newly_added = {pr_num for pr_num, _ in merged_prs_for_review}
        for row in (tracked_merged_rows or []):
            if len(merged_prs_for_review) >= MAX_REVIEW_BACKFILL:
                break
            pr_num = row.get("pr_number")
            author = row.get("author_login", "")
            if pr_num and pr_num not in newly_added:
                merged_prs_for_review.append((pr_num, author))
                newly_added.add(pr_num)

    for pr_number, pr_author in merged_prs_for_review:
        try:
            reviews_resp = await github_api("GET", f"/repos/{owner}/{repo_name}/pulls/{pr_number}/reviews?per_page=100", token)
            if reviews_resp.status == 429:
                console.error(f"[LeaderboardBackfill] Exiting early due to rate limit/429 for {owner}/{repo_name}.")
                break
            if reviews_resp.status != 200:
                continue
            reviews = json.loads(await reviews_resp.text())
            credit_rows = await _d1_all(db, "SELECT reviewer_login FROM leaderboard_review_credits WHERE org = ? AND repo = ? AND pr_number = ? AND month_key = ?", (owner, repo_name, pr_number, mk))
            already_credited_set = {row["reviewer_login"] for row in (credit_rows or [])}
            seen_reviewers: set = set()
            for review in reviews:
                reviewer = review.get("user") or {}
                if _is_bot(reviewer):
                    continue
                reviewer_login = reviewer.get("login", "")
                if not reviewer_login or reviewer_login == pr_author:
                    continue
                if reviewer_login in seen_reviewers:
                    continue
                seen_reviewers.add(reviewer_login)
                if reviewer_login in already_credited_set:
                    continue
                if len(already_credited_set) >= 2:
                    break
                await _d1_run(db, "INSERT INTO leaderboard_review_credits (org, repo, pr_number, month_key, reviewer_login, created_at) VALUES (?, ?, ?, ?, ?, ?)", (owner, repo_name, pr_number, mk, reviewer_login, now_ts))
                await _d1_inc_monthly(db, owner, mk, reviewer_login, "reviews", 1)
                already_credited_set.add(reviewer_login)
        except Exception:
            pass

    try:
        await _d1_run(db, """
            INSERT INTO leaderboard_backfill_repo_done (org, month_key, repo, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(org, month_key, repo) DO UPDATE SET updated_at = excluded.updated_at
        """, (owner, mk, repo_name, int(time.time())))
        return True
    except Exception:
        return False


async def _reset_leaderboard_month(org: str, month_key: str, db) -> dict:
    await _ensure_leaderboard_schema(db)
    deleted: dict = {}

    for table, params in [
        ("leaderboard_monthly_stats", (org, month_key)),
        ("leaderboard_backfill_repo_done", (org, month_key)),
        ("leaderboard_review_credits", (org, month_key)),
        ("leaderboard_backfill_state", (org, month_key)),
    ]:
        try:
            await _d1_run(db, f"DELETE FROM {table} WHERE org = ? AND month_key = ?", params)
            deleted[table] = "cleared"
        except Exception as e:
            deleted[table] = f"error: {e}"

    start_ts, end_ts = _month_window(month_key)
    try:
        await _d1_run(
            db,
            """
            DELETE FROM leaderboard_pr_state
            WHERE org = ? AND (
                closed_at BETWEEN ? AND ?
                OR (state = 'open' AND closed_at IS NULL AND updated_at BETWEEN ? AND ?)
            )
            """,
            (org, start_ts, end_ts, start_ts, end_ts),
        )
        deleted["leaderboard_pr_state"] = "cleared"
    except Exception as e:
        deleted["leaderboard_pr_state"] = f"error: {e}"

    try:
        await _d1_run(db, "DELETE FROM leaderboard_open_prs WHERE org = ?", (org,))
        deleted["leaderboard_open_prs"] = "cleared"
    except Exception as e:
        deleted["leaderboard_open_prs"] = f"error: {e}"

    return deleted


async def _fetch_org_repos(org: str, token: str, limit: int = 10) -> list:
    """Fetch repositories in the organization (most recently updated first).
    
    Args:
        org: Organization name
        token: GitHub API token
        limit: Maximum number of repos to return (default: 10 to prevent subrequest limits)
    """
    # Fetch repos sorted by most recently pushed to reduce API calls for active repos
    resp = await github_api("GET", f"/orgs/{org}/repos?sort=pushed&direction=desc&per_page={limit}", token)
    if resp.status != 200:
        return []
    repos = json.loads(await resp.text())
    return repos[:limit]

async def _calculate_leaderboard_stats(owner: str, repos: list, token: str, window_months: int = 1) -> dict:
    """Calculate leaderboard stats across ALL repositories using GitHub Search API.
    
    This approach uses GitHub's search API to query across all org repos efficiently,
    staying well under Cloudflare's 50 subrequest limit even with 50+ repos.
    
    Args:
        owner: Organization or user name
        repos: List of repository objects (used for repo count, not iteration)
        token: GitHub API token
        window_months: Number of months to look back (default: 1 for monthly)
    
    Returns:
        Dictionary with user stats and sorted leaderboard
    """
    now_seconds = int(time.time())
    now = time.gmtime(now_seconds)
    
    # Calculate time window
    start_of_month = time.struct_time((now.tm_year, now.tm_mon, 1, 0, 0, 0, 0, 0, 0))
    start_timestamp = int(time.mktime(start_of_month))
    
    # End of month calculation
    if now.tm_mon == 12:
        end_month = 1
        end_year = now.tm_year + 1
    else:
        end_month = now.tm_mon + 1
        end_year = now.tm_year
    end_of_month = time.struct_time((end_year, end_month, 1, 0, 0, 0, 0, 0, 0))
    end_timestamp = int(time.mktime(end_of_month)) - 1
    
    # Format date range for search API
    start_date = time.strftime("%Y-%m-%d", time.gmtime(start_timestamp))
    end_date = time.strftime("%Y-%m-%d", time.gmtime(end_timestamp))
    
    user_stats = {}
    
    def ensure_user(login: str):
        if login not in user_stats:
            user_stats[login] = {
                "openPrs": 0,
                "mergedPrs": 0,
                "closedPrs": 0,
                "reviews": 0,
                "comments": 0,
                "total": 0
            }
    
    # Use GitHub Search API to query across ALL repos efficiently
    # This dramatically reduces API calls: ~6 calls total vs 150+ with per-repo approach
    
    # 1. Count open PRs (current state across all repos) - 1-2 calls
    page = 1
    while page <= 3:  # Max 3 pages = 300 PRs
        resp = await github_api(
            "GET",
            f"/search/issues?q=is:pr+is:open+org:{owner}&per_page=100&page={page}",
            token
        )
        if resp.status != 200:
            break
        data = json.loads(await resp.text())
        items = data.get("items", [])
        if not items:
            break
        
        for pr in items:
            if pr.get("user") and not _is_bot(pr["user"]):
                login = pr["user"]["login"]
                ensure_user(login)
                user_stats[login]["openPrs"] += 1
        
        if len(items) < 100:
            break
        page += 1
    
    # 2. Fetch merged PRs from this month - 1-2 calls
    page = 1
    while page <= 3:
        resp = await github_api(
            "GET",
            f"/search/issues?q=is:pr+is:merged+org:{owner}+merged:{start_date}..{end_date}&per_page=100&page={page}",
            token
        )
        if resp.status != 200:
            break
        data = json.loads(await resp.text())
        items = data.get("items", [])
        if not items:
            break
        
        for pr in items:
            if pr.get("user") and not _is_bot(pr["user"]):
                login = pr["user"]["login"]
                ensure_user(login)
                user_stats[login]["mergedPrs"] += 1
        
        if len(items) < 100:
            break
        page += 1
    
    # 3. Fetch closed (not merged) PRs from this month - 1-2 calls
    page = 1
    while page <= 3:
        resp = await github_api(
            "GET",
            f"/search/issues?q=is:pr+is:closed+is:unmerged+org:{owner}+closed:{start_date}..{end_date}&per_page=100&page={page}",
            token
        )
        if resp.status != 200:
            break
        data = json.loads(await resp.text())
        items = data.get("items", [])
        if not items:
            break
        
        for pr in items:
            if pr.get("user") and not _is_bot(pr["user"]):
                login = pr["user"]["login"]
                ensure_user(login)
                user_stats[login]["closedPrs"] += 1
        
        if len(items) < 100:
            break
        page += 1
    
    # 4. Search for comments in this month across org (optional, budget permitting)
    # Limit to 2 pages to stay under budget
    since_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(start_timestamp))
    # Note: Skipping comment counting to conserve API budget
    # With 50 repos, we need to prioritize PRs and reviews
    
    # 5. Fetch reviews from a sample of merged PRs - budget 15 calls
    # Strategy: Get repo URLs from merged PRs, fetch reviews for top 15 PRs
    max_review_calls = 15
    review_calls_used = 0
    
    # Get merged PRs again (already cached in memory from step 2)
    page = 1
    sampled_prs = []
    while page <= 2 and len(sampled_prs) < max_review_calls:
        resp = await github_api(
            "GET",
            f"/search/issues?q=is:pr+is:merged+org:{owner}+merged:{start_date}..{end_date}&per_page=50&page={page}&sort=updated",
            token
        )
        if resp.status != 200:
            break
        data = json.loads(await resp.text())
        items = data.get("items", [])
        if not items:
            break
        
        # Extract repo and PR number from each PR
        for pr in items:
            if len(sampled_prs) >= max_review_calls:
                break
            # Parse repo from repository_url: /repos/{owner}/{repo}
            repo_url = pr.get("repository_url", "")
            if repo_url:
                parts = repo_url.split("/")
                if len(parts) >= 2:
                    repo_name = parts[-1]
                    pr_number = pr.get("number")
                    if repo_name and pr_number:
                        sampled_prs.append((repo_name, pr_number))
        
        if len(items) < 50:
            break
        page += 1
    
    # Fetch reviews for sampled PRs
    for repo_name, pr_number in sampled_prs:
        if review_calls_used >= max_review_calls:
            break
        
        resp_reviews = await github_api(
            "GET",
            f"/repos/{owner}/{repo_name}/pulls/{pr_number}/reviews",
            token
        )
        review_calls_used += 1
        
        if resp_reviews.status == 200:
            reviews = json.loads(await resp_reviews.text())
            pr_review_count = {}
            
            for review in reviews:
                if review.get("user") and not _is_bot(review["user"]):
                    submitted_at = review.get("submitted_at")
                    if submitted_at:
                        review_ts = _parse_github_timestamp(submitted_at)
                        if start_timestamp <= review_ts <= end_timestamp:
                            login = review["user"]["login"]
                            pr_review_count[login] = pr_review_count.get(login, 0) + 1
            
            # Count only first 2 reviewers per PR to avoid spam
            for login in list(pr_review_count.keys())[:2]:
                ensure_user(login)
                user_stats[login]["reviews"] += 1
    
    # Calculate total scores
    # open: +1, merged: +10, closed: -2, reviews: +5, comments: +2
    for login in user_stats:
        s = user_stats[login]
        s["total"] = (s["openPrs"] * 1) + (s["mergedPrs"] * 10) + (s["closedPrs"] * -2) + (s["reviews"] * 5) + (s["comments"] * 2)
    
    # Sort users by total score, then merged PRs, then reviews, then alphabetically
    sorted_users = sorted(
        [{"login": login, **stats} for login, stats in user_stats.items()],
        key=lambda u: (-u["total"], -u["mergedPrs"], -u["reviews"], u["login"].lower())
    )
    
    return {
        "users": user_stats,
        "sorted": sorted_users,
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp
    }

async def _fetch_leaderboard_data(owner: str, repo: str, token: str, env=None) -> tuple:
    """Fetch leaderboard data for *owner*, running D1 backfill when available.

    Returns a ``(leaderboard_data, leaderboard_note, is_org)`` tuple where
    ``leaderboard_data`` is the dict expected by ``_format_leaderboard_comment``
    and ``leaderboard_note`` is an optional informational string about backfill
    progress (may be empty).  ``is_org`` indicates whether *owner* is a GitHub
    organisation (used by callers that need to choose comment wording).
    """
    leaderboard_note = ""
    owner_data = None
    is_org = False

    owner_resp = await github_api("GET", f"/users/{owner}", token)
    if owner_resp.status == 200:
        owner_data = json.loads(await owner_resp.text())
        is_org = owner_data.get("type") == "Organization"
        console.log(f"[Leaderboard] Owner {owner} is_org={is_org}")
    else:
        console.error(f"[Leaderboard] Owner lookup failed for {owner}: status={owner_resp.status}")

    # Prefer D1-backed stats for accurate and scalable org-wide leaderboard.
    leaderboard_data = await _calculate_leaderboard_stats_from_d1(owner, env)
    console.log(f"[Leaderboard] Initial D1 data: {bool(leaderboard_data)}, has_users={bool(leaderboard_data and leaderboard_data.get('sorted')) if leaderboard_data else False}")

    # Always prioritize seeding the current repo so requester sees their repo's activity immediately.
    if leaderboard_data is not None and is_org:
        console.log(f"[Leaderboard] D1 is available, attempting to seed current repo {owner}/{repo}")
        seeded_current = await _backfill_repo_month_if_needed(owner, repo, token, env)
        console.log(f"[Leaderboard] Current repo backfill result: seeded_current={seeded_current}")
        if seeded_current:
            console.log(f"[Leaderboard] Seeded current repo {owner}/{repo} for immediate leaderboard accuracy")
            leaderboard_data = await _calculate_leaderboard_stats_from_d1(owner, env) or leaderboard_data
            console.log(f"[Leaderboard] After current repo seed, data has {len(leaderboard_data.get('sorted', []))} users")
    else:
        console.log(f"[Leaderboard] Skipped current repo backfill: leaderboard_data={bool(leaderboard_data)}, is_org={is_org}")

    # Continue backfill until completed, not just when data is empty.
    if leaderboard_data is not None and is_org:
        db = _d1_binding(env)
        if db:
            month_key = _month_key()
            state = await _get_backfill_state(db, owner, month_key)

            if not state.get("completed"):
                console.log(
                    f"[Leaderboard] Running incremental backfill for {owner} "
                    f"month={month_key} page={state.get('next_page')}"
                )
                backfill_result = await _run_incremental_backfill(owner, token, env)
                if backfill_result:
                    leaderboard_data = await _calculate_leaderboard_stats_from_d1(owner, env) or leaderboard_data
                    console.log(f"[Leaderboard] After incremental backfill, data has {len(leaderboard_data.get('sorted', []))} users")
                    if backfill_result.get("completed"):
                        leaderboard_note = (
                            f"Backfill completed in this request; seeded {backfill_result.get('processed', 0)} repos in the final chunk."
                        )
                    elif backfill_result.get("ran"):
                        leaderboard_note = (
                            f"Backfill in progress: seeded {backfill_result.get('processed', 0)} repos in this run; "
                            f"next page {backfill_result.get('next_page', '?')}. "
                            "Run `/leaderboard` again to continue filling historical data."
                        )
                    else:
                        leaderboard_note = "Backfill did not progress this run; leaderboard still updates from new webhook events."
                else:
                    leaderboard_note = "Backfill state unavailable; leaderboard still updates from new webhook events."

    # Fallback to API-based calculation when D1 is unavailable.
    if leaderboard_data is None:
        if owner_data is None:
            resp = await github_api("GET", f"/users/{owner}", token)
            if resp.status == 200:
                owner_data = json.loads(await resp.text())
                is_org = owner_data.get("type") == "Organization"
        if is_org:
            repos = await _fetch_org_repos(owner, token)
        else:
            repos = [{"name": repo}]
        leaderboard_data = await _calculate_leaderboard_stats(owner, repos, token)

    return leaderboard_data, leaderboard_note, is_org
