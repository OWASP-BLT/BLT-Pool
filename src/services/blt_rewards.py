import calendar
import json
import time
from typing import Optional, Tuple
from urllib.parse import quote


LEADERBOARD_MARKER = "<!-- leaderboard-bot -->"
REVIEWER_LEADERBOARD_MARKER = "<!-- reviewer-leaderboard-bot -->"
MERGED_PR_COMMENT_MARKER = "<!-- merged-pr-comment-bot -->"
LEADERBOARD_COMMAND = "/leaderboard"
MAX_OPEN_PRS_PER_AUTHOR = 50



def month_key(ts: Optional[int] = None) -> str:
    """Return YYYY-MM month key for UTC timestamp (or now)."""
    if ts is None:
        ts = int(time.time())
    return time.strftime("%Y-%m", time.gmtime(ts))


def month_window(mk: str) -> Tuple[int, int]:
    """Return start/end timestamps (UTC) for a YYYY-MM key."""
    year, month = mk.split("-")
    y = int(year)
    m = int(month)
    start_struct = time.struct_time((y, m, 1, 0, 0, 0, 0, 0, 0))
    start_ts = int(calendar.timegm(start_struct))
    if m == 12:
        next_struct = time.struct_time((y + 1, 1, 1, 0, 0, 0, 0, 0, 0))
    else:
        next_struct = time.struct_time((y, m + 1, 1, 0, 0, 0, 0, 0, 0))
    end_ts = int(calendar.timegm(next_struct)) - 1
    return start_ts, end_ts


def parse_github_timestamp(ts_str: str) -> int:
    """Parse GitHub ISO 8601 timestamp to Unix timestamp."""
    import re
    match = re.match(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z", ts_str)
    if match:
        year, month, day, hour, minute, second = map(int, match.groups())
        dt = time.struct_time((year, month, day, hour, minute, second, 0, 0, 0))
        return int(calendar.timegm(dt))
    return 0


def avatar_img_tag(login: str, size: int = 20) -> str:
    """Return a fixed-size GitHub avatar image tag safe for markdown tables."""
    safe_login = quote(str(login), safe="")
    return (
        f"<img src=\"https://avatars.githubusercontent.com/{safe_login}?size={size}&v=4\" "
        f"width=\"{size}\" height=\"{size}\" alt=\"{login}\" />"
    )




def d1_binding(env):
    """Return D1 binding object if configured, otherwise None."""
    return getattr(env, "LEADERBOARD_DB", None) if env else None


def _to_py(value):
    """Best-effort conversion for JS proxy values returned by Workers runtime."""
    try:
        from pyodide.ffi import to_py
        return to_py(value)
    except Exception:
        return value


async def d1_run(db, sql: str, params: tuple = ()):
    try:
        from js import console
        stmt = db.prepare(sql)
        if params:
            stmt = stmt.bind(*params)
        result = await stmt.run()
        return result
    except Exception as e:
        from js import console
        console.error(f"[D1.run] Error executing {sql[:60]}: {e}")
        raise


async def d1_all(db, sql: str, params: tuple = ()) -> list:
    stmt = db.prepare(sql)
    if params:
        stmt = stmt.bind(*params)
    raw_result = await stmt.all()
    try:
        from js import JSON as JS_JSON
        js_json = JS_JSON.stringify(raw_result)
        parsed = json.loads(str(js_json))
        rows = parsed.get("results") if isinstance(parsed, dict) else None
        if isinstance(rows, list):
            return rows
    except Exception:
        pass
    result = _to_py(raw_result)
    rows = None
    if isinstance(result, dict):
        rows = result.get("results")
    if rows is None:
        try:
            rows = result.get("results")
        except Exception:
            rows = getattr(result, "results", None)
    rows = _to_py(rows)
    if rows is None:
        return []
    if isinstance(rows, list):
        return rows
    try:
        return list(rows)
    except Exception:
        return []


async def d1_first(db, sql: str, params: tuple = ()):
    rows = await d1_all(db, sql, params)
    return rows[0] if rows else None


async def d1_has_column(db, table_name: str, column_name: str) -> bool:
    """Return True when the table already contains the given column."""
    try:
        rows = await d1_all(db, f"PRAGMA table_info({table_name})")
    except Exception:
        return False
    normalized = (column_name or "").strip().lower()
    for row in rows:
        if str(row.get("name") or "").strip().lower() == normalized:
            return True
    return False




async def ensure_leaderboard_schema(db) -> None:
    """Create leaderboard tables if they do not exist."""
    await d1_run(
        db,
        """
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
        """,
    )
    await d1_run(
        db,
        """
        CREATE TABLE IF NOT EXISTS leaderboard_open_prs (
            org TEXT NOT NULL,
            user_login TEXT NOT NULL,
            open_prs INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (org, user_login)
        )
        """,
    )
    await d1_run(
        db,
        """
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
        """,
    )
    await d1_run(
        db,
        """
        CREATE TABLE IF NOT EXISTS leaderboard_review_credits (
            org TEXT NOT NULL,
            repo TEXT NOT NULL,
            pr_number INTEGER NOT NULL,
            month_key TEXT NOT NULL,
            reviewer_login TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            PRIMARY KEY (org, repo, pr_number, month_key, reviewer_login)
        )
        """,
    )
    await d1_run(
        db,
        """
        CREATE TABLE IF NOT EXISTS leaderboard_backfill_state (
            org TEXT NOT NULL,
            month_key TEXT NOT NULL,
            next_page INTEGER NOT NULL DEFAULT 1,
            completed INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (org, month_key)
        )
        """,
    )
    await d1_run(
        db,
        """
        CREATE TABLE IF NOT EXISTS leaderboard_backfill_repo_done (
            org TEXT NOT NULL,
            month_key TEXT NOT NULL,
            repo TEXT NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (org, month_key, repo)
        )
        """,
    )




async def inc_open_pr(db, org: str, user_login: str, delta: int) -> None:
    from js import console
    now = int(time.time())
    try:
        await d1_run(
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
        console.log(f"[D1] Updated open PR count org={org} user={user_login} delta={delta}")
    except Exception as e:
        console.error(f"[D1] Failed to update open PRs org={org} user={user_login}: {e}")


async def inc_monthly(db, org: str, mk: str, user_login: str, field: str, delta: int = 1) -> None:
    from js import console
    now = int(time.time())
    if field not in {"merged_prs", "closed_prs", "reviews", "comments"}:
        return
    try:
        await d1_run(
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
            (org, mk, user_login, delta, delta, now, delta, delta),
        )
        console.log(f"[D1] Updated {field} org={org} month={mk} user={user_login} +{delta}")
    except Exception as e:
        console.error(f"[D1] Failed to update {field} org={org} month={mk} user={user_login}: {e}")


async def track_pr_opened(payload: dict, env, is_bot_fn, d1_binding_fn) -> None:
    from js import console
    db = d1_binding_fn(env)
    if not db:
        return
    pr = payload.get("pull_request") or {}
    author = pr.get("user") or {}
    if is_bot_fn(author):
        return
    org = (payload.get("repository") or {}).get("owner", {}).get("login", "")
    repo = (payload.get("repository") or {}).get("name", "")
    pr_number = pr.get("number")
    author_login = author.get("login", "")
    if not (org and repo and pr_number and author_login):
        return
    await ensure_leaderboard_schema(db)
    existing = await d1_first(
        db,
        "SELECT state FROM leaderboard_pr_state WHERE org = ? AND repo = ? AND pr_number = ?",
        (org, repo, pr_number),
    )
    if not existing or existing.get("state") != "open":
        await inc_open_pr(db, org, author_login, 1)
    now = int(time.time())
    await d1_run(
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


async def track_pr_closed(payload: dict, env, is_bot_fn, d1_binding_fn) -> None:
    from js import console
    db = d1_binding_fn(env)
    if not db:
        return
    pr = payload.get("pull_request") or {}
    author = pr.get("user") or {}
    if is_bot_fn(author):
        return
    org = (payload.get("repository") or {}).get("owner", {}).get("login", "")
    repo = (payload.get("repository") or {}).get("name", "")
    pr_number = pr.get("number")
    author_login = author.get("login", "")
    closed_at = pr.get("closed_at")
    merged_at = pr.get("merged_at")
    merged = bool(pr.get("merged"))
    closed_ts = parse_github_timestamp(closed_at) if closed_at else int(time.time())
    if not (org and repo and pr_number and author_login):
        return
    await ensure_leaderboard_schema(db)
    existing = await d1_first(
        db,
        "SELECT state, merged, closed_at FROM leaderboard_pr_state WHERE org = ? AND repo = ? AND pr_number = ?",
        (org, repo, pr_number),
    )
    if existing and existing.get("state") == "closed" and int(existing.get("merged") or 0) == int(merged):
        existing_closed_at = int(existing.get("closed_at") or 0)
        if existing_closed_at == int(closed_ts or 0):
            return
    if existing and existing.get("state") == "open":
        await inc_open_pr(db, org, author_login, -1)
    event_ts = parse_github_timestamp(merged_at) if merged and merged_at else closed_ts
    mk = month_key(event_ts)
    if merged:
        await inc_monthly(db, org, mk, author_login, "merged_prs", 1)
    else:
        await inc_monthly(db, org, mk, author_login, "closed_prs", 1)
    now = int(time.time())
    await d1_run(
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


async def track_pr_reopened(payload: dict, env, is_bot_fn, d1_binding_fn) -> None:
    from js import console
    db = d1_binding_fn(env)
    if not db:
        return
    pr = payload.get("pull_request") or {}
    author = pr.get("user") or {}
    if is_bot_fn(author):
        return
    org = (payload.get("repository") or {}).get("owner", {}).get("login", "")
    repo = (payload.get("repository") or {}).get("name", "")
    pr_number = pr.get("number")
    author_login = author.get("login", "")
    if not (org and repo and pr_number and author_login):
        return
    await ensure_leaderboard_schema(db)
    existing = await d1_first(
        db,
        "SELECT state, merged, closed_at FROM leaderboard_pr_state WHERE org = ? AND repo = ? AND pr_number = ?",
        (org, repo, pr_number),
    )
    if existing and existing.get("state") == "closed":
        prev_merged = int(existing.get("merged") or 0)
        prev_closed_at = int(existing.get("closed_at") or 0)
        prev_mk = month_key(prev_closed_at) if prev_closed_at else month_key()
        field = "merged_prs" if prev_merged else "closed_prs"
        await inc_monthly(db, org, prev_mk, author_login, field, -1)
    if not existing or existing.get("state") != "open":
        await inc_open_pr(db, org, author_login, 1)
    now = int(time.time())
    await d1_run(
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


async def track_comment(payload: dict, env, is_bot_fn, is_coderabbit_ping_fn, extract_command_fn, d1_binding_fn) -> None:
    db = d1_binding_fn(env)
    if not db:
        return
    comment = payload.get("comment") or {}
    user = comment.get("user") or {}
    if is_bot_fn(user):
        return
    body = comment.get("body", "")
    if is_coderabbit_ping_fn(body):
        return
    if extract_command_fn(body):
        return
    org = (payload.get("repository") or {}).get("owner", {}).get("login", "")
    login = user.get("login", "")
    created_at = comment.get("created_at")
    if not (org and login):
        return
    await ensure_leaderboard_schema(db)
    mk = month_key(parse_github_timestamp(created_at) if created_at else int(time.time()))
    await inc_monthly(db, org, mk, login, "comments", 1)


async def track_review(payload: dict, env, is_bot_fn, d1_binding_fn) -> None:
    from js import console
    db = d1_binding_fn(env)
    if not db:
        console.log("[D1] REVIEW: No DB binding")
        return
    review = payload.get("review") or {}
    reviewer = review.get("user") or {}
    if is_bot_fn(reviewer):
        return
    pr = payload.get("pull_request") or {}
    org = (payload.get("repository") or {}).get("owner", {}).get("login", "")
    repo = (payload.get("repository") or {}).get("name", "")
    pr_number = pr.get("number")
    reviewer_login = reviewer.get("login", "")
    submitted_at = review.get("submitted_at")
    if not (org and repo and pr_number and reviewer_login):
        return
    await ensure_leaderboard_schema(db)
    mk = month_key(parse_github_timestamp(submitted_at) if submitted_at else int(time.time()))
    exists = await d1_first(
        db,
        """
        SELECT 1 FROM leaderboard_review_credits
        WHERE org = ? AND repo = ? AND pr_number = ? AND month_key = ? AND reviewer_login = ?
        """,
        (org, repo, pr_number, mk, reviewer_login),
    )
    if exists:
        return
    cnt_row = await d1_first(
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
    await d1_run(
        db,
        """
        INSERT INTO leaderboard_review_credits (org, repo, pr_number, month_key, reviewer_login, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (org, repo, pr_number, mk, reviewer_login, int(time.time())),
    )
    await inc_monthly(db, org, mk, reviewer_login, "reviews", 1)




async def calculate_stats_from_d1(owner: str, env) -> Optional[dict]:
    """Read current-month leaderboard stats from D1 if configured."""
    from js import console
    db = d1_binding(env)
    if not db:
        console.error("[D1] No D1 binding available")
        return None
    await ensure_leaderboard_schema(db)
    mk = month_key()
    start_timestamp, end_timestamp = month_window(mk)
    monthly_rows = await d1_all(
        db,
        """
        SELECT user_login, merged_prs, closed_prs, reviews, comments
        FROM leaderboard_monthly_stats
        WHERE org = ? AND month_key = ?
        """,
        (owner, mk),
    )
    open_rows = await d1_all(
        db,
        """
        SELECT user_login, open_prs
        FROM leaderboard_open_prs
        WHERE org = ?
        """,
        (owner,),
    )
    user_stats = {}

    def ensure(login: str):
        if login not in user_stats:
            user_stats[login] = {
                "openPrs": 0, "mergedPrs": 0, "closedPrs": 0,
                "reviews": 0, "comments": 0, "total": 0,
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
        key=lambda u: (-u["total"], -u["mergedPrs"], -u["reviews"], u["login"].lower()),
    )
    return {
        "users": user_stats,
        "sorted": sorted_users,
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp,
    }




async def get_backfill_state(db, owner: str, mk: str) -> dict:
    row = await d1_first(
        db,
        "SELECT next_page, completed FROM leaderboard_backfill_state WHERE org = ? AND month_key = ?",
        (owner, mk),
    )
    if row:
        return {"next_page": int(row.get("next_page") or 1), "completed": bool(int(row.get("completed") or 0))}
    return {"next_page": 1, "completed": False}


async def set_backfill_state(db, owner: str, mk: str, next_page: int, completed: bool) -> None:
    from js import console
    try:
        await d1_run(
            db,
            """
            INSERT INTO leaderboard_backfill_state (org, month_key, next_page, completed, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(org, month_key) DO UPDATE SET
                next_page = excluded.next_page,
                completed = excluded.completed,
                updated_at = excluded.updated_at
            """,
            (owner, mk, next_page, 1 if completed else 0, int(time.time())),
        )
    except Exception as e:
        console.error(f"[Backfill] Failed to update state: {e}")


async def reset_leaderboard_month(org: str, mk: str, db) -> dict:
    """Clear all leaderboard data for an org/month so a fresh backfill can re-populate it."""
    await ensure_leaderboard_schema(db)
    deleted: dict = {}
    for table, params in [
        ("leaderboard_monthly_stats", (org, mk)),
        ("leaderboard_backfill_repo_done", (org, mk)),
        ("leaderboard_review_credits", (org, mk)),
        ("leaderboard_backfill_state", (org, mk)),
    ]:
        try:
            await d1_run(db, f"DELETE FROM {table} WHERE org = ? AND month_key = ?", params)
            deleted[table] = "cleared"
        except Exception as e:
            from js import console
            console.error(f"[AdminReset] Error clearing {table}: {e}")
            deleted[table] = f"error: {e}"
    start_ts, end_ts = month_window(mk)
    try:
        await d1_run(
            db,
            """
            DELETE FROM leaderboard_pr_state
            WHERE org = ?
              AND (
                closed_at BETWEEN ? AND ?
                OR (state = 'open' AND closed_at IS NULL AND updated_at BETWEEN ? AND ?)
              )
            """,
            (org, start_ts, end_ts, start_ts, end_ts),
        )
        deleted["leaderboard_pr_state"] = "cleared"
    except Exception as e:
        from js import console
        console.error(f"[AdminReset] Error clearing leaderboard_pr_state: {e}")
        deleted["leaderboard_pr_state"] = f"error: {e}"
    try:
        await d1_run(db, "DELETE FROM leaderboard_open_prs WHERE org = ?", (org,))
        deleted["leaderboard_open_prs"] = "cleared"
    except Exception as e:
        from js import console
        console.error(f"[AdminReset] Error clearing leaderboard_open_prs: {e}")
        deleted["leaderboard_open_prs"] = f"error: {e}"
    return deleted



def format_leaderboard_comment(author_login: str, leaderboard_data: dict, owner: str, note: str = "") -> str:
    """Format a leaderboard comment for a specific user."""
    sorted_users = leaderboard_data["sorted"]
    start_ts = leaderboard_data["start_timestamp"]
    author_index = -1
    for i, user in enumerate(sorted_users):
        if user["login"] == author_login:
            author_index = i
            break
    month_struct = time.gmtime(start_ts)
    display_month = time.strftime("%B %Y", month_struct)
    comment = LEADERBOARD_MARKER + "\n"
    comment += "## 📊 Monthly Leaderboard\n\n"
    comment += f"Hi @{author_login}! Here's how you rank for {display_month}:\n\n"
    comment += "| Rank | User | Open PRs | PRs (merged) | PRs (closed) | Reviews | Comments | Total |\n"
    comment += "| --- | --- | --- | --- | --- | --- | --- | --- |\n"

    def row_for(rank: int, u: dict, bold: bool = False, medal: str = "") -> str:
        av = avatar_img_tag(u["login"])
        user_cell = f"{av} **`@{u['login']}`** ✨" if bold else f"{av} `@{u['login']}`"
        rank_cell = f"{medal} {rank}" if medal else f"{rank}"
        return (f"| {rank_cell} | {user_cell} | {u['openPrs']} | {u['mergedPrs']} | "
                f"{u['closedPrs']} | {u['reviews']} | {u['comments']} | **{u['total']}** |")

    if not sorted_users:
        av = avatar_img_tag(author_login)
        comment += f"| - | {av} **`@{author_login}`** ✨ | 0 | 0 | 0 | 0 | 0 | **0** |\n"
        comment += "\n_No leaderboard activity has been recorded for this month yet._\n"
    elif author_index == -1:
        for i in range(min(5, len(sorted_users))):
            medal = ["🥇", "🥈", "🥉"][i] if i < 3 else ""
            comment += row_for(i + 1, sorted_users[i], False, medal) + "\n"
    else:
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


def format_reviewer_leaderboard_comment(leaderboard_data: dict, owner: str, pr_reviewers: list = None) -> str:
    """Format a reviewer leaderboard comment showing top reviewers for the month."""
    sorted_users = leaderboard_data["sorted"]
    start_ts = leaderboard_data["start_timestamp"]
    reviewer_sorted = sorted(
        [u for u in sorted_users if u["reviews"] > 0],
        key=lambda u: (-u["reviews"], u["login"].lower()),
    )
    month_struct = time.gmtime(start_ts)
    display_month = time.strftime("%B %Y", month_struct)
    comment = REVIEWER_LEADERBOARD_MARKER + "\n"
    comment += "## 🔍 Reviewer Leaderboard\n\n"
    comment += f"Top reviewers for {display_month} (across the {owner} org):\n\n"
    medals = ["🥇", "🥈", "🥉"]

    def row_for(rank: int, u: dict, highlight: bool = False) -> str:
        medal = medals[rank - 1] if rank <= 3 else ""
        rank_cell = f"{medal} {rank}" if medal else f"{rank}"
        av = avatar_img_tag(u["login"])
        user_cell = f"{av} **`@{u['login']}`** ⭐" if highlight else f"{av} `@{u['login']}`"
        return f"| {rank_cell} | {user_cell} | {u['reviews']} |"

    comment += "| Rank | Reviewer | Reviews this month |\n"
    comment += "| --- | --- | --- |\n"
    pr_reviewer_set = set(pr_reviewers or [])
    if not reviewer_sorted:
        comment += "| - | _No review activity recorded yet_ | 0 |\n"
    else:
        total = len(reviewer_sorted)
        center_idx = None
        if pr_reviewer_set:
            for i, u in enumerate(reviewer_sorted):
                if u["login"] in pr_reviewer_set:
                    center_idx = i
                    break
        if center_idx is not None:
            start_idx = center_idx - 2
            end_idx = center_idx + 2
            if start_idx < 0:
                end_idx -= start_idx
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
            for i, u in enumerate(reviewer_sorted[:5]):
                highlight = u["login"] in pr_reviewer_set
                comment += row_for(i + 1, u, highlight) + "\n"
    comment += "\n---\n"
    comment += (
        "Reviews earn **+5 points** each in the monthly leaderboard "
        "(first two reviewers per PR). Thank you to everyone who helps review PRs! 🙏\n"
    )
    return comment
