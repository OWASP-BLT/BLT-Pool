"""D1 database helpers and leaderboard tracking functions."""

import calendar
import json
import time
from typing import Optional, Tuple

from js import console

from constants import INITIAL_MENTORS


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------


def _pr_state_upsert_sql() -> str:
    return """
        INSERT INTO leaderboard_pr_state (org, repo, pr_number, author_login, state, merged, closed_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(org, repo, pr_number) DO UPDATE SET
            author_login = excluded.author_login,
            state = excluded.state,
            merged = excluded.merged,
            closed_at = excluded.closed_at,
            updated_at = excluded.updated_at
    """


async def _d1_upsert_pr_state(
    db,
    org: str,
    repo: str,
    pr_number: int,
    author_login: str,
    state: str,
    merged: int,
    closed_at: Optional[int],
    updated_at: int,
) -> None:
    await _d1_run(
        db,
        _pr_state_upsert_sql(),
        (org, repo, pr_number, author_login, state, int(merged), closed_at, updated_at),
    )



def _env_int(env, name: str, default: int) -> int:
    raw = None
    if env is not None:
        if isinstance(env, dict):
            raw = env.get(name)
        else:
            raw = getattr(env, name, None)
    if raw is None:
        return default
    try:
        value = int(raw)
        if value <= 0:
            return default
        return value
    except Exception:
        return default


def _reconcile_settings(env) -> dict:
    return {
        "repos_per_page": _env_int(env, "RECONCILE_REPOS_PER_PAGE", 100),
        "prs_per_page": _env_int(env, "RECONCILE_PRS_PER_PAGE", 100),
        "max_closed_pages": _env_int(env, "RECONCILE_MAX_CLOSED_PAGES_PER_REPO", 20),
        "max_open_pages": _env_int(env, "RECONCILE_MAX_OPEN_PAGES_PER_REPO", 20),
        "lock_lease_seconds": _env_int(env, "RECONCILE_LOCK_LEASE_SECONDS", 120),
        "timeout_seconds": _env_int(env, "RECONCILE_TIMEOUT_SECONDS", 20),
    }


def _month_key(ts: Optional[int] = None) -> str:
    """Return YYYY-MM month key for UTC timestamp (or now)."""
    if ts is None:
        ts = int(time.time())
    return time.strftime("%Y-%m", time.gmtime(ts))


def _month_window(month_key: str) -> Tuple[int, int]:
    """Return start/end timestamps (UTC) for a YYYY-MM key."""
    year, month = month_key.split("-")
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


def _d1_binding(env):
    """Return D1 binding object if configured, otherwise None."""
    db = getattr(env, "LEADERBOARD_DB", None) if env else None
    return db


async def _d1_run(db, sql: str, params: tuple = ()):
    try:
        stmt = db.prepare(sql)
        if params:
            stmt = stmt.bind(*params)
        result = await stmt.run()
        return result
    except Exception as e:
        console.error(f"[D1.run] Error executing {sql[:60]}: {e}")
        raise


async def _d1_batch(db, statements: list):
    """Execute a list of (sql, params) tuples as a D1 batch when supported."""
    if not statements:
        return []
    try:
        batch_fn = getattr(db, "batch", None)
        if callable(batch_fn):
            prepared = []
            for sql, params in statements:
                stmt = db.prepare(sql)
                if params:
                    stmt = stmt.bind(*params)
                prepared.append(stmt)
            try:
                from pyodide.ffi import to_js  # noqa: PLC0415 - runtime import
                return await batch_fn(to_js(prepared))
            except Exception:
                return await batch_fn(prepared)

        # Local-test fallback for mock DBs that do not implement batch.
        results = []
        for sql, params in statements:
            results.append(await _d1_run(db, sql, params or ()))
        return results
    except Exception as e:
        console.error(f"[D1.batch] Error executing batch ({len(statements)} statements): {e}")
        raise


def _to_py(value):
    """Best-effort conversion for JS proxy values returned by Workers runtime."""
    try:
        from pyodide.ffi import to_py  # noqa: PLC0415 - runtime import
        return to_py(value)
    except Exception:
        return value


def _d1_result_to_dict(raw_result):
    """Best-effort conversion for D1 run() result to a Python dict."""
    try:
        from js import JSON as JS_JSON  # noqa: PLC0415 - runtime import
        js_json = JS_JSON.stringify(raw_result)
        parsed = json.loads(str(js_json))
        if isinstance(parsed, dict):
            return parsed
    except Exception as e:
        console.log(f"[D1.run] Result conversion failed; falling back ({e})")

    converted = _to_py(raw_result)
    if isinstance(converted, dict):
        return converted
    try:
        return dict(converted)
    except Exception:
        return None


async def _d1_all(db, sql: str, params: tuple = ()) -> list:
    stmt = db.prepare(sql)
    if params:
        stmt = stmt.bind(*params)
    raw_result = await stmt.all()

    # Cloudflare D1 returns JS proxy objects at runtime; serialize through JS JSON
    # first to reliably convert to Python dict/list structures.
    try:
        from js import JSON as JS_JSON  # noqa: PLC0415 - runtime import
        js_json = JS_JSON.stringify(raw_result)
        parsed = json.loads(str(js_json))
        rows = parsed.get("results") if isinstance(parsed, dict) else None
        if isinstance(rows, list):
            return rows
    except Exception:
        pass

    # Fallback path for local tests or non-JS proxy values.
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


async def _d1_first(db, sql: str, params: tuple = ()):
    rows = await _d1_all(db, sql, params)
    return rows[0] if rows else None


async def _ensure_leaderboard_schema(db) -> None:
    """Create leaderboard tables if they do not exist."""
    await _d1_run(
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
            pr_updated_at INTEGER NOT NULL DEFAULT 0,
            review_updated_at INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (org, month_key, user_login)
        )
        """,
    )
    await _d1_run(
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
    await _d1_run(
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
    await _d1_run(
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
    await _d1_run(
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
    await _d1_run(
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
    await _d1_run(
        db,
        """
        CREATE TABLE IF NOT EXISTS leaderboard_reconcile_locks (
            org TEXT NOT NULL PRIMARY KEY,
            holder TEXT NOT NULL,
            lock_until INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """,
    )
    await _d1_run(
        db,
        """
        CREATE TABLE IF NOT EXISTS leaderboard_runtime_meta (
            key TEXT NOT NULL PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """,
    )
    await _d1_run(
        db,
        """
        CREATE TABLE IF NOT EXISTS mentor_assignments (
            org TEXT NOT NULL,
            mentor_login TEXT NOT NULL,
            issue_repo TEXT NOT NULL,
            issue_number INTEGER NOT NULL,
            assigned_at INTEGER NOT NULL,
            mentee_login TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (org, issue_repo, issue_number)
        )
        """,
    )
    # Migration: add mentee_login column to existing tables that pre-date this field.
    try:
        await _d1_run(
            db,
            "ALTER TABLE mentor_assignments ADD COLUMN mentee_login TEXT NOT NULL DEFAULT ''",
        )
    except Exception:
        pass  # Column already exists — ignore the error.
    # Migration: add PR-specific fence column for merged/closed reconciliation.
    try:
        await _d1_run(
            db,
            "ALTER TABLE leaderboard_monthly_stats ADD COLUMN pr_updated_at INTEGER NOT NULL DEFAULT 0",
        )
    except Exception as e:
        msg = str(e).lower()
        if "duplicate column" in msg or "already exists" in msg:
            pass  # Column already exists — ignore the known migration race.
        else:
            console.error(
                "[D1.migration] Failed SQL: ALTER TABLE leaderboard_monthly_stats "
                f"ADD COLUMN pr_updated_at INTEGER NOT NULL DEFAULT 0; error={e}"
            )
            raise
    # Migration: add review-specific fence column for review-count reconciliation.
    try:
        await _d1_run(
            db,
            "ALTER TABLE leaderboard_monthly_stats ADD COLUMN review_updated_at INTEGER NOT NULL DEFAULT 0",
        )
    except Exception as e:
        msg = str(e).lower()
        if "duplicate column" in msg or "already exists" in msg:
            pass
        else:
            console.error(
                "[D1.migration] Failed SQL: ALTER TABLE leaderboard_monthly_stats "
                f"ADD COLUMN review_updated_at INTEGER NOT NULL DEFAULT 0; error={e}"
            )
            raise
    await _d1_run(
        db,
        """
        CREATE TABLE IF NOT EXISTS mentors (
            github_username TEXT NOT NULL PRIMARY KEY,
            name TEXT NOT NULL,
            specialties TEXT NOT NULL DEFAULT '[]',
            max_mentees INTEGER NOT NULL DEFAULT 3,
            active INTEGER NOT NULL DEFAULT 1,
            timezone TEXT NOT NULL DEFAULT '',
            referred_by TEXT NOT NULL DEFAULT ''
        )
        """,
    )
    await _d1_run(
        db,
        """
        CREATE TABLE IF NOT EXISTS mentor_stats_cache (
            org TEXT NOT NULL,
            github_username TEXT NOT NULL,
            merged_prs INTEGER NOT NULL DEFAULT 0,
            reviews INTEGER NOT NULL DEFAULT 0,
            fetched_at INTEGER NOT NULL,
            PRIMARY KEY (org, github_username)
        )
        """,
    )
    await _populate_mentors_table(db)


# ---------------------------------------------------------------------------
# Mentor table helpers
# ---------------------------------------------------------------------------

async def _populate_mentors_table(db) -> None:
    """Seed the mentors table with the initial mentor list (idempotent).

    Uses INSERT OR IGNORE so that existing rows are never overwritten; safe
    to call on every cold start.
    """
    for m in INITIAL_MENTORS:
        try:
            await _d1_run(
                db,
                """
                INSERT OR IGNORE INTO mentors
                    (github_username, name, specialties, max_mentees, active, timezone, referred_by)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    m["github_username"],
                    m["name"],
                    json.dumps(m.get("specialties") or []),
                    m.get("max_mentees", 3),
                    1 if m.get("active", True) else 0,
                    m.get("timezone", "") or "",
                    m.get("referred_by", "") or "",
                ),
            )
        except Exception as exc:
            console.error(f"[MentorPool] Failed to seed mentor {m['github_username']}: {exc}")


async def _load_mentors_from_d1(db) -> list:
    """Load the mentor list from the D1 ``mentors`` table.

    Returns a list of mentor dicts compatible with the rest of the codebase
    (same keys as the old YAML format).  Returns ``[]`` on error.
    """
    try:
        await _ensure_leaderboard_schema(db)
        rows = await _d1_all(
            db,
            "SELECT github_username, name, specialties, max_mentees, active, timezone, referred_by FROM mentors",
        )
        mentors = []
        for row in rows:
            try:
                specialties = json.loads(row.get("specialties") or "[]")
            except Exception:
                specialties = []
            mentors.append({
                "github_username": row["github_username"],
                "name": row["name"],
                "specialties": specialties,
                "max_mentees": int(row.get("max_mentees") or 3),
                "active": bool(row.get("active", 1)),
                "timezone": row.get("timezone") or "",
                "referred_by": row.get("referred_by") or "",
            })
        console.log(f"[MentorPool] Loaded {len(mentors)} mentors from D1")
        return mentors
    except Exception as exc:
        console.error(f"[MentorPool] Failed to load mentors from D1: {exc}")
        return []


async def _d1_add_mentor(
    db,
    github_username: str,
    name: str,
    specialties: list,
    max_mentees: int = 3,
    active: bool = True,
    timezone: str = "",
    referred_by: str = "",
) -> None:
    """Insert or replace a mentor row in the D1 ``mentors`` table."""
    await _d1_run(
        db,
        """
        INSERT INTO mentors
            (github_username, name, specialties, max_mentees, active, timezone, referred_by)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(github_username) DO UPDATE SET
            name        = excluded.name,
            specialties = excluded.specialties,
            max_mentees = excluded.max_mentees,
            active      = excluded.active,
            timezone    = excluded.timezone,
            referred_by = excluded.referred_by
        """,
        (
            github_username,
            name,
            json.dumps(specialties),
            max_mentees,
            1 if active else 0,
            timezone or "",
            referred_by or "",
        ),
    )


async def _d1_record_mentor_assignment(
    db, org: str, mentor_login: str, repo: str, issue_number: int, mentee_login: str = ""
) -> None:
    """Upsert a mentor→issue assignment into D1 for load-map tracking."""
    now = int(time.time())
    try:
        await _d1_run(
            db,
            """
            INSERT INTO mentor_assignments (org, mentor_login, issue_repo, issue_number, assigned_at, mentee_login)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(org, issue_repo, issue_number) DO UPDATE SET
                mentor_login = excluded.mentor_login,
                mentee_login = excluded.mentee_login,
                assigned_at  = excluded.assigned_at
            """,
            (org, mentor_login, repo, issue_number, now, mentee_login),
        )
        console.log(f"[D1] Recorded mentor assignment: @{mentor_login} → {org}/{repo}#{issue_number}")
    except Exception as exc:
        console.error(f"[D1] Failed to record mentor assignment: {exc}")


async def _d1_remove_mentor_assignment(db, org: str, repo: str, issue_number: int) -> None:
    """Remove a mentor assignment record from D1 (used on handoff/issue close)."""
    try:
        await _d1_run(
            db,
            "DELETE FROM mentor_assignments WHERE org = ? AND issue_repo = ? AND issue_number = ?",
            (org, repo, issue_number),
        )
        console.log(f"[D1] Removed mentor assignment: {org}/{repo}#{issue_number}")
    except Exception as exc:
        console.error(f"[D1] Failed to remove mentor assignment: {exc}")


async def _d1_get_mentor_loads(db, org: str) -> dict:
    """Return a mapping of mentor_login → active assignment count from D1."""
    try:
        rows = await _d1_all(
            db,
            """
            SELECT mentor_login, COUNT(*) as cnt
            FROM mentor_assignments
            WHERE org = ?
            GROUP BY mentor_login
            """,
            (org,),
        )
        return {
            row["mentor_login"]: int(row.get("cnt") or 0)
            for row in rows
            if row.get("mentor_login")
        }
    except Exception as exc:
        console.error(f"[D1] Failed to get mentor loads: {exc}")
        return {}


async def _d1_get_active_assignments(db, org: str) -> list:
    """Return all active mentor assignments from D1 for the given org.

    Returns a list of dicts with keys: org, mentor_login, mentee_login, issue_repo, issue_number, assigned_at.
    Returns an empty list when D1 is unavailable or the query fails.
    """
    try:
        rows = await _d1_all(
            db,
            """
            SELECT org, mentor_login, mentee_login, issue_repo, issue_number, assigned_at
            FROM mentor_assignments
            WHERE org = ?
            ORDER BY assigned_at DESC
            """,
            (org,),
        )
        return [
            {
                "org": row.get("org", org),
                "mentor_login": row.get("mentor_login", ""),
                "mentee_login": row.get("mentee_login", ""),
                "issue_repo": row.get("issue_repo", ""),
                "issue_number": int(row.get("issue_number") or 0),
                "assigned_at": int(row.get("assigned_at") or 0),
            }
            for row in rows
            if row.get("mentor_login") and row.get("issue_repo")
        ]
    except Exception as exc:
        console.error(f"[D1] Failed to get active assignments: {exc}")
        return []


def _time_ago(ts: int) -> str:
    """Return a human-readable 'X time ago' string for a Unix timestamp."""
    diff = int(time.time()) - ts
    if diff < 60:
        return "just now"
    if diff < 3600:
        m = diff // 60
        return f"{m} minute{'s' if m != 1 else ''} ago"
    if diff < 86400:
        h = diff // 3600
        return f"{h} hour{'s' if h != 1 else ''} ago"
    if diff < 86400 * 30:
        d = diff // 86400
        return f"{d} day{'s' if d != 1 else ''} ago"
    if diff < 86400 * 365:
        mo = diff // (86400 * 30)
        return f"{mo} month{'s' if mo != 1 else ''} ago"
    y = diff // (86400 * 365)
    return f"{y} year{'s' if y != 1 else ''} ago"


async def _d1_get_user_comment_totals(db, org: str, logins: list) -> dict:
    """Return total all-time comment counts per user from leaderboard_monthly_stats.

    Args:
        db:     D1 database binding.
        org:    GitHub organisation name.
        logins: List of GitHub usernames to look up.

    Returns a ``{login: total_comments}`` mapping.  Missing users default to 0.
    """
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
        result = await _d1_run(
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
    is_pr_field = field in {"merged_prs", "closed_prs"}
    is_review_field = field == "reviews"
    try:
        if is_pr_field:
            result = await _d1_run(
                db,
                f"""
                INSERT INTO leaderboard_monthly_stats (org, month_key, user_login, {field}, updated_at, pr_updated_at)
                VALUES (?, ?, ?, CASE WHEN ? < 0 THEN 0 ELSE ? END, ?, ?)
                ON CONFLICT(org, month_key, user_login) DO UPDATE SET
                    {field} = CASE
                        WHEN leaderboard_monthly_stats.{field} + ? < 0 THEN 0
                        ELSE leaderboard_monthly_stats.{field} + ?
                    END,
                    updated_at = excluded.updated_at,
                    pr_updated_at = excluded.pr_updated_at
                """,
                (org, month_key, user_login, delta, delta, now, now, delta, delta),
            )
        elif is_review_field:
            result = await _d1_run(
                db,
                f"""
                INSERT INTO leaderboard_monthly_stats (org, month_key, user_login, {field}, updated_at, review_updated_at)
                VALUES (?, ?, ?, CASE WHEN ? < 0 THEN 0 ELSE ? END, ?, ?)
                ON CONFLICT(org, month_key, user_login) DO UPDATE SET
                    {field} = CASE
                        WHEN leaderboard_monthly_stats.{field} + ? < 0 THEN 0
                        ELSE leaderboard_monthly_stats.{field} + ?
                    END,
                    updated_at = excluded.updated_at,
                    review_updated_at = excluded.review_updated_at
                """,
                (org, month_key, user_login, delta, delta, now, now, delta, delta),
            )
        else:
            result = await _d1_run(
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


def _extract_pr_tracking_context(payload: dict) -> tuple:
    pr = payload.get("pull_request") or {}
    author = pr.get("user") or {}
    if _is_bot(author):
        return None, None, None, None, None, None
    org = (payload.get("repository") or {}).get("owner", {}).get("login", "")
    repo = (payload.get("repository") or {}).get("name", "")
    pr_number = pr.get("number")
    author_login = author.get("login", "")
    if not (org and repo and pr_number and author_login):
        return None, None, None, None, None, None
    return pr, author, org, repo, pr_number, author_login


async def _track_pr_opened_in_d1(payload: dict, env) -> None:
    db = _d1_binding(env)
    if not db:
        return
    pr, author, org, repo, pr_number, author_login = _extract_pr_tracking_context(payload)
    if not org:
        return

    await _ensure_leaderboard_schema(db)
    existing = await _d1_first(
        db,
        "SELECT author_login, state, merged, closed_at FROM leaderboard_pr_state WHERE org = ? AND repo = ? AND pr_number = ?",
        (org, repo, pr_number),
    )
    if existing and existing.get("state") == "closed":
        # Ignore stale opened deliveries for PRs already known as closed.
        # True reopens are handled by _track_pr_reopened_in_d1.
        return

    if not existing or existing.get("state") != "open":
        await _d1_inc_open_pr(db, org, author_login, 1)

    now = int(time.time())
    await _d1_upsert_pr_state(db, org, repo, pr_number, author_login, "open", 0, None, now)


async def _track_pr_closed_in_d1(payload: dict, env) -> None:
    db = _d1_binding(env)
    if not db:
        return
    pr, author, org, repo, pr_number, author_login = _extract_pr_tracking_context(payload)
    if not org:
        return
    closed_at = pr.get("closed_at")
    merged_at = pr.get("merged_at")
    merged = bool(pr.get("merged"))
    closed_ts = _parse_github_timestamp(closed_at) if closed_at else int(time.time())

    await _ensure_leaderboard_schema(db)
    existing = await _d1_first(
        db,
        "SELECT author_login, state, merged, closed_at FROM leaderboard_pr_state WHERE org = ? AND repo = ? AND pr_number = ?",
        (org, repo, pr_number),
    )

    # Idempotency: skip if we already recorded the same closed state.
    if existing and existing.get("state") == "closed" and int(existing.get("merged") or 0) == int(merged):
        existing_closed_at = int(existing.get("closed_at") or 0)
        if existing_closed_at == int(closed_ts or 0):
            return

    # If this PR is already tracked as closed but with a different state/timestamp,
    # reverse the previous monthly credit before applying the new one.
    if existing and existing.get("state") == "closed":
        existing_author_login = existing.get("author_login") or author_login
        prev_merged = int(existing.get("merged") or 0)
        prev_closed_at = int(existing.get("closed_at") or 0)
        prev_mk = _month_key(prev_closed_at) if prev_closed_at else _month_key()
        prev_field = "merged_prs" if prev_merged else "closed_prs"
        await _d1_inc_monthly(db, org, prev_mk, existing_author_login, prev_field, -1)

    if existing and existing.get("state") == "open":
        open_author_login = existing.get("author_login") or author_login
        await _d1_inc_open_pr(db, org, open_author_login, -1)

    event_ts = _parse_github_timestamp(merged_at) if merged and merged_at else closed_ts
    mk = _month_key(event_ts)
    if merged:
        await _d1_inc_monthly(db, org, mk, author_login, "merged_prs", 1)
    else:
        await _d1_inc_monthly(db, org, mk, author_login, "closed_prs", 1)

    now = int(time.time())
    await _d1_upsert_pr_state(db, org, repo, pr_number, author_login, "closed", 1 if merged else 0, closed_ts, now)


async def _track_pr_reopened_in_d1(payload: dict, env) -> None:
    db = _d1_binding(env)
    if not db:
        return

    pr, author, org, repo, pr_number, author_login = _extract_pr_tracking_context(payload)
    if not org:
        return

    await _ensure_leaderboard_schema(db)
    existing = await _d1_first(
        db,
        "SELECT author_login, state, merged, closed_at FROM leaderboard_pr_state WHERE org = ? AND repo = ? AND pr_number = ?",
        (org, repo, pr_number),
    )

    # Reopening should reverse the previous close/merge credit so the final
    # state remains accurate when the PR is closed again later.
    if existing and existing.get("state") == "closed":
        existing_author_login = existing.get("author_login") or author_login
        prev_merged = int(existing.get("merged") or 0)
        prev_closed_at = int(existing.get("closed_at") or 0)
        prev_mk = _month_key(prev_closed_at) if prev_closed_at else _month_key()
        field = "merged_prs" if prev_merged else "closed_prs"
        await _d1_inc_monthly(db, org, prev_mk, existing_author_login, field, -1)

    if not existing or existing.get("state") != "open":
        await _d1_inc_open_pr(db, org, author_login, 1)

    now = int(time.time())
    await _d1_upsert_pr_state(db, org, repo, pr_number, author_login, "open", 0, None, now)


async def _track_comment_in_d1(payload: dict, env) -> None:
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
    # Ignore slash commands so bot commands do not inflate leaderboard comments.
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
        console.log("[D1] REVIEW: No DB binding")
        return
    review = payload.get("review") or {}
    reviewer = review.get("user") or {}
    if _is_bot(reviewer):
        bot_name = reviewer.get("login", "unknown")
        console.log(f"[D1] REVIEW: Skipped bot {bot_name}")
        return
    pr = payload.get("pull_request") or {}
    org = (payload.get("repository") or {}).get("owner", {}).get("login", "")
    repo = (payload.get("repository") or {}).get("name", "")
    pr_number = pr.get("number")
    reviewer_login = reviewer.get("login", "")
    submitted_at = review.get("submitted_at")
    if not (org and repo and pr_number and reviewer_login):
        console.log(f"[D1] REVIEW: Missing fields org={bool(org)} repo={bool(repo)} pr={pr_number} reviewer={reviewer_login}")
        return
    
    console.log(f"[D1] REVIEW: Processing {reviewer_login} reviewing {org}/{repo}#{pr_number}")

    await _ensure_leaderboard_schema(db)
    mk = _month_key(_parse_github_timestamp(submitted_at) if submitted_at else int(time.time()))

    # Only first two unique reviewers per PR/month get credit.
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


