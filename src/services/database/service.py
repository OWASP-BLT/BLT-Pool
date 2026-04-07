"""Database service for BLT-Pool D1 operations."""

import json
import time
from typing import Optional

try:
    from js import JSON, console
except ImportError:
    # Fallback for testing outside Cloudflare Workers
    class _MockConsole:
        def log(self, *args, **kwargs): pass
        def error(self, *args, **kwargs): pass
    console = _MockConsole()
    JSON = None

from services.mentor_seed import INITIAL_MENTORS


class DatabaseService:
    """Encapsulate all D1 database operations for BLT-Pool."""

    def __init__(self, db):
        """Initialize DatabaseService with a D1 binding.
        
        Args:
            db: Cloudflare D1 database binding
        """
        self.db = db

    @staticmethod
    def get_binding(env):
        """Return D1 binding object if configured, otherwise None."""
        return getattr(env, "LEADERBOARD_DB", None) if env else None

    async def run(self, sql: str, params: tuple = ()):
        """Execute a SQL statement without returning results."""
        try:
            stmt = self.db.prepare(sql)
            if params:
                for p in params:
                    stmt = stmt.bind(p)
            await stmt.run()
        except Exception as e:
            console.error(f"[D1] Error running SQL: {e}")
            raise

    async def all(self, sql: str, params: tuple = ()) -> list:
        """Execute a SQL query and return all rows as a list of dicts."""
        stmt = self.db.prepare(sql)
        if params:
            for p in params:
                stmt = stmt.bind(p)
        raw_result = await stmt.all()

        # Cloudflare D1 returns JS proxy objects at runtime; serialize through JS JSON
        # first to reliably convert to Python dict/list structures.
        try:
            from js import Object
            from pyodide.ffi import to_js
            json_str = JSON.stringify(to_js(raw_result, dict_converter=Object.fromEntries))
            result = json.loads(json_str)
        except Exception:
            result = self._to_py(raw_result)

        # Fallback path for local tests or non-JS proxy values.
        result = self._to_py(result)
        rows = None
        if isinstance(result, dict):
            rows = result.get("results")
        if rows is None:
            if hasattr(raw_result, "results"):
                rows = getattr(raw_result, "results", None)
            elif isinstance(raw_result, list):
                rows = raw_result

        rows = self._to_py(rows)
        if rows is None:
            return []
        if isinstance(rows, list):
            return rows
        try:
            return list(rows)
        except Exception:
            return []

    async def first(self, sql: str, params: tuple = ()):
        """Execute a SQL query and return the first row, or None."""
        rows = await self.all(sql, params)
        return rows[0] if rows else None

    @staticmethod
    def _to_py(value):
        """Best-effort conversion for JS proxy values returned by Workers runtime."""
        try:
            return json.loads(JSON.stringify(value))
        except Exception:
            return value

    # -------------------------------------------------------------------------
    # Schema Management
    # -------------------------------------------------------------------------

    async def ensure_schema(self) -> None:
        """Create all necessary tables if they do not exist."""
        await self._create_leaderboard_tables()
        await self._create_mentor_tables()
        await self._migrate_mentor_assignments()
        await self._populate_mentors_table()

    async def _create_leaderboard_tables(self) -> None:
        """Create leaderboard-related tables."""
        await self.run(
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
            """
        )
        await self.run(
            """
            CREATE TABLE IF NOT EXISTS leaderboard_open_prs (
                org TEXT NOT NULL,
                user_login TEXT NOT NULL,
                open_prs INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (org, user_login)
            )
            """
        )
        await self.run(
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
            """
        )
        await self.run(
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
            """
        )
        await self.run(
            """
            CREATE TABLE IF NOT EXISTS leaderboard_backfill_state (
                org TEXT NOT NULL,
                month_key TEXT NOT NULL,
                next_page INTEGER NOT NULL DEFAULT 1,
                completed INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (org, month_key)
            )
            """
        )
        await self.run(
            """
            CREATE TABLE IF NOT EXISTS leaderboard_backfill_repo_done (
                org TEXT NOT NULL,
                month_key TEXT NOT NULL,
                repo TEXT NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (org, month_key, repo)
            )
            """
        )

    async def _create_mentor_tables(self) -> None:
        """Create mentor-related tables."""
        await self.run(
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
            """
        )
        await self.run(
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
            """
        )

    async def _migrate_mentor_assignments(self) -> None:
        """Add mentee_login column to existing tables that pre-date this field."""
        try:
            await self.run(
                "ALTER TABLE mentor_assignments ADD COLUMN mentee_login TEXT NOT NULL DEFAULT ''"
            )
        except Exception:
            pass  # Column already exists

    async def _populate_mentors_table(self) -> None:
        """Seed the mentors table with the initial mentor list (idempotent)."""
        for m in INITIAL_MENTORS:
            await self.run(
                """
                INSERT INTO mentors
                    (github_username, name, specialties, max_mentees, active, timezone, referred_by)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(github_username) DO NOTHING
                """,
                (
                    m.get("github_username", ""),
                    m.get("name", ""),
                    json.dumps(m.get("specialties", [])),
                    m.get("max_mentees", 3),
                    1 if m.get("active", True) else 0,
                    m.get("timezone", ""),
                    m.get("referred_by", ""),
                ),
            )

    # -------------------------------------------------------------------------
    # Mentor Operations
    # -------------------------------------------------------------------------

    async def load_mentors(self) -> list:
        """Load the mentor list from the D1 mentors table."""
        try:
            rows = await self.all(
                """
                SELECT github_username, name, specialties, max_mentees, active, timezone, referred_by
                FROM mentors
                ORDER BY name
                """
            )
            mentors = []
            for row in rows:
                specialties_raw = row.get("specialties", "[]")
                try:
                    specialties = json.loads(specialties_raw) if isinstance(specialties_raw, str) else specialties_raw
                except (json.JSONDecodeError, TypeError):
                    specialties = []
                mentors.append({
                    "github_username": row.get("github_username", ""),
                    "name": row.get("name", ""),
                    "specialties": specialties if isinstance(specialties, list) else [],
                    "max_mentees": int(row.get("max_mentees", 3)),
                    "active": bool(int(row.get("active", 1))),
                    "timezone": row.get("timezone", ""),
                    "referred_by": row.get("referred_by", ""),
                    "status": "available",
                })
            return mentors
        except Exception as exc:
            console.error(f"[D1] Failed to load mentors: {exc}")
            return []

    async def add_mentor(
        self,
        github_username: str,
        name: str,
        specialties: list,
        max_mentees: int = 3,
        active: bool = True,
        timezone: str = "",
        referred_by: str = "",
    ) -> None:
        """Insert or replace a mentor row in the D1 mentors table."""
        await self.run(
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

    async def record_mentor_assignment(
        self, org: str, mentor_login: str, repo: str, issue_number: int, mentee_login: str = ""
    ) -> None:
        """Upsert a mentor→issue assignment into D1 for load-map tracking."""
        now = int(time.time())
        try:
            await self.run(
                """
                INSERT INTO mentor_assignments (org, mentor_login, issue_repo, issue_number, assigned_at, mentee_login)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(org, issue_repo, issue_number) DO UPDATE SET
                    mentor_login = excluded.mentor_login,
                    assigned_at  = excluded.assigned_at,
                    mentee_login = excluded.mentee_login
                """,
                (org, mentor_login, repo, issue_number, now, mentee_login or ""),
            )
        except Exception as exc:
            console.error(f"[D1] Failed to record mentor assignment: {exc}")

    async def remove_mentor_assignment(self, org: str, repo: str, issue_number: int) -> None:
        """Remove a mentor assignment record from D1."""
        try:
            await self.run(
                "DELETE FROM mentor_assignments WHERE org = ? AND issue_repo = ? AND issue_number = ?",
                (org, repo, issue_number),
            )
        except Exception as exc:
            console.error(f"[D1] Failed to remove mentor assignment: {exc}")

    async def get_mentor_loads(self, org: str) -> dict:
        """Return a mapping of mentor_login → active assignment count from D1."""
        try:
            rows = await self.all(
                """
                SELECT mentor_login, COUNT(*) as cnt
                FROM mentor_assignments
                WHERE org = ?
                GROUP BY mentor_login
                """,
                (org,),
            )
            return {row["mentor_login"]: int(row.get("cnt", 0)) for row in rows}
        except Exception as exc:
            console.error(f"[D1] Failed to get mentor loads: {exc}")
            return {}

    async def get_active_assignments(self, org: str) -> list:
        """Return all active mentor assignments from D1 for the given org."""
        try:
            rows = await self.all(
                """
                SELECT org, mentor_login, mentee_login, issue_repo, issue_number, assigned_at
                FROM mentor_assignments
                WHERE org = ?
                ORDER BY assigned_at DESC
                """,
                (org,),
            )
            assignments = []
            for row in rows:
                assignments.append({
                    "org": row.get("org", ""),
                    "mentor_login": row.get("mentor_login", ""),
                    "mentee_login": row.get("mentee_login", ""),
                    "issue_repo": row.get("issue_repo", ""),
                    "issue_number": int(row.get("issue_number", 0)),
                    "assigned_at": int(row.get("assigned_at", 0)),
                })
            return assignments
        except Exception as exc:
            console.error(f"[D1] Failed to get active assignments: {exc}")
            return []

    async def get_user_comment_totals(self, org: str, logins: list) -> dict:
        """Return total all-time comment counts per user from leaderboard_monthly_stats."""
        if not logins:
            return {}
        try:
            placeholders = ", ".join(["?" for _ in logins])
            sql = f"""
                SELECT user_login, SUM(comments) as total_comments
                FROM leaderboard_monthly_stats
                WHERE org = ? AND user_login IN ({placeholders})
                GROUP BY user_login
            """
            rows = await self.all(sql, (org, *logins))
            return {row["user_login"]: int(row.get("total_comments", 0)) for row in rows}
        except Exception as exc:
            console.error(f"[D1] Failed to get user comment totals: {exc}")
            return {}

    # -------------------------------------------------------------------------
    # Leaderboard Operations
    # -------------------------------------------------------------------------

    async def inc_open_pr(self, org: str, user_login: str, delta: int) -> None:
        """Increment or decrement the open PR count for a user."""
        now = int(time.time())
        try:
            existing = await self.first(
                "SELECT open_prs FROM leaderboard_open_prs WHERE org = ? AND user_login = ?",
                (org, user_login),
            )
            if existing:
                new_count = max(0, int(existing.get("open_prs", 0)) + delta)
                await self.run(
                    "UPDATE leaderboard_open_prs SET open_prs = ?, updated_at = ? WHERE org = ? AND user_login = ?",
                    (new_count, now, org, user_login),
                )
            else:
                await self.run(
                    "INSERT INTO leaderboard_open_prs (org, user_login, open_prs, updated_at) VALUES (?, ?, ?, ?)",
                    (org, user_login, max(0, delta), now),
                )
        except Exception as e:
            console.error(f"[D1] Failed to inc_open_pr: {e}")

    async def inc_monthly(self, org: str, month_key: str, user_login: str, field: str, delta: int = 1) -> None:
        """Increment a monthly stat field for a user."""
        now = int(time.time())
        if field not in {"merged_prs", "closed_prs", "reviews", "comments"}:
            raise ValueError(f"Invalid field: {field}")
        try:
            existing = await self.first(
                f"SELECT {field} FROM leaderboard_monthly_stats WHERE org = ? AND month_key = ? AND user_login = ?",
                (org, month_key, user_login),
            )
            if existing:
                new_val = int(existing.get(field, 0)) + delta
                await self.run(
                    f"UPDATE leaderboard_monthly_stats SET {field} = ?, updated_at = ? WHERE org = ? AND month_key = ? AND user_login = ?",
                    (new_val, now, org, month_key, user_login),
                )
            else:
                await self.run(
                    f"INSERT INTO leaderboard_monthly_stats (org, month_key, user_login, {field}, updated_at) VALUES (?, ?, ?, ?, ?)",
                    (org, month_key, user_login, delta, now),
                )
        except Exception as e:
            console.error(f"[D1] Failed to inc_monthly: {e}")

    async def get_monthly_stats(self, org: str, month_key: str) -> list:
        """Get all monthly stats for a given org and month."""
        return await self.all(
            """
            SELECT user_login, merged_prs, closed_prs, reviews, comments
            FROM leaderboard_monthly_stats
            WHERE org = ? AND month_key = ?
            """,
            (org, month_key),
        )

    async def get_open_pr_stats(self, org: str) -> list:
        """Get all open PR stats for a given org."""
        return await self.all(
            """
            SELECT user_login, open_prs
            FROM leaderboard_open_prs
            WHERE org = ?
            """,
            (org,),
        )

    async def get_backfill_state(self, org: str, month_key: str) -> dict:
        """Get backfill state for a given org and month."""
        row = await self.first(
            """
            SELECT next_page, completed FROM leaderboard_backfill_state
            WHERE org = ? AND month_key = ?
            """,
            (org, month_key),
        )
        if row:
            return {
                "next_page": int(row.get("next_page", 1)),
                "completed": bool(int(row.get("completed", 0))),
            }
        return {"next_page": 1, "completed": False}

    async def set_backfill_state(self, org: str, month_key: str, next_page: int, completed: bool) -> None:
        """Set backfill state for a given org and month."""
        try:
            now = int(time.time())
            await self.run(
                """
                INSERT INTO leaderboard_backfill_state (org, month_key, next_page, completed, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(org, month_key) DO UPDATE SET
                    next_page = excluded.next_page,
                    completed = excluded.completed,
                    updated_at = excluded.updated_at
                """,
                (org, month_key, next_page, 1 if completed else 0, now),
            )
        except Exception as e:
            console.error(f"[D1] Failed to set_backfill_state: {e}")

    async def is_repo_backfilled(self, org: str, month_key: str, repo: str) -> bool:
        """Check if a repo has been backfilled for a given month."""
        row = await self.first(
            """
            SELECT 1 FROM leaderboard_backfill_repo_done
            WHERE org = ? AND month_key = ? AND repo = ?
            """,
            (org, month_key, repo),
        )
        return bool(row)

    async def mark_repo_backfilled(self, org: str, month_key: str, repo: str) -> None:
        """Mark a repo as backfilled for a given month."""
        try:
            now = int(time.time())
            await self.run(
                """
                INSERT INTO leaderboard_backfill_repo_done (org, month_key, repo, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(org, month_key, repo) DO UPDATE SET updated_at = excluded.updated_at
                """,
                (org, month_key, repo, now),
            )
        except Exception as e:
            console.error(f"[D1] Failed to mark_repo_backfilled: {e}")

    async def get_pr_state(self, org: str, repo: str, pr_number: int) -> Optional[dict]:
        """Get the state of a PR from D1."""
        return await self.first(
            "SELECT state, merged, closed_at FROM leaderboard_pr_state WHERE org = ? AND repo = ? AND pr_number = ?",
            (org, repo, pr_number),
        )

    async def upsert_pr_state(
        self, org: str, repo: str, pr_number: int, author_login: str, state: str, merged: bool, closed_at: Optional[int]
    ) -> None:
        """Upsert a PR state record."""
        now = int(time.time())
        await self.run(
            """
            INSERT INTO leaderboard_pr_state (org, repo, pr_number, author_login, state, merged, closed_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(org, repo, pr_number) DO UPDATE SET
                author_login = excluded.author_login,
                state = excluded.state,
                merged = excluded.merged,
                closed_at = excluded.closed_at,
                updated_at = excluded.updated_at
            """,
            (org, repo, pr_number, author_login, state, 1 if merged else 0, closed_at, now),
        )

    async def check_review_credit_exists(
        self, org: str, repo: str, pr_number: int, month_key: str, reviewer_login: str
    ) -> bool:
        """Check if a review credit already exists."""
        row = await self.first(
            """
            SELECT 1 FROM leaderboard_review_credits
            WHERE org = ? AND repo = ? AND pr_number = ? AND month_key = ? AND reviewer_login = ?
            """,
            (org, repo, pr_number, month_key, reviewer_login),
        )
        return bool(row)

    async def count_review_credits(self, org: str, repo: str, pr_number: int, month_key: str) -> int:
        """Count the number of review credits for a PR/month."""
        row = await self.first(
            """
            SELECT COUNT(*) AS cnt FROM leaderboard_review_credits
            WHERE org = ? AND repo = ? AND pr_number = ? AND month_key = ?
            """,
            (org, repo, pr_number, month_key),
        )
        return int((row or {}).get("cnt", 0))

    async def add_review_credit(
        self, org: str, repo: str, pr_number: int, month_key: str, reviewer_login: str
    ) -> None:
        """Add a review credit record."""
        await self.run(
            """
            INSERT INTO leaderboard_review_credits (org, repo, pr_number, month_key, reviewer_login, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (org, repo, pr_number, month_key, reviewer_login, int(time.time())),
        )

    async def get_tracked_pr_numbers(self, org: str, repo: str) -> dict:
        """Get all tracked PR numbers and their states for a repo."""
        rows = await self.all(
            "SELECT pr_number, state FROM leaderboard_pr_state WHERE org = ? AND repo = ?",
            (org, repo),
        )
        return {int(row["pr_number"]): row.get("state", "") for row in rows}

    async def reset_leaderboard_month(self, org: str, month_key: str) -> dict:
        """Clear all leaderboard data for an org/month for re-backfill."""
        from services.libs import TimeUtils
        
        await self.ensure_schema()
        deleted: dict = {}

        for table, params in [
            ("leaderboard_monthly_stats", (org, month_key)),
            ("leaderboard_backfill_repo_done", (org, month_key)),
            ("leaderboard_review_credits", (org, month_key)),
            ("leaderboard_backfill_state", (org, month_key)),
        ]:
            try:
                await self.run(f"DELETE FROM {table} WHERE org = ? AND month_key = ?", params)
                deleted[table] = "cleared"
            except Exception as e:
                deleted[table] = f"error: {e}"
                console.error(f"[AdminReset] Failed to clear {table}: {e}")

        # Scope the pr_state delete to the target month's timestamp window
        start_ts, end_ts = TimeUtils.month_window(month_key)
        try:
            await self.run(
                """
                DELETE FROM leaderboard_pr_state
                WHERE org = ? AND closed_at >= ? AND closed_at <= ?
                """,
                (org, start_ts, end_ts),
            )
            await self.run(
                """
                DELETE FROM leaderboard_pr_state
                WHERE org = ? AND closed_at IS NULL
                """,
                (org,),
            )
            deleted["leaderboard_pr_state"] = "cleared (scoped to month window)"
        except Exception as e:
            deleted["leaderboard_pr_state"] = f"error: {e}"
            console.error(f"[AdminReset] Failed to clear leaderboard_pr_state: {e}")

        try:
            await self.run("DELETE FROM leaderboard_open_prs WHERE org = ?", (org,))
            deleted["leaderboard_open_prs"] = "cleared"
        except Exception as e:
            deleted["leaderboard_open_prs"] = f"error: {e}"
            console.error(f"[AdminReset] Failed to clear leaderboard_open_prs: {e}")

        console.log(f"[AdminReset] Cleared leaderboard data for org={org} month={month_key}")
        return deleted
