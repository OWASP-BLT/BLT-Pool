"""models/assignment.py — Mentor-assignment entity for BLT-Pool.

D1 CRUD operations for the ``mentor_assignments`` table:
- Record / update an assignment
- Remove an assignment (on handoff or issue close)
- Query loads and active assignments
"""

import time
from typing import Optional

from js import console

from core.db import _d1_all, _d1_first, _d1_run


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
