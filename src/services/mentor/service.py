"""Mentor service for BLT-Pool mentor matching and assignment operations."""

import json
import random
import time
from typing import Optional

try:
    from js import console
except ImportError:
    # Fallback for testing outside Cloudflare Workers
    class _MockConsole:
        def log(self, *args, **kwargs): pass
        def error(self, *args, **kwargs): pass
    console = _MockConsole()

from services.database import DatabaseService
from services.libs import TimeUtils, Validators


class MentorService:
    """Manage mentor pool, assignments, and matching logic."""

    # Mentor pool constants
    MENTOR_COMMAND = "/mentor"
    UNMENTOR_COMMAND = "/unmentor"
    MENTOR_PAUSE_COMMAND = "/mentor-pause"
    HANDOFF_COMMAND = "/handoff"
    REMATCH_COMMAND = "/rematch"
    NEEDS_MENTOR_LABEL = "needs-mentor"
    MENTOR_ASSIGNED_LABEL = "mentor-assigned"
    MENTOR_MAX_MENTEES = 3
    MENTOR_STALE_DAYS = 14
    MENTOR_LABEL_COLOR = "7057ff"
    MENTOR_ASSIGNED_LABEL_COLOR = "0075ca"
    MENTOR_AUTO_PR_REVIEWER_ENABLED = False
    SECURITY_BYPASS_LABELS = {"security", "vulnerability", "security-sensitive", "private-security"}

    def __init__(self, github_service):
        """Initialize MentorService.
        
        Args:
            github_service: GitHubService instance for API calls
        """
        self.github = github_service

    # -------------------------------------------------------------------------
    # Mentor Loading & Configuration
    # -------------------------------------------------------------------------

    async def load_mentors(self, env=None, owner: str = "", repo: str = "", token: str = "") -> list:
        """Load mentor list from D1 or fallback to local seed data."""
        db_binding = DatabaseService.get_binding(env)
        if db_binding:
            db = DatabaseService(db_binding)
            await db.ensure_schema()
            mentors = await db.load_mentors()
            if mentors:
                return mentors

        # Fallback to seed data
        from services.mentor_seed import INITIAL_MENTORS
        return INITIAL_MENTORS

    async def fetch_mentor_stats(self, env, org: str) -> dict:
        """Fetch mentor statistics from D1 (merged PRs and reviews)."""
        db_binding = DatabaseService.get_binding(env)
        if not db_binding:
            return {}
        db = DatabaseService(db_binding)

        try:
            await db.ensure_schema()
            mk = TimeUtils.month_key()
            rows = await db.all(
                """
                SELECT user_login, SUM(merged_prs) AS merged_prs, SUM(reviews) AS reviews
                FROM leaderboard_monthly_stats
                WHERE org = ?
                GROUP BY user_login
                """,
                (org,),
            )
            stats = {}
            for row in rows:
                login = row.get("user_login", "").lower()
                stats[login] = {
                    "merged_prs": int(row.get("merged_prs", 0)),
                    "reviews": int(row.get("reviews", 0)),
                }
            return stats
        except Exception as exc:
            console.error(f"[MentorStats] Failed to fetch stats: {exc}")
            return {}

    async def get_mentor_load_map(self, owner: str, token: str, env=None) -> dict:
        """Return a mapping of mentor_login → active assignment count."""
        db_binding = DatabaseService.get_binding(env)
        if db_binding:
            db = DatabaseService(db_binding)
            await db.ensure_schema()
            loads = await db.get_mentor_loads(owner)
            if loads:
                return loads

        # Fallback: scan issues with needs-mentor or mentor-assigned labels
        console.log("[MentorLoad] D1 not available, scanning issues for mentor assignments")
        loads = {}
        try:
            # Implementation would scan issues here but we'll rely on D1 primarily
            pass
        except Exception as exc:
            console.error(f"[MentorLoad] Failed to scan issues: {exc}")
        return loads

    # -------------------------------------------------------------------------
    # Mentor Selection & Matching
    # -------------------------------------------------------------------------

    async def select_mentor(
        self,
        owner: str,
        token: str,
        issue_labels: Optional[list] = None,
        mentors_config: Optional[list] = None,
        exclude: Optional[str] = None,
        env=None,
    ) -> Optional[dict]:
        """Select the best available mentor for an issue using round-robin + load balancing."""
        if mentors_config is None:
            mentors_config = await self.load_mentors(env, owner, "", token)

        active = [m for m in mentors_config if m.get("active", True) and m.get("status") != "inactive"]
        if not active:
            console.log("[MentorSelect] No active mentors available")
            return None

        # Filter by specialty if issue has relevant labels
        if issue_labels:
            label_set = {lbl.lower() for lbl in issue_labels}
            specialty_matches = []
            for m in active:
                m_specs = {s.lower() for s in m.get("specialties", [])}
                if m_specs & label_set:
                    specialty_matches.append(m)
            if specialty_matches:
                active = specialty_matches

        # Get current loads
        load_map = await self.get_mentor_load_map(owner, token, env)

        # Filter out mentors who are at capacity or excluded
        available = []
        for m in active:
            login = m.get("github_username", "")
            if exclude and login.lower() == exclude.lower():
                continue
            current_load = load_map.get(login, 0)
            max_mentees = m.get("max_mentees", self.MENTOR_MAX_MENTEES)
            if current_load < max_mentees:
                available.append((m, current_load))

        if not available:
            console.log("[MentorSelect] No mentors available (all at capacity)")
            return None

        # Sort by load (ascending) then randomize within same load level
        available.sort(key=lambda x: x[1])
        min_load = available[0][1]
        least_loaded = [m for m, load in available if load == min_load]
        
        # Randomize selection among least loaded mentors
        return random.choice(least_loaded)

    async def find_assigned_mentor_from_comments(
        self, owner: str, repo: str, issue_number: int, token: str
    ) -> Optional[str]:
        """Find the mentor assigned to an issue by scanning comments."""
        try:
            resp = await self.github.github_api(
                "GET",
                f"/repos/{owner}/{repo}/issues/{issue_number}/comments",
                token,
            )
            if resp.status != 200:
                return None
            comments = json.loads(await resp.text())
            for comment in comments:
                body = comment.get("body", "")
                if "has been assigned as your mentor" in body:
                    # Extract mentor username from comment
                    for line in body.split("\n"):
                        if "@" in line and "has been assigned" in line:
                            parts = line.split("@")
                            if len(parts) > 1:
                                mentor_login = parts[1].split()[0].rstrip(".,!?:;)")
                                return mentor_login
            return None
        except Exception as exc:
            console.error(f"[MentorFind] Failed to find mentor from comments: {exc}")
            return None

    def is_security_issue(self, issue: dict) -> bool:
        """Return True if the issue has security-related labels."""
        labels = issue.get("labels", [])
        label_names = {lbl.get("name", "").lower() for lbl in labels}
        return bool(label_names & self.SECURITY_BYPASS_LABELS)

    # -------------------------------------------------------------------------
    # Assignment Operations
    # -------------------------------------------------------------------------

    async def assign_mentor_to_issue(
        self,
        owner: str,
        repo: str,
        issue: dict,
        contributor_login: str,
        token: str,
        mentors_config: Optional[list] = None,
        exclude: Optional[str] = None,
        env=None,
    ) -> bool:
        """Assign a mentor to an issue and post assignment comment. Returns True if assigned."""
        if self.is_security_issue(issue):
            console.log(f"[MentorAssign] Issue #{issue.get('number')} is security-sensitive, skipping auto-assignment")
            return False

        issue_number = issue.get("number")
        labels = issue.get("labels", [])
        label_names = [lbl.get("name", "") for lbl in labels]

        # Ensure labels exist
        await self._ensure_label_exists(owner, repo, self.NEEDS_MENTOR_LABEL, self.MENTOR_LABEL_COLOR, token)
        await self._ensure_label_exists(owner, repo, self.MENTOR_ASSIGNED_LABEL, self.MENTOR_ASSIGNED_LABEL_COLOR, token)

        # Select mentor
        mentor = await self.select_mentor(owner, token, label_names, mentors_config, exclude, env)
        if not mentor:
            console.log(f"[MentorAssign] No available mentor for issue #{issue_number}")
            return False

        mentor_login = mentor.get("github_username", "")
        mentor_name = mentor.get("name", mentor_login)

        # Post assignment comment
        assignment_msg = (
            f"👋 Hey @{contributor_login}!\n\n"
            f"**@{mentor_login}** ({mentor_name}) has been assigned as your mentor for this issue.\n\n"
            f"Feel free to reach out if you have questions or need guidance. Happy coding! 🚀"
        )
        await self.github.create_comment(owner, repo, issue_number, assignment_msg, token)

        # Update labels: remove needs-mentor, add mentor-assigned
        if self.NEEDS_MENTOR_LABEL in label_names:
            await self.github.github_api(
                "DELETE",
                f"/repos/{owner}/{repo}/issues/{issue_number}/labels/{self.NEEDS_MENTOR_LABEL}",
                token,
            )
        await self.github.github_api(
            "POST",
            f"/repos/{owner}/{repo}/issues/{issue_number}/labels",
            token,
            {"labels": [self.MENTOR_ASSIGNED_LABEL]},
        )

        # Record assignment in D1
        db_binding = DatabaseService.get_binding(env)
        if db_binding:
            db = DatabaseService(db_binding)
            await db.ensure_schema()
            await db.record_mentor_assignment(owner, mentor_login, repo, issue_number, contributor_login)

        console.log(f"[MentorAssign] Assigned {mentor_login} to {owner}/{repo}#{issue_number}")
        return True

    async def _ensure_label_exists(self, owner: str, repo: str, name: str, color: str, token: str) -> None:
        """Ensure a GitHub label exists in a repository."""
        resp = await self.github.github_api("GET", f"/repos/{owner}/{repo}/labels/{name}", token)
        if resp.status == 404:
            await self.github.github_api(
                "POST",
                f"/repos/{owner}/{repo}/labels",
                token,
                {"name": name, "color": color, "description": ""},
            )

    # -------------------------------------------------------------------------
    # Command Handlers
    # -------------------------------------------------------------------------

    async def handle_mentor_command(
        self,
        owner: str,
        repo: str,
        issue: dict,
        login: str,
        token: str,
        mentors_config: Optional[list] = None,
        env=None,
    ) -> None:
        """Handle /mentor command to request mentor assignment."""
        issue_number = issue.get("number")
        issue_author = (issue.get("user") or {}).get("login", "")

        if login.lower() != issue_author.lower():
            await self.github.create_comment(
                owner, repo, issue_number,
                "⚠️ Only the issue author can request a mentor assignment.",
                token
            )
            return

        assigned = await self.assign_mentor_to_issue(
            owner, repo, issue, issue_author, token, mentors_config, None, env
        )
        if not assigned:
            await self.github.create_comment(
                owner, repo, issue_number,
                "⚠️ All mentors are currently at capacity. Please try again later or reach out to the maintainers.",
                token
            )

    async def handle_mentor_unassign(
        self,
        owner: str,
        repo: str,
        issue: dict,
        login: str,
        token: str,
        env=None,
    ) -> None:
        """Handle /unmentor command to remove mentor assignment."""
        issue_number = issue.get("number")
        is_maintainer = await self.github.is_maintainer(owner, repo, login, token)
        
        if not is_maintainer:
            await self.github.create_comment(
                owner, repo, issue_number,
                "⚠️ Only maintainers can unassign mentors.",
                token
            )
            return

        # Find current mentor
        mentor_login = await self.find_assigned_mentor_from_comments(owner, repo, issue_number, token)
        
        # Remove label
        await self.github.github_api(
            "DELETE",
            f"/repos/{owner}/{repo}/issues/{issue_number}/labels/{self.MENTOR_ASSIGNED_LABEL}",
            token,
        )

        # Remove from D1
        db_binding = DatabaseService.get_binding(env)
        if db_binding:
            db = DatabaseService(db_binding)
            await db.ensure_schema()
            await db.remove_mentor_assignment(owner, repo, issue_number)

        msg = f"✅ Mentor assignment removed"
        if mentor_login:
            msg += f" (@{mentor_login})"
        await self.github.create_comment(owner, repo, issue_number, msg, token)

    async def handle_mentor_pause(
        self,
        owner: str,
        repo: str,
        issue: dict,
        login: str,
        token: str,
        mentors_config: Optional[list] = None,
        env=None,
    ) -> None:
        """Handle /mentor-pause command to pause mentor assignment."""
        issue_number = issue.get("number")
        issue_author = (issue.get("user") or {}).get("login", "")

        if login.lower() != issue_author.lower():
            is_maintainer = await self.github.is_maintainer(owner, repo, login, token)
            if not is_maintainer:
                await self.github.create_comment(
                    owner, repo, issue_number,
                    "⚠️ Only the issue author or maintainers can pause mentor matching.",
                    token
                )
                return

        # Remove needs-mentor label if present
        await self.github.github_api(
            "DELETE",
            f"/repos/{owner}/{repo}/issues/{issue_number}/labels/{self.NEEDS_MENTOR_LABEL}",
            token,
        )

        await self.github.create_comment(
            owner, repo, issue_number,
            "⏸️ Mentor matching paused for this issue. Use `/mentor` to resume.",
            token
        )

    async def handle_mentor_handoff(
        self,
        owner: str,
        repo: str,
        issue: dict,
        login: str,
        token: str,
        mentors_config: Optional[list] = None,
        env=None,
    ) -> None:
        """Handle /handoff command to reassign a different mentor."""
        issue_number = issue.get("number")
        is_maintainer = await self.github.is_maintainer(owner, repo, login, token)
        
        # Find current mentor
        current_mentor = await self.find_assigned_mentor_from_comments(owner, repo, issue_number, token)
        
        if not is_maintainer and (not current_mentor or login.lower() != current_mentor.lower()):
            await self.github.create_comment(
                owner, repo, issue_number,
                "⚠️ Only the assigned mentor or maintainers can hand off this issue.",
                token
            )
            return

        # Remove current assignment
        db_binding = DatabaseService.get_binding(env)
        if db_binding:
            db = DatabaseService(db_binding)
            await db.ensure_schema()
            await db.remove_mentor_assignment(owner, repo, issue_number)

        # Remove label
        await self.github.github_api(
            "DELETE",
            f"/repos/{owner}/{repo}/issues/{issue_number}/labels/{self.MENTOR_ASSIGNED_LABEL}",
            token,
        )

        # Assign new mentor (exclude current one)
        issue_author = (issue.get("user") or {}).get("login", "")
        assigned = await self.assign_mentor_to_issue(
            owner, repo, issue, issue_author, token, mentors_config, current_mentor, env
        )
        
        if not assigned:
            await self.github.create_comment(
                owner, repo, issue_number,
                "⚠️ Unable to find another available mentor. All mentors may be at capacity.",
                token
            )

    async def handle_mentor_rematch(
        self,
        owner: str,
        repo: str,
        issue: dict,
        login: str,
        token: str,
        mentors_config: Optional[list] = None,
        env=None,
    ) -> None:
        """Handle /rematch command to get a different mentor match."""
        issue_number = issue.get("number")
        issue_author = (issue.get("user") or {}).get("login", "")

        if login.lower() != issue_author.lower():
            is_maintainer = await self.github.is_maintainer(owner, repo, login, token)
            if not is_maintainer:
                await self.github.create_comment(
                    owner, repo, issue_number,
                    "⚠️ Only the issue author or maintainers can request a rematch.",
                    token
                )
                return

        # Find current mentor
        current_mentor = await self.find_assigned_mentor_from_comments(owner, repo, issue_number, token)

        # Remove current assignment
        db_binding = DatabaseService.get_binding(env)
        if db_binding:
            db = DatabaseService(db_binding)
            await db.ensure_schema()
            await db.remove_mentor_assignment(owner, repo, issue_number)

        # Remove label
        await self.github.github_api(
            "DELETE",
            f"/repos/{owner}/{repo}/issues/{issue_number}/labels/{self.MENTOR_ASSIGNED_LABEL}",
            token,
        )

        # Assign new mentor (exclude current one)
        assigned = await self.assign_mentor_to_issue(
            owner, repo, issue, issue_author, token, mentors_config, current_mentor, env
        )
        
        if not assigned:
            await self.github.create_comment(
                owner, repo, issue_number,
                "⚠️ Unable to find another available mentor. All mentors may be at capacity.",
                token
            )

    # -------------------------------------------------------------------------
    # Stale Assignment Checking
    # -------------------------------------------------------------------------

    async def check_stale_assignments(self, owner: str, repo: str, token: str, env=None) -> None:
        """Check for stale mentor assignments and post reminders."""
        db_binding = DatabaseService.get_binding(env)
        if not db_binding:
            return
        
        db = DatabaseService(db_binding)
        await db.ensure_schema()
        
        assignments = await db.get_active_assignments(owner)
        stale_threshold = int(time.time()) - (self.MENTOR_STALE_DAYS * 86400)
        
        for assignment in assignments:
            if assignment.get("assigned_at", 0) < stale_threshold:
                issue_repo = assignment.get("issue_repo", "")
                issue_number = assignment.get("issue_number", 0)
                mentor_login = assignment.get("mentor_login", "")
                
                if issue_repo == repo:
                    # Post reminder comment
                    await self.github.create_comment(
                        owner, repo, issue_number,
                        f"👋 @{mentor_login} — This issue has been assigned to you for {self.MENTOR_STALE_DAYS}+ days. "
                        f"Please check in or use `/handoff` if you need to reassign.",
                        token
                    )
