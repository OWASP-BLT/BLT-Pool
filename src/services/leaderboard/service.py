"""Leaderboard service for BLT-Pool statistics and rankings."""

import json
import time
from typing import Optional, Tuple

try:
    from js import console, fetch
except ImportError:
    # Fallback for testing outside Cloudflare Workers
    class _MockConsole:
        def log(self, *args, **kwargs): pass
        def error(self, *args, **kwargs): pass
    console = _MockConsole()
    fetch = None

from services.database import DatabaseService
from services.libs import TimeUtils


class LeaderboardService:
    """Manage leaderboard calculations, tracking, and backfill operations."""

    # Leaderboard configuration constants
    LEADERBOARD_MARKER = "<!-- leaderboard-bot -->"
    REVIEWER_LEADERBOARD_MARKER = "<!-- reviewer-leaderboard-bot -->"
    MERGED_PR_COMMENT_MARKER = "<!-- merged-pr-comment-bot -->"
    MAX_OPEN_PRS_PER_AUTHOR = 50
    LEADERBOARD_COMMENT_MARKER = LEADERBOARD_MARKER

    def __init__(self, github_service, rendering_service):
        """Initialize LeaderboardService.
        
        Args:
            github_service: GitHubService instance for API calls
            rendering_service: RenderingService instance for response formatting
        """
        self.github = github_service
        self.renderer = rendering_service

    # -------------------------------------------------------------------------
    # Tracking Events
    # -------------------------------------------------------------------------

    async def track_pr_opened(self, payload: dict, env) -> None:
        """Track a newly opened PR in D1."""
        db_binding = DatabaseService.get_binding(env)
        if not db_binding:
            return
        db = DatabaseService(db_binding)

        pr = payload.get("pull_request") or {}
        author = pr.get("user") or {}
        if self.github.is_bot(author):
            return

        org = (payload.get("repository") or {}).get("owner", {}).get("login", "")
        repo = (payload.get("repository") or {}).get("name", "")
        pr_number = pr.get("number")
        author_login = author.get("login", "")
        if not (org and repo and pr_number and author_login):
            return

        await db.ensure_schema()
        existing = await db.get_pr_state(org, repo, pr_number)
        if not existing or existing.get("state") != "open":
            await db.inc_open_pr(org, author_login, 1)

        await db.upsert_pr_state(org, repo, pr_number, author_login, "open", False, None)

    async def track_pr_closed(self, payload: dict, env) -> None:
        """Track a closed/merged PR in D1."""
        db_binding = DatabaseService.get_binding(env)
        if not db_binding:
            return
        db = DatabaseService(db_binding)

        pr = payload.get("pull_request") or {}
        author = pr.get("user") or {}
        if self.github.is_bot(author):
            return

        org = (payload.get("repository") or {}).get("owner", {}).get("login", "")
        repo = (payload.get("repository") or {}).get("name", "")
        pr_number = pr.get("number")
        author_login = author.get("login", "")
        closed_at = pr.get("closed_at")
        merged_at = pr.get("merged_at")
        merged = bool(pr.get("merged"))
        closed_ts = TimeUtils.parse_github_timestamp(closed_at) if closed_at else int(time.time())
        if not (org and repo and pr_number and author_login):
            return

        await db.ensure_schema()
        existing = await db.get_pr_state(org, repo, pr_number)

        # Idempotency: skip if we already recorded the same closed state
        if existing and existing.get("state") == "closed" and int(existing.get("merged") or 0) == int(merged):
            console.log(
                f"[D1] PR {org}/{repo}#{pr_number} already tracked as closed (merged={merged}), skipping"
            )
            return

        if existing and existing.get("state") == "open":
            await db.inc_open_pr(org, author_login, -1)

        event_ts = TimeUtils.parse_github_timestamp(merged_at) if merged and merged_at else closed_ts
        mk = TimeUtils.month_key(event_ts)
        if merged:
            await db.inc_monthly(org, mk, author_login, "merged_prs", 1)
        else:
            await db.inc_monthly(org, mk, author_login, "closed_prs", 1)

        await db.upsert_pr_state(org, repo, pr_number, author_login, "closed", merged, closed_ts)

    async def track_comment(self, payload: dict, env) -> None:
        """Track a comment in D1."""
        db_binding = DatabaseService.get_binding(env)
        if not db_binding:
            return
        db = DatabaseService(db_binding)

        comment = payload.get("comment") or {}
        user = comment.get("user") or {}
        if self.github.is_bot(user):
            return

        body = comment.get("body", "")
        if self.github.is_coderabbit_ping(body):
            return

        org = (payload.get("repository") or {}).get("owner", {}).get("login", "")
        login = user.get("login", "")
        created_at = comment.get("created_at")
        if not (org and login):
            return

        await db.ensure_schema()
        mk = TimeUtils.month_key(
            TimeUtils.parse_github_timestamp(created_at) if created_at else int(time.time())
        )
        await db.inc_monthly(org, mk, login, "comments", 1)

    async def track_review(self, payload: dict, env) -> None:
        """Track a review in D1."""
        db_binding = DatabaseService.get_binding(env)
        if not db_binding:
            console.log("[D1] REVIEW: No DB binding")
            return
        db = DatabaseService(db_binding)

        review = payload.get("review") or {}
        reviewer = review.get("user") or {}
        if self.github.is_bot(reviewer):
            console.log(f"[D1] REVIEW: Skipping bot reviewer {reviewer.get('login')}")
            return

        pr = payload.get("pull_request") or {}
        org = (payload.get("repository") or {}).get("owner", {}).get("login", "")
        repo = (payload.get("repository") or {}).get("name", "")
        pr_number = pr.get("number")
        reviewer_login = reviewer.get("login", "")
        submitted_at = review.get("submitted_at")
        if not (org and repo and pr_number and reviewer_login):
            console.log(f"[D1] REVIEW: Missing required fields")
            return

        console.log(f"[D1] REVIEW: Processing {reviewer_login} reviewing {org}/{repo}#{pr_number}")

        await db.ensure_schema()
        mk = TimeUtils.month_key(
            TimeUtils.parse_github_timestamp(submitted_at) if submitted_at else int(time.time())
        )

        # Only first two unique reviewers per PR/month get credit
        exists = await db.check_review_credit_exists(org, repo, pr_number, mk, reviewer_login)
        if exists:
            console.log(f"[D1] REVIEW: Credit already exists for {reviewer_login}")
            return

        already = await db.count_review_credits(org, repo, pr_number, mk)
        if already >= 2:
            console.log(f"[D1] REVIEW: Max 2 reviewers already credited")
            return

        await db.add_review_credit(org, repo, pr_number, mk, reviewer_login)
        await db.inc_monthly(org, mk, reviewer_login, "reviews", 1)

    # -------------------------------------------------------------------------
    # Statistics & Leaderboard Calculation
    # -------------------------------------------------------------------------

    async def calculate_stats_from_d1(self, owner: str, env) -> Optional[dict]:
        """Read current-month leaderboard stats from D1 if configured."""
        db_binding = DatabaseService.get_binding(env)
        if not db_binding:
            console.log("[Leaderboard] D1 not configured, skipping stats calculation")
            return None

        db = DatabaseService(db_binding)
        await db.ensure_schema()
        mk = TimeUtils.month_key()
        start_timestamp, end_timestamp = TimeUtils.month_window(mk)

        monthly_rows = await db.get_monthly_stats(owner, mk)
        open_rows = await db.get_open_pr_stats(owner)

        console.log(
            f"[D1] Queried org={owner} mk={mk}: {len(monthly_rows or [])} monthly, {len(open_rows or [])} open"
        )
        if not monthly_rows and not open_rows:
            return None

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
            login = row.get("user_login", "")
            if not login:
                continue
            ensure(login)
            user_stats[login]["mergedPrs"] += int(row.get("merged_prs", 0))
            user_stats[login]["closedPrs"] += int(row.get("closed_prs", 0))
            user_stats[login]["reviews"] += int(row.get("reviews", 0))
            user_stats[login]["comments"] += int(row.get("comments", 0))

        for row in open_rows:
            login = row.get("user_login", "")
            if not login:
                continue
            ensure(login)
            user_stats[login]["openPrs"] = int(row.get("open_prs", 0))

        for login in user_stats:
            stats = user_stats[login]
            stats["total"] = (
                stats["mergedPrs"] * 10
                + stats["reviews"] * 5
                + stats["comments"] * 2
                + stats["openPrs"] * 1
                - stats["closedPrs"] * 2
            )

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

    async def calculate_stats_from_github(self, owner: str, repos: list, token: str, window_months: int = 1) -> dict:
        """Calculate leaderboard stats using GitHub Search API (fallback when D1 unavailable)."""
        now_seconds = int(time.time())
        now = time.gmtime(now_seconds)

        start_of_month = time.struct_time((now.tm_year, now.tm_mon, 1, 0, 0, 0, 0, 0, 0))
        start_timestamp = int(time.mktime(start_of_month))

        if now.tm_mon == 12:
            end_year, end_month = now.tm_year + 1, 1
        else:
            end_year, end_month = now.tm_year, now.tm_mon + 1
        end_of_month = time.struct_time((end_year, end_month, 1, 0, 0, 0, 0, 0, 0))
        end_timestamp = int(time.mktime(end_of_month)) - 1

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
                    "total": 0,
                }

        # Count open PRs (current state across all repos)
        page = 1
        while page <= 3:
            resp = await self.github.github_api(
                "GET",
                f"/search/issues?q=org:{owner}+type:pr+is:open&per_page=100&page={page}",
                token,
            )
            if resp.status != 200:
                break
            data = json.loads(await resp.text())
            items = data.get("items", [])
            for pr in items:
                author = pr.get("user") or {}
                author_login = author.get("login", "")
                if author_login and self.github.is_human(author):
                    ensure_user(author_login)
                    user_stats[author_login]["openPrs"] += 1
            if len(items) < 100:
                break
            page += 1

        # Fetch merged PRs from this month
        page = 1
        while page <= 3:
            resp = await self.github.github_api(
                "GET",
                f"/search/issues?q=org:{owner}+type:pr+is:merged+merged:{start_date}..{end_date}&per_page=100&page={page}",
                token,
            )
            if resp.status != 200:
                break
            data = json.loads(await resp.text())
            items = data.get("items", [])
            for pr in items:
                author = pr.get("user") or {}
                author_login = author.get("login", "")
                if author_login and self.github.is_human(author):
                    ensure_user(author_login)
                    user_stats[author_login]["mergedPrs"] += 1
            if len(items) < 100:
                break
            page += 1

        # Fetch closed (not merged) PRs from this month
        page = 1
        while page <= 3:
            resp = await self.github.github_api(
                "GET",
                f"/search/issues?q=org:{owner}+type:pr+is:unmerged+is:closed+closed:{start_date}..{end_date}&per_page=100&page={page}",
                token,
            )
            if resp.status != 200:
                break
            data = json.loads(await resp.text())
            items = data.get("items", [])
            for pr in items:
                author = pr.get("user") or {}
                author_login = author.get("login", "")
                if author_login and self.github.is_human(author):
                    ensure_user(author_login)
                    user_stats[author_login]["closedPrs"] += 1
            if len(items) < 100:
                break
            page += 1

        # Calculate total scores
        for login in user_stats:
            stats = user_stats[login]
            stats["total"] = (
                stats["mergedPrs"] * 10
                + stats["reviews"] * 5
                + stats["comments"] * 2
                + stats["openPrs"] * 1
                - stats["closedPrs"] * 2
            )

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

    # -------------------------------------------------------------------------
    # Leaderboard Formatting
    # -------------------------------------------------------------------------

    def format_leaderboard_comment(self, author_login: str, leaderboard_data: dict, owner: str, note: str = "") -> str:
        """Format a leaderboard comment for posting on issues/PRs."""
        sorted_users = leaderboard_data.get("sorted", [])
        if not sorted_users:
            return (
                f"## {self.LEADERBOARD_MARKER}\n\n"
                f"### 📊 Monthly Leaderboard\n\nNo activity data available yet for this month.\n"
            )

        start_ts = leaderboard_data.get("start_timestamp", 0)
        end_ts = leaderboard_data.get("end_timestamp", 0)
        start_date = time.strftime("%b %d", time.gmtime(start_ts))
        end_date = time.strftime("%b %d, %Y", time.gmtime(end_ts))

        table_header = (
            "| Rank | Contributor | Merged | Reviews | Comments | Open | Closed | Total |\n"
            "|------|-------------|--------|---------|----------|------|--------|-------|\n"
        )
        rows = []
        author_rank = None
        for idx, user in enumerate(sorted_users, start=1):
            login = user.get("login", "")
            merged = user.get("mergedPrs", 0)
            reviews = user.get("reviews", 0)
            comments = user.get("comments", 0)
            open_prs = user.get("openPrs", 0)
            closed = user.get("closedPrs", 0)
            total = user.get("total", 0)

            emoji = ""
            if idx == 1:
                emoji = "🥇 "
            elif idx == 2:
                emoji = "🥈 "
            elif idx == 3:
                emoji = "🥉 "

            highlight = "**" if login.lower() == author_login.lower() else ""
            row = (
                f"| {emoji}{idx} | {highlight}@{login}{highlight} | {merged} | {reviews} | "
                f"{comments} | {open_prs} | {closed} | {total} |"
            )
            rows.append(row)
            if login.lower() == author_login.lower():
                author_rank = idx
            if idx >= 10 and author_rank:
                break

        # If author not in top 10, append their row
        if not author_rank:
            for idx, user in enumerate(sorted_users, start=1):
                if user.get("login", "").lower() == author_login.lower():
                    login = user.get("login", "")
                    merged = user.get("mergedPrs", 0)
                    reviews = user.get("reviews", 0)
                    comments = user.get("comments", 0)
                    open_prs = user.get("openPrs", 0)
                    closed = user.get("closedPrs", 0)
                    total = user.get("total", 0)
                    rows.append(f"| ... | ... | ... | ... | ... | ... | ... | ... |")
                    rows.append(
                        f"| {idx} | **@{login}** | {merged} | {reviews} | {comments} | {open_prs} | {closed} | {total} |"
                    )
                    break

        table = table_header + "\n".join(rows)
        note_section = f"\n\n{note}" if note else ""

        return (
            f"## {self.LEADERBOARD_MARKER}\n\n"
            f"### 📊 Monthly Leaderboard ({start_date} – {end_date})\n\n"
            f"{table}\n\n"
            f"**Scoring:** Merged PR = 10 pts | Review = 5 pts | Comment = 2 pts | Open PR = 1 pt | Closed PR = -2 pts\n"
            f"[View full rankings →](https://blt-pool.owasp-blt.workers.dev/?org={owner}){note_section}"
        )

    def format_reviewer_leaderboard_comment(
        self, leaderboard_data: dict, owner: str, pr_reviewers: list = None
    ) -> str:
        """Format a reviewer-focused leaderboard comment."""
        sorted_users = leaderboard_data.get("sorted", [])
        if not sorted_users:
            return (
                f"## {self.REVIEWER_LEADERBOARD_MARKER}\n\n"
                f"### 👀 Reviewer Leaderboard\n\nNo review data available yet for this month.\n"
            )

        start_ts = leaderboard_data.get("start_timestamp", 0)
        end_ts = leaderboard_data.get("end_timestamp", 0)
        start_date = time.strftime("%b %d", time.gmtime(start_ts))
        end_date = time.strftime("%b %d, %Y", time.gmtime(end_ts))

        # Filter to only users with reviews > 0
        reviewers = [u for u in sorted_users if u.get("reviews", 0) > 0]
        reviewers.sort(key=lambda u: (-u.get("reviews", 0), -u.get("total", 0), u.get("login", "").lower()))

        table_header = (
            "| Rank | Reviewer | Reviews | PRs | Total |\n"
            "|------|----------|---------|-----|-------|\n"
        )
        rows = []
        for idx, user in enumerate(reviewers[:10], start=1):
            login = user.get("login", "")
            reviews = user.get("reviews", 0)
            merged = user.get("mergedPrs", 0)
            total = user.get("total", 0)

            emoji = ""
            if idx == 1:
                emoji = "🥇 "
            elif idx == 2:
                emoji = "🥈 "
            elif idx == 3:
                emoji = "🥉 "

            highlight = ""
            if pr_reviewers and login.lower() in [r.lower() for r in pr_reviewers]:
                highlight = "**"

            row = f"| {emoji}{idx} | {highlight}@{login}{highlight} | {reviews} | {merged} | {total} |"
            rows.append(row)

        table = table_header + "\n".join(rows)

        return (
            f"## {self.REVIEWER_LEADERBOARD_MARKER}\n\n"
            f"### 👀 Top Reviewers ({start_date} – {end_date})\n\n"
            f"{table}\n\n"
            f"Thank you to our amazing reviewers! 🙏\n"
            f"[View full rankings →](https://blt-pool.owasp-blt.workers.dev/?org={owner})"
        )

    # -------------------------------------------------------------------------
    # Backfill Operations
    # -------------------------------------------------------------------------

    async def run_incremental_backfill(self, owner: str, token: str, env, repos_per_request: int = 5) -> Optional[dict]:
        """Backfill leaderboard data in small chunks and report progress."""
        db_binding = DatabaseService.get_binding(env)
        if not db_binding:
            console.log("[Backfill] D1 not configured")
            return None
        db = DatabaseService(db_binding)

        await db.ensure_schema()
        month_key = TimeUtils.month_key()
        start_ts, end_ts = TimeUtils.month_window(month_key)
        start_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(start_ts))

        state = await db.get_backfill_state(owner, month_key)
        console.log(f"[Backfill] Current state: page={state['next_page']}, completed={state['completed']}")
        if state["completed"]:
            console.log("[Backfill] Already completed")
            return {"ran": False, "completed": True, "message": "Backfill already completed"}

        page = state["next_page"]
        console.log(f"[Backfill] Fetching repos page {page} for {owner}")
        repos_resp = await self.github.github_api(
            "GET",
            f"/orgs/{owner}/repos?sort=full_name&direction=asc&per_page={repos_per_request}&page={page}",
            token,
        )
        if repos_resp.status != 200:
            console.error(f"[Backfill] Failed to fetch repos: {repos_resp.status}")
            return None

        repos = json.loads(await repos_resp.text())
        console.log(f"[Backfill] Got {len(repos)} repos on page {page}")
        if not repos:
            await db.set_backfill_state(owner, month_key, page, True)
            return {"ran": True, "completed": True, "processed": 0, "message": "No more repos"}

        processed = 0
        for repo_obj in repos:
            repo_name = repo_obj.get("name", "")
            if repo_name:
                already_done = await db.is_repo_backfilled(owner, month_key, repo_name)
                if not already_done:
                    try:
                        await self.backfill_repo_month(owner, repo_name, token, env, month_key, start_ts, end_ts)
                        processed += 1
                    except Exception as exc:
                        console.error(f"[Backfill] Failed to backfill {repo_name}: {exc}")

        done = len(repos) < repos_per_request
        console.log(f"[Backfill] Processed {processed} repos, done={done}")
        await db.set_backfill_state(owner, month_key, page + 1, done)
        return {
            "ran": True,
            "completed": done,
            "processed": processed,
            "next_page": page + 1,
            "month_key": month_key,
            "since": start_iso,
        }

    async def backfill_repo_month(
        self,
        owner: str,
        repo_name: str,
        token: str,
        env,
        month_key: Optional[str] = None,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
    ) -> bool:
        """Backfill leaderboard stats for one repo once per month. Returns True if newly seeded."""
        db_binding = DatabaseService.get_binding(env)
        if not db_binding:
            console.log(f"[Backfill] D1 not configured for {repo_name}")
            return False
        db = DatabaseService(db_binding)

        await db.ensure_schema()
        mk = month_key or TimeUtils.month_key()
        if start_ts is None or end_ts is None:
            start_ts, end_ts = TimeUtils.month_window(mk)

        already = await db.is_repo_backfilled(owner, mk, repo_name)
        if already:
            console.log(f"[Backfill] Repo {owner}/{repo_name} already backfilled for {mk}")
            return False

        console.log(f"[Backfill] Starting backfill for {owner}/{repo_name} month={mk}")

        # Get tracked PRs to avoid double-counting
        tracked_state = await db.get_tracked_pr_numbers(owner, repo_name)
        already_tracked = set(tracked_state.keys())
        console.log(f"[Backfill] {len(already_tracked)} PRs already tracked for {owner}/{repo_name}")

        now_ts = int(time.time())

        # Open PRs snapshot
        open_resp = await self.github.github_api(
            "GET",
            f"/repos/{owner}/{repo_name}/pulls?state=open&per_page=100",
            token,
        )
        if open_resp.status == 200:
            open_prs = json.loads(await open_resp.text())
            for pr in open_prs:
                pr_number = pr.get("number")
                author = pr.get("user") or {}
                author_login = author.get("login", "")
                if not author_login or self.github.is_bot(author):
                    continue
                if pr_number in already_tracked:
                    continue
                await db.upsert_pr_state(owner, repo_name, pr_number, author_login, "open", False, None)
                await db.inc_open_pr(owner, author_login, 1)

        await db.mark_repo_backfilled(owner, mk, repo_name)
        return True
