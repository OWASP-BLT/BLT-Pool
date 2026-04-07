"""Webhook handler service for BLT-Pool GitHub events."""

import json
from typing import Optional

try:
    from js import Response, console
except ImportError:
    # Fallback for testing outside Cloudflare Workers
    class _MockConsole:
        def log(self, *args, **kwargs): pass
        def error(self, *args, **kwargs): pass
    console = _MockConsole()
    Response = None

from services.github import GitHubService
from services.leaderboard import LeaderboardService
from services.mentor import MentorService


class WebhookService:
    """Handle all GitHub webhook events."""

    # Assignment/issue management constants
    ASSIGN_COMMAND = "/assign"
    UNASSIGN_COMMAND = "/unassign"
    LEADERBOARD_COMMAND = "/leaderboard"
    MAX_ASSIGNEES = 1
    ASSIGNMENT_DURATION_HOURS = 8
    BUG_LABELS = {"bug", "vulnerability", "security"}

    def __init__(
        self,
        github_service: GitHubService,
        leaderboard_service: LeaderboardService,
        mentor_service: MentorService,
        rendering_service,
    ):
        """Initialize WebhookService.
        
        Args:
            github_service: GitHubService instance
            leaderboard_service: LeaderboardService instance
            mentor_service: MentorService instance
            rendering_service: RenderingService instance
        """
        self.github = github_service
        self.leaderboard = leaderboard_service
        self.mentor = mentor_service
        self.renderer = rendering_service

    async def handle_webhook(self, request, env) -> Response:
        """Main webhook handler — dispatches events to specific handlers."""
        # Verify webhook signature
        payload_bytes = await request.arrayBuffer()
        payload_bytes = bytes(payload_bytes)
        signature = request.headers.get("X-Hub-Signature-256", "")
        webhook_secret = getattr(env, "WEBHOOK_SECRET", "")
        
        if not self.github.verify_signature(payload_bytes, signature, webhook_secret):
            return self.renderer.json_response({"error": "Invalid signature"}, 401)

        # Parse payload
        try:
            payload = json.loads(payload_bytes.decode("utf-8"))
        except Exception as exc:
            console.error(f"[Webhook] Failed to parse payload: {exc}")
            return self.renderer.json_response({"error": "Invalid payload"}, 400)

        event_type = request.headers.get("X-GitHub-Event", "")
        action = payload.get("action", "")

        # Get installation token
        installation_id = (payload.get("installation") or {}).get("id")
        if not installation_id:
            console.log(f"[Webhook] No installation ID for event {event_type}")
            return self.renderer.json_response({"status": "ignored"}, 200)

        app_id = getattr(env, "APP_ID", "")
        private_key = getattr(env, "PRIVATE_KEY", "")
        token = await self.github.get_installation_token(installation_id, app_id, private_key)
        if not token:
            return self.renderer.json_response({"error": "Failed to get token"}, 500)

        # Dispatch to appropriate handler
        try:
            if event_type == "issue_comment" and action == "created":
                await self.handle_issue_comment(payload, token, env)
            elif event_type == "issues" and action == "opened":
                await self.handle_issue_opened(payload, token, env)
            elif event_type == "issues" and action == "labeled":
                await self.handle_issue_labeled(payload, token, env)
            elif event_type == "pull_request" and action == "opened":
                await self.handle_pull_request_opened(payload, token, env)
            elif event_type == "pull_request" and action in ("closed", "reopened"):
                await self.handle_pull_request_closed(payload, token, env)
            elif event_type == "pull_request_review" and action == "submitted":
                await self.handle_pull_request_review(payload, token, env)
            elif event_type == "workflow_run":
                await self.handle_workflow_run(payload, token)
            elif event_type == "check_run":
                await self.handle_check_run(payload, token)
            else:
                console.log(f"[Webhook] Unhandled event: {event_type} / {action}")
        except Exception as exc:
            console.error(f"[Webhook] Handler error for {event_type}/{action}: {exc}")
            return self.renderer.json_response({"error": str(exc)}, 500)

        return self.renderer.json_response({"status": "ok"}, 200)

    # -------------------------------------------------------------------------
    # Event Handlers
    # -------------------------------------------------------------------------

    async def handle_issue_comment(self, payload: dict, token: str, env=None) -> None:
        """Handle issue_comment.created events."""
        comment = payload.get("comment") or {}
        issue = payload.get("issue") or {}
        repo_data = payload.get("repository") or {}
        owner = (repo_data.get("owner") or {}).get("login", "")
        repo = repo_data.get("name", "")
        comment_id = comment.get("id")
        body = comment.get("body", "")
        user = comment.get("user") or {}
        login = user.get("login", "")

        if not (owner and repo and login):
            return

        # Ignore bot comments
        if self.github.is_bot(user):
            return

        # Track comment in leaderboard
        await self.leaderboard.track_comment(payload, env)

        # Extract command
        command = self.github.extract_command(body)
        if not command:
            return

        # Handle commands
        if command == self.ASSIGN_COMMAND:
            await self._assign(owner, repo, issue, login, token)
            await self.github.create_reaction(owner, repo, comment_id, "+1", token)
        elif command == self.UNASSIGN_COMMAND:
            await self._unassign(owner, repo, issue, login, token)
            await self.github.create_reaction(owner, repo, comment_id, "+1", token)
        elif command == self.LEADERBOARD_COMMAND:
            # Implementation would post leaderboard here
            pass
        elif command == self.mentor.MENTOR_COMMAND:
            await self.mentor.handle_mentor_command(owner, repo, issue, login, token, None, env)
        elif command == self.mentor.UNMENTOR_COMMAND:
            await self.mentor.handle_mentor_unassign(owner, repo, issue, login, token, env)
        elif command == self.mentor.MENTOR_PAUSE_COMMAND:
            await self.mentor.handle_mentor_pause(owner, repo, issue, login, token, None, env)
        elif command == self.mentor.HANDOFF_COMMAND:
            await self.mentor.handle_mentor_handoff(owner, repo, issue, login, token, None, env)
        elif command == self.mentor.REMATCH_COMMAND:
            await self.mentor.handle_mentor_rematch(owner, repo, issue, login, token, None, env)

    async def handle_issue_opened(self, payload: dict, token: str, env=None) -> None:
        """Handle issues.opened events."""
        issue = payload.get("issue") or {}
        repo_data = payload.get("repository") or {}
        owner = (repo_data.get("owner") or {}).get("login", "")
        repo = repo_data.get("name", "")
        labels = issue.get("labels", [])
        label_names = {lbl.get("name", "").lower() for lbl in labels}

        # Check if issue has bug labels
        if self.BUG_LABELS & label_names:
            blt_api_url = getattr(env, "BLT_API_URL", "https://blt-api.owasp-blt.workers.dev")
            issue_data = {
                "url": issue.get("html_url", ""),
                "github_url": issue.get("html_url", ""),
                "description": issue.get("title", ""),
                "label": "general",
            }
            await self.github.report_bug_to_blt(blt_api_url, issue_data)

    async def handle_issue_labeled(self, payload: dict, token: str, env=None) -> None:
        """Handle issues.labeled events."""
        issue = payload.get("issue") or {}
        label = payload.get("label") or {}
        label_name = label.get("name", "")
        repo_data = payload.get("repository") or {}
        owner = (repo_data.get("owner") or {}).get("login", "")
        repo = repo_data.get("name", "")

        # Auto-assign mentor when needs-mentor label is added
        if label_name == self.mentor.NEEDS_MENTOR_LABEL:
            issue_author = (issue.get("user") or {}).get("login", "")
            await self.mentor.assign_mentor_to_issue(owner, repo, issue, issue_author, token, None, None, env)

    async def handle_pull_request_opened(self, payload: dict, token: str, env=None) -> None:
        """Handle pull_request.opened events."""
        await self.leaderboard.track_pr_opened(payload, env)

    async def handle_pull_request_closed(self, payload: dict, token: str, env=None) -> None:
        """Handle pull_request.closed and reopened events."""
        action = payload.get("action", "")
        
        if action == "closed":
            await self.leaderboard.track_pr_closed(payload, env)
        elif action == "reopened":
            # Track as opened again
            await self.leaderboard.track_pr_opened(payload, env)

    async def handle_pull_request_review(self, payload: dict, token: str, env=None) -> None:
        """Handle pull_request_review.submitted events."""
        await self.leaderboard.track_review(payload, env)

    async def handle_workflow_run(self, payload: dict, token: str) -> None:
        """Handle workflow_run events."""
        console.log("[Webhook] workflow_run event received (no action)")

    async def handle_check_run(self, payload: dict, token: str) -> None:
        """Handle check_run events."""
        console.log("[Webhook] check_run event received (no action)")

    # -------------------------------------------------------------------------
    # Helper Methods for Issue Assignment
    # -------------------------------------------------------------------------

    async def _assign(self, owner: str, repo: str, issue: dict, login: str, token: str) -> None:
        """Assign an issue to a user."""
        issue_number = issue.get("number")
        issue_author = (issue.get("user") or {}).get("login", "")
        assignees = [a.get("login", "") for a in issue.get("assignees", [])]

        # Only allow issue author or maintainers to assign
        if login.lower() != issue_author.lower():
            is_maintainer = await self.github.is_maintainer(owner, repo, login, token)
            if not is_maintainer:
                await self.github.create_comment(
                    owner, repo, issue_number,
                    "⚠️ Only the issue author or maintainers can assign this issue.",
                    token
                )
                return

        # Check if already at max assignees
        if len(assignees) >= self.MAX_ASSIGNEES:
            await self.github.create_comment(
                owner, repo, issue_number,
                f"⚠️ This issue already has {self.MAX_ASSIGNEES} assignee(s). Please unassign first.",
                token
            )
            return

        # Assign the user
        resp = await self.github.github_api(
            "POST",
            f"/repos/{owner}/{repo}/issues/{issue_number}/assignees",
            token,
            {"assignees": [login]},
        )
        
        if resp.status in (200, 201):
            await self.github.create_comment(
                owner, repo, issue_number,
                f"✅ Assigned to @{login}",
                token
            )
        else:
            await self.github.create_comment(
                owner, repo, issue_number,
                "⚠️ Failed to assign. Please try again.",
                token
            )

    async def _unassign(self, owner: str, repo: str, issue: dict, login: str, token: str) -> None:
        """Unassign an issue from a user."""
        issue_number = issue.get("number")
        assignees = [a.get("login", "") for a in issue.get("assignees", [])]

        if not assignees:
            await self.github.create_comment(
                owner, repo, issue_number,
                "⚠️ This issue has no assignees.",
                token
            )
            return

        # Unassign
        resp = await self.github.github_api(
            "DELETE",
            f"/repos/{owner}/{repo}/issues/{issue_number}/assignees",
            token,
            {"assignees": assignees},
        )
        
        if resp.status in (200, 201):
            await self.github.create_comment(
                owner, repo, issue_number,
                "✅ Unassigned all assignees",
                token
            )
