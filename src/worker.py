"""BLT-Pool — Mentor Matching & GitHub Automation Platform.

A dual-purpose platform that:
1. Connects contributors with mentors through a shared mentor pool
2. Automates GitHub workflows (issue assignment, leaderboard, webhooks)

Homepage (/) displays the mentor grid with availability and assignments.
GitHub App documentation and installation at /github-app
(legacy alias: /github-app).

Entry point: ``on_fetch(request, env)`` — called by the Cloudflare runtime for
every incoming HTTP request.

Environment variables / secrets (configure via ``wrangler.toml`` or
``wrangler secret put``):
    APP_ID             — GitHub App numeric ID
    PRIVATE_KEY        — GitHub App RSA private key (PEM, PKCS#1 or PKCS#8)
    WEBHOOK_SECRET     — GitHub App webhook secret
    GITHUB_APP_SLUG    — GitHub App slug used to build the install URL
    BLT_API_URL        — BLT API base URL (default: https://blt-api.owasp-blt.workers.dev)
    GITHUB_CLIENT_ID   — OAuth client ID (optional)
    GITHUB_CLIENT_SECRET — OAuth client secret (optional)
"""

from js import Headers, Response, console  # Cloudflare Workers JS bindings

from core.db import _d1_binding
from models.mentor import _populate_mentors_table, _fetch_mentors_config, _load_mentors_from_d1
from models.assignment import _d1_get_active_assignments
from models.leaderboard import _ensure_leaderboard_schema, _fetch_leaderboard_data, _get_backfill_state, _run_incremental_backfill, _d1_get_user_comment_totals
from views.pages import _landing_html, _index_html, _webhook_security_status, _callback_html, _github_app_html, _secret_vars_status_html, _html, _json
from controllers.api import _handle_add_mentor
from controllers.webhook import handle_webhook
from controllers.mentor_commands import _check_stale_mentor_assignments
from services.admin import AdminService

# Re-exports required by existing test suites in test_worker.py
from core.crypto import _der_len, _wrap_pkcs1_as_pkcs8, pem_to_pkcs8_der, _b64url, verify_signature, create_github_jwt
from core.github_client import _gh_headers, github_api, get_installation_token, get_installation_access_token, create_comment, create_reaction, report_bug_to_blt, _is_human, _is_bot, _is_coderabbit_ping, _is_maintainer, _extract_command, _ensure_label_exists
from core.db import _month_key, _month_window, _d1_run, _to_py, _d1_all, _d1_first, _time_ago
from models.mentor import _parse_yaml_scalar, _parse_mentors_yaml, _fetch_mentors_config, _load_mentors_local, _fetch_mentor_stats_from_d1, _get_mentor_load_map, _select_mentor, _find_assigned_mentor_from_comments, _get_last_human_activity_ts, _is_security_issue, _d1_add_mentor, _NAME_RE, _GH_USERNAME_RE, _SPECIALTY_RE, _TIMEZONE_RE, MENTOR_ASSIGNED_LABEL, NEEDS_MENTOR_LABEL, MENTOR_LABEL_COLOR, MENTOR_MAX_MENTEES, SECURITY_BYPASS_LABELS, MENTOR_STALE_DAYS, _MENTOR_STATS_CACHE_TTL
from models.assignment import _d1_record_mentor_assignment, _d1_remove_mentor_assignment, _d1_get_mentor_loads, _d1_get_active_assignments
from models.leaderboard import _ensure_leaderboard_schema, _d1_get_user_comment_totals, _d1_inc_open_pr, _d1_inc_monthly, _track_pr_opened_in_d1, _track_pr_closed_in_d1, _track_pr_reopened_in_d1, _track_comment_in_d1, _track_review_in_d1, _calculate_leaderboard_stats_from_d1, _get_backfill_state, _set_backfill_state, _run_incremental_backfill, _backfill_repo_month_if_needed, _reset_leaderboard_month, _fetch_org_repos, _calculate_leaderboard_stats, _fetch_leaderboard_data
from views.comments import _parse_github_timestamp, _avatar_img_tag, _format_leaderboard_comment, _format_reviewer_leaderboard_comment, _post_reviewer_leaderboard, _post_or_update_leaderboard, _check_and_close_excess_prs, _check_rank_improvement, LEADERBOARD_MARKER, REVIEWER_LEADERBOARD_MARKER, MERGED_PR_COMMENT_MARKER
from views.pages import _CALLBACK_HTML, _generate_mentor_row, _build_referral_leaderboard
from controllers.issue_handlers import handle_issue_comment, _assign, _unassign, _approve, _deny, _NO_WELCOME_REPOS_YML_PATH, _NO_WELCOME_REPOS_CACHE, _load_no_welcome_repos, handle_issue_opened, handle_issue_labeled, ASSIGN_COMMAND, UNASSIGN_COMMAND, APPROVE_COMMAND, DENY_COMMAND, LEADERBOARD_COMMAND, MENTOR_COMMAND, UNMENTOR_COMMAND, MENTOR_PAUSE_COMMAND, HANDOFF_COMMAND, REMATCH_COMMAND, MAX_ASSIGNEES, ASSIGNMENT_DURATION_HOURS, BUG_LABELS, HELP_WANTED_LABEL, TRIAGE_REVIEWER, NEEDS_APPROVAL_LABEL, NEEDS_APPROVAL_LABEL_COLOR
from controllers.pr_handlers import handle_pull_request_opened, _request_mentor_reviewer_for_pr, _assign_round_robin_mentor_reviewer, _post_merged_pr_combined_comment, handle_pull_request_closed, handle_pull_request_review_submitted, check_unresolved_conversations, label_pending_checks, check_workflows_awaiting_approval, _try_label_pending_checks, handle_workflow_run, handle_check_run, MENTOR_AUTO_PR_REVIEWER_ENABLED
from controllers.mentor_commands import _assign_mentor_to_issue, handle_mentor_command, handle_mentor_unassign, handle_mentor_pause, handle_mentor_handoff, handle_mentor_rematch
from controllers.peer_review import _is_excluded_reviewer, get_valid_reviewers, ensure_label_exists, update_peer_review_labels, check_peer_review_and_comment, handle_pull_request_review, handle_pull_request_for_review
from controllers.api import _verify_gh_user_exists, _handle_admin_reset
from services.mentor_seed import INITIAL_MENTORS
_INITIAL_MENTORS = INITIAL_MENTORS


# ---------------------------------------------------------------------------
# Cloudflare Workers entry point
# ---------------------------------------------------------------------------

import traceback

async def on_fetch(request, env) -> Response:
    """Main routing entry point for incoming HTTP requests."""
    url = str(request.url)

    # Allow requests from GitHub domains for CORS when making client-side requests from the GitHub UI (e.g., from comment forms)
    headers = Headers.new([
        ["Access-Control-Allow-Origin", "*"],
        ["Access-Control-Allow-Methods", "GET, POST, OPTIONS"],
        ["Access-Control-Allow-Headers", "Content-Type"]
    ])

    if request.method == "OPTIONS":
        return Response.new("", headers=headers, status=204)

    try:
        # 1. GitHub App Webhook endpoint
        if url.endswith("/webhook") and request.method == "POST":
            return await handle_webhook(request, env)

        # 2. Mentor matching pool directory (Homepage)
        if url.endswith("/") and request.method == "GET":
            from core.db import _d1_binding
            db = _d1_binding(env)
            if db:
                await _ensure_leaderboard_schema(db)
                from models.mentor import _populate_mentors_table
                await _populate_mentors_table(db)
            
            # Fetch mentors, passing True to trigger D1 population if needed
            mentors = await _fetch_mentors_config(env=env)
            from models.leaderboard import _calculate_leaderboard_stats_from_d1
            stats = await _calculate_leaderboard_stats_from_d1("OWASP", env) or {}
            active_assignments = await _d1_get_active_assignments(db, "OWASP") if db else []
            
            mentor_logins = [m.get("github_username") for m in mentors if m.get("github_username")]
            mentee_logins = [a.get("mentee_login") for a in active_assignments if a.get("mentee_login")]
            comment_stats = await _d1_get_user_comment_totals(db, "OWASP", mentor_logins + mentee_logins) if db else {}

            return _html(
                _index_html(
                    mentors=mentors,
                    mentor_stats=stats,
                    active_assignments=active_assignments,
                    assignment_comment_stats=comment_stats,
                )
            )

        # 3. GitHub App documentation & install link
        if (url.endswith("/github-app") or url.endswith("/github-app/")) and request.method == "GET":
            slug = getattr(env, "GITHUB_APP_SLUG", "blt-github-app")
            return _html(_github_app_html(slug, env))

        # 4. GitHub callback (redirects back to homepage after app installation)
        if url.split("?")[0].endswith("/callback") and request.method == "GET":
            return _html(_callback_html())

        # 5. REST API: Add new mentor (called from client-side JS on the homepage form)
        if url.endswith("/api/mentors") and request.method == "POST":
            return await _handle_add_mentor(request, env)
            
        if url.endswith("/api/github/webhooks") and request.method == "POST":
            from controllers.webhook import handle_webhook
            return await handle_webhook(request, env)
            
        if url.endswith("/admin/reset-leaderboard-month") and request.method == "POST":
            return await _handle_admin_reset(request, env)
            
        if url.endswith("/health") and request.method == "GET":
            from views.pages import _webhook_security_status
            return _json(_webhook_security_status(env))

        # Admin Service Integration
        if "/admin" in url:
            return await AdminService(env).handle(request)

        return _json({"error": "Not found"}, 404)
    except Exception as exc:
        traceback.print_exc()
        console.error(f"[BLT] Setup/routing error: {exc}")
        return _json({"error": "Internal server error"}, 500)

# ---------------------------------------------------------------------------
# Cloudflare Workers Cron Trigger
# ---------------------------------------------------------------------------

async def on_scheduled(controller, env, ctx=None):
    """Entry point for Cloudflare Scheduled (Cron) events.

    Triggers background jobs that cannot rely on webhooks, such as
    backfilling historical leaderboard stats and releasing stale mentors.
    """
    console.log("[BLT][Timer] Triggered scheduled background task")
    try:
        await _run_scheduled(env)
    except Exception as e:
        console.error(f"[BLT][Timer] Uncaught error: {e}")

# Provide both entry point names just in case the JS shim expects a specific one.
scheduled = on_scheduled


async def _run_scheduled(env) -> None:
    """Run all scheduled background tasks."""
    from core.db import _d1_binding, _month_key
    installation_id = "56316277"  # Default OWASP installation
    app_id = getattr(env, "APP_ID", "")
    private_key = getattr(env, "PRIVATE_KEY", "")
    owner = "OWASP-BLT"
    
    db = _d1_binding(env)
    
    # 1. Backfill stats
    if db and app_id and private_key:
        month_key = _month_key()
        state = await _get_backfill_state(db, owner, month_key)
        if not state["completed"]:
            token = await get_installation_token(installation_id, app_id, private_key)
            if not token:
                console.error("[Leaderboard] Cannot run backfill: failed to get token")
            else:
                await _run_incremental_backfill(owner, token, env)
        
    # 2. Free stale mentor assignments
    if app_id and private_key:
        token = await get_installation_token(installation_id, app_id, private_key)
        if token:
            # Check a few core repositories (this could be expanded or made dynamic)
            for repo in ["owasp.github.io", "BLT", "blt-extension"]:
                await _check_stale_mentor_assignments("OWASP", repo, token)
