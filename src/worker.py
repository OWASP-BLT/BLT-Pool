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

import base64
import calendar
import hashlib
import hmac
import json
import os
import re
import time
import traceback
from typing import Optional, Tuple

from js import Headers, Response, console, fetch  # Cloudflare Workers JS bindings

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
from controllers.pr_handlers import handle_pull_request_opened, _request_mentor_reviewer_for_pr, _assign_round_robin_mentor_reviewer, _post_merged_pr_combined_comment, handle_pull_request_closed, handle_pull_request_review_submitted, label_pending_checks, check_workflows_awaiting_approval, _try_label_pending_checks, handle_workflow_run, handle_check_run, MENTOR_AUTO_PR_REVIEWER_ENABLED, UNRESOLVED_CONVERSATIONS_CHECK_NAME, UNRESOLVED_CONVERSATIONS_MARKER
from controllers.mentor_commands import _assign_mentor_to_issue, handle_mentor_command, handle_mentor_unassign, handle_mentor_pause, handle_mentor_handoff, handle_mentor_rematch
from controllers.peer_review import _is_excluded_reviewer, get_valid_reviewers, ensure_label_exists, update_peer_review_labels, check_peer_review_and_comment, handle_pull_request_review, handle_pull_request_for_review
from controllers.api import _verify_gh_user_exists, _handle_admin_reset
from services.mentor_seed import INITIAL_MENTORS
_INITIAL_MENTORS = INITIAL_MENTORS

def _admin_path(env) -> str:
    return getattr(env, "ADMIN_PATH", "/admin")


async def check_unresolved_conversations(payload, token):
    """Check for unresolved PR review threads and update a GitHub check-run + PR comment."""
    pr = payload.get("pull_request")
    if not pr:
        return

    owner = payload["repository"]["owner"]["login"]
    repo = payload["repository"]["name"]
    number = pr["number"]
    head_sha = pr.get("head", {}).get("sha", "")

    query = """
    query($owner: String!, $repo: String!, $number: Int!) {
      repository(owner: $owner, name: $repo) {
        pullRequest(number: $number) {
          reviewThreads(first: 100) {
            nodes { isResolved }
          }
        }
      }
    }
    """

    resp = await fetch(
        "https://api.github.com/graphql",
        method="POST",
        headers=_gh_headers(token),
        body=json.dumps({
            "query": query,
            "variables": {"owner": owner, "repo": repo, "number": number},
        }),
    )

    if resp.status != 200:
        console.error(f"[BLT] GraphQL query failed: {resp.status}")
        return

    result = json.loads(await resp.text())
    pull_request = (
        result.get("data", {})
        .get("repository", {})
        .get("pullRequest")
    )
    if result.get("errors") or pull_request is None:
        console.error(f"[BLT] GraphQL reviewThreads query returned errors: {result.get('errors')}")
        return
    threads = pull_request.get("reviewThreads", {}).get("nodes", [])
    unresolved_count = sum(not t.get("isResolved", True) for t in threads)
    unresolved = unresolved_count > 0

    # -----------------------------------------------------------------------
    # Check-run management
    # -----------------------------------------------------------------------
    existing_check_run_id = None
    if head_sha:
        cr_resp = await github_api(
            "GET",
            f"/repos/{owner}/{repo}/commits/{head_sha}/check-runs?check_name={UNRESOLVED_CONVERSATIONS_CHECK_NAME}",
            token,
        )
        if cr_resp.status == 200:
            cr_data = json.loads(await cr_resp.text())
            runs = cr_data.get("check_runs", [])
            if runs:
                existing_check_run_id = runs[0]["id"]

    import time as _time
    cr_payload = {
        "name": UNRESOLVED_CONVERSATIONS_CHECK_NAME,
        "head_sha": head_sha,
        "status": "completed",
        "conclusion": "failure" if unresolved else "success",
        "completed_at": _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime()),
        "output": {
            "title": f"{unresolved_count} unresolved conversation(s)" if unresolved else "All conversations resolved",
            "summary": f"There are {unresolved_count} unresolved review thread(s)." if unresolved else "No unresolved review threads.",
        },
    }

    if existing_check_run_id:
        await github_api("PATCH", f"/repos/{owner}/{repo}/check-runs/{existing_check_run_id}", token, cr_payload)
    else:
        await github_api("POST", f"/repos/{owner}/{repo}/check-runs", token, cr_payload)

    # -----------------------------------------------------------------------
    # PR comment management
    # -----------------------------------------------------------------------
    existing_comment_id = None
    comments_resp = await github_api("GET", f"/repos/{owner}/{repo}/issues/{number}/comments?per_page=100", token)
    if comments_resp.status == 200:
        for c in json.loads(await comments_resp.text()):
            if UNRESOLVED_CONVERSATIONS_MARKER in (c.get("body") or ""):
                existing_comment_id = c["id"]
                break

    if unresolved:
        body = (
            f"{UNRESOLVED_CONVERSATIONS_MARKER}\n"
            f"⚠️ This PR has **{unresolved_count}** unresolved review conversation(s). "
            "Please resolve them before merging."
        )
        if existing_comment_id:
            await github_api("PATCH", f"/repos/{owner}/{repo}/issues/comments/{existing_comment_id}", token, {"body": body})
        else:
            await create_comment(owner, repo, number, body, token)
    else:
        if existing_comment_id:
            await github_api("DELETE", f"/repos/{owner}/{repo}/issues/comments/{existing_comment_id}", token)

    # -----------------------------------------------------------------------
    # Label management
    # -----------------------------------------------------------------------
    from urllib.parse import quote
    resp_labels = await github_api("GET", f"/repos/{owner}/{repo}/issues/{number}/labels", token)
    if resp_labels.status == 200:
        for lb in json.loads(await resp_labels.text()):
            if lb.get("name", "").startswith("unresolved-conversations"):
                await github_api(
                    "DELETE",
                    f"/repos/{owner}/{repo}/issues/{number}/labels/{quote(lb['name'], safe='')}",
                    token,
                )

    label = f"unresolved-conversations: {unresolved_count}"
    if unresolved:
        await _ensure_label_exists(owner, repo, label, "e74c3c", token)
    else:
        await _ensure_label_exists(owner, repo, label, "5cb85c", token)

    await github_api(
        "POST",
        f"/repos/{owner}/{repo}/issues/{number}/labels",
        token,
        {"labels": [label]},
    )


# Cloudflare Workers entry point
async def on_fetch(request, env) -> Response:
    """Main routing entry point for incoming HTTP requests."""
    url = str(request.url)
    method = request.method
    path = "/" + "/".join(url.split("//", 1)[-1].split("/")[1:]).split("?")[0]

    # Allow requests from GitHub domains for CORS when making client-side requests from the GitHub UI (e.g., from comment forms)
    headers = Headers.new([
        ["Access-Control-Allow-Origin", "*"],
        ["Access-Control-Allow-Methods", "GET, POST, OPTIONS"],
        ["Access-Control-Allow-Headers", "Content-Type"]
    ])

    if request.method == "OPTIONS":
        return Response.new("", headers=headers, status=204)

    if path == "/logo-sm.png" or path.endswith("logo-sm.png"):
        return await env.ASSETS.fetch(request)

    admin_response = await AdminService(env).handle(request)
    if admin_response is not None:
        return admin_response

    if method == "GET" and path == "/":
        # Load mentors from D1.
        org = getattr(env, "GITHUB_ORG", "OWASP-BLT")
        mentors: list = []
        try:
            mentors = await _load_mentors_local(env)
        except Exception as exc:
            console.error(f"[MentorPool] Failed to load mentors for homepage: {exc}")
        # Fetch per-mentor activity stats from D1 (best-effort; no stats if D1 unavailable).
        mentor_stats: dict = {}
        try:
            token = getattr(env, "GITHUB_TOKEN", "")
            mentor_stats = await _fetch_mentor_stats_from_d1(env, org, mentors=mentors, token=token)
        except Exception as exc:
            console.error(f"[MentorPool] Failed to fetch mentor stats for homepage: {exc}")
        # Fetch active mentor assignments from D1 (best-effort).
        active_assignments: list = []
        assignment_comment_stats: dict = {}
        db = _d1_binding(env)
        if db:
            try:
                await _ensure_leaderboard_schema(db)
                active_assignments = await _d1_get_active_assignments(db, org)
            except Exception as exc:
                console.error(f"[MentorPool] Failed to fetch active assignments for homepage: {exc}")
            if active_assignments:
                try:
                    all_logins = list({
                        login
                        for a in active_assignments
                        for login in (a["mentor_login"], a.get("mentee_login", ""))
                        if login
                    })
                    assignment_comment_stats = await _d1_get_user_comment_totals(db, org, all_logins)
                except Exception as exc:
                    console.error(f"[MentorPool] Failed to fetch assignment comment stats: {exc}")
        return _html(_index_html(mentors, mentor_stats, active_assignments, assignment_comment_stats, _admin_path(env)))

    if method == "GET" and path == "/github-app":
        app_slug = getattr(env, "GITHUB_APP_SLUG", "")
        return _html(_github_app_html(app_slug, env))

    if method == "GET" and path == "/health":
        webhook_security = _webhook_security_status(env)
        return _json(
            {
                "status": "ok" if webhook_security["ready"] else "degraded",
                "service": "BLT-Pool",
                "checks": {
                    "webhook_security": webhook_security,
                },
            }
        )

    if method == "POST" and path == "/api/mentors":
        return await _handle_add_mentor(request, env)

    if method == "POST" and path == "/api/github/webhooks":
        return await handle_webhook(request, env)

    # GitHub redirects here after a successful installation
    if method == "GET" and path == "/callback":
        return _html(_callback_html())

    # Admin: reset corrupted leaderboard data for a given org/month so a fresh
    # backfill can re-populate it.  Requires ADMIN_SECRET env variable.
    if method == "POST" and path in {"/admin/reset-leaderboard-month", f"{_admin_path(env)}/reset-leaderboard-month"}:
        admin_secret = getattr(env, "ADMIN_SECRET", "")
        if not admin_secret:
            return _json({"error": "Admin endpoint not configured"}, 403)
        auth_header = (request.headers.get("Authorization") or "").strip()
        if auth_header != f"Bearer {admin_secret}":
            return _json({"error": "Unauthorized"}, 401)
        try:
            body = json.loads(await request.text())
        except Exception:
            return _json({"error": "Invalid JSON body"}, 400)
        org = (body.get("org") or "").strip()
        if not org:
            return _json({"error": "Missing required field: org"}, 400)
        month_key = (body.get("month_key") or "").strip()
        if not month_key:
            return _json(
                {"error": "Missing required field: month_key (e.g. '2026-03'). "
                 "Provide an explicit month to prevent accidental resets."},
                400,
            )
        if not re.fullmatch(r"\d{4}-\d{2}", month_key):
            return _json({"error": "month_key must be in YYYY-MM format (e.g. '2026-03')"}, 400)
        db = _d1_binding(env)
        if not db:
            return _json({"error": "No D1 binding available"}, 500)
        deleted = await _reset_leaderboard_month(org, month_key, db)
        return _json({"ok": True, "org": org, "month_key": month_key, "tables_cleared": deleted})

    return _json({"error": "Not found"}, 404)


# ---------------------------------------------------------------------------
# Scheduled event handler — runs on cron triggers
# ---------------------------------------------------------------------------


async def _run_scheduled(env):
    """Handle scheduled cron events to check and unassign stale issues.
    
    This runs periodically (configured in wrangler.toml) to find issues that:
    - Have assignees
    - Were assigned more than ASSIGNMENT_DURATION_HOURS ago
    - Have no linked pull requests
    
    Such issues are automatically unassigned to free them up for other contributors.
    """
    console.log("[CRON] Starting stale assignment check...")

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
                await _check_stale_mentor_assignments("OWASP", repo, token, env=env)
