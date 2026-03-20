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
import hmac as _hmac
import html as _html_mod
import json
import os
import re
import time
import types as _types
from typing import Optional, Tuple
from urllib.parse import quote, urlparse

from js import Headers, Response, console, fetch
from index_template import GITHUB_PAGE_HTML
from services.admin import AdminService, has_merged_pr_in_org
from services.mentor_seed import INITIAL_MENTORS

# Import all modules to make their contents available
import constants
import crypto
import github_client
import db
import leaderboard
import mentor_pool
import event_handlers
import html_gen

# Re-export everything from each module into this namespace
# Import constants, regex patterns, and functions/classes defined in our modules
# Skip stdlib imports like Optional, Tuple, etc.
_STDLIB_MODULES = {'typing', 'js', 'calendar', 'json', 'time', 'urllib', 're', 'secrets', 'html', 'base64', 'hashlib', 'hmac', 'pyodide'}
_OUR_MODULES = {'constants', 'crypto', 'github_client', 'db', 'leaderboard', 'mentor_pool', 'event_handlers', 'html_gen'}
for _mod in [constants, crypto, github_client, db, leaderboard, mentor_pool, event_handlers, html_gen]:
    _mod_name = _mod.__name__
    for _name in dir(_mod):
        if _name.startswith('__'):
            continue
        _obj = getattr(_mod, _name)
        
        # Decide whether to import this object
        should_import = True
        
        if hasattr(_obj, '__module__'):
            obj_mod = _obj.__module__
            if obj_mod in _STDLIB_MODULES or obj_mod.split('.')[0] in _STDLIB_MODULES:
                # It's from stdlib - but check if it's a constant/pattern that was created in our module
                # Functions and classes have their defining module set, but instances don't
                if isinstance(_obj, (_types.FunctionType, type)):
                    # It's a function or class from stdlib, skip it
                    should_import = False
                else:
                    # It's an instance (like a regex Pattern) - import it if the name suggests it's a constant
                    should_import = _name.isupper() or _name.startswith('_')
            elif obj_mod not in _OUR_MODULES and obj_mod != _mod_name:
                # From some other external module
                should_import = False
                
        if should_import:
            globals()[_name] = _obj


def _rebind_globals_to_worker():
    """Rebind all imported functions to use this module's globals dict.
    
    This ensures patch.object(_worker, 'X', mock) works correctly because all
    functions in this namespace will look up 'X' from this module's __dict__
    at call time. Critical for test compatibility after module refactoring.
    
    Only rebinds functions that were defined in our own modules (constants, crypto,
    github_client, db, leaderboard, mentor_pool, event_handlers, html_gen).
    Does NOT rebind functions from standard library or third-party modules.
    """
    this_globals = globals()
    our_modules = {'constants', 'crypto', 'github_client', 'db', 'leaderboard', 'mentor_pool', 'event_handlers', 'html_gen'}
    
    # Track which functions are aliases (point to the same function object)
    # Build this before rebinding so we can fix aliases after
    aliases_to_fix = []  # List of (alias_name, primary_name) tuples
    functions_seen = {}  # Maps function id to primary name
    for _name, _obj in list(this_globals.items()):
        if not isinstance(_obj, _types.FunctionType):
            continue
        _id = id(_obj)
        if _id in functions_seen:
            # This is an alias
            aliases_to_fix.append((_name, functions_seen[_id]))
        else:
            functions_seen[_id] = _name
    
    # Rebind functions
    for _name, _obj in list(this_globals.items()):
        if isinstance(_obj, _types.FunctionType) and _obj.__globals__ is not this_globals:
            # Only rebind if the function was defined in one of our modules
            obj_module = getattr(_obj, '__module__', '')
            if obj_module in our_modules:
                _new_fn = _types.FunctionType(
                    _obj.__code__,
                    this_globals,
                    _obj.__name__,
                    _obj.__defaults__,
                    _obj.__closure__,
                )
                _new_fn.__kwdefaults__ = _obj.__kwdefaults__
                _new_fn.__annotations__ = _obj.__annotations__
                this_globals[_name] = _new_fn
                
    # Fix aliases to point to rebound functions
    for alias_name, primary_name in aliases_to_fix:
        this_globals[alias_name] = this_globals[primary_name]


_rebind_globals_to_worker()


# ---------------------------------------------------------------------------
# Main entry point — called by the Cloudflare runtime
# ---------------------------------------------------------------------------


async def on_fetch(request, env) -> Response:
    method = request.method
    path = urlparse(str(request.url)).path.rstrip("/") or "/"

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
        return _html(_index_html(mentors, mentor_stats, active_assignments, assignment_comment_stats))

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
    if method == "POST" and path == "/admin/reset-leaderboard-month":
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
        # Get GitHub App installation token
        app_id = getattr(env, "APP_ID", "")
        private_key = getattr(env, "PRIVATE_KEY", "")
        
        if not app_id or not private_key:
            console.error("[CRON] Missing APP_ID or PRIVATE_KEY")
            return
        
        # For cron jobs, we need to iterate through all installations
        # Get an app JWT first
        jwt_token = await create_github_jwt(app_id, private_key)
        
        # Fetch all installations
        installations_resp = await github_api("GET", "/app/installations", jwt_token)
        if installations_resp.status != 200:
            console.error(f"[CRON] Failed to fetch installations: {installations_resp.status}")
            return
        
        installations = json.loads(await installations_resp.text())
        console.log(f"[CRON] Found {len(installations)} installations")
        
        for installation in installations:
            install_id = installation["id"]
            account = installation["account"]
            account_login = account.get("login", "unknown")
            
            console.log(f"[CRON] Processing installation {install_id} for {account_login}")
            
            # Get installation token
            token = await get_installation_access_token(install_id, jwt_token)
            if not token:
                console.error(f"[CRON] Failed to get token for installation {install_id}")
                continue
            
            # Fetch all repos for this installation (limit to 20 for cron to prevent timeouts)
            repos = []
            if account.get("type") == "Organization":
                repos = await _fetch_org_repos(account_login, token, limit=20)
            else:
                # For user accounts, fetch user repos (limited)
                repos_resp = await github_api("GET", f"/users/{account_login}/repos?per_page=20", token)
                if repos_resp.status == 200:
                    repos = json.loads(await repos_resp.text())
            
            console.log(f"[CRON] Checking {len(repos)} repositories")
            
            # Check each repository for stale assignments
            for repo_data in repos:
                repo_name = repo_data["name"]
                owner = repo_data["owner"]["login"]
                
                await _check_stale_assignments(owner, repo_name, token)
                await _check_stale_mentor_assignments(owner, repo_name, token)
        
        console.log("[CRON] Stale assignment check complete")
        
    except Exception as e:
        console.error(f"[CRON] Error during scheduled task: {e}")


async def on_scheduled(controller, env, ctx=None):
    """Cloudflare Python Workers cron entrypoint."""
    await _run_scheduled(env)


async def scheduled(event, env):
    """Backward-compatible alias for runtimes expecting scheduled()."""
    await _run_scheduled(env)


async def _check_stale_assignments(owner: str, repo: str, token: str):
    """Check a repository for stale issue assignments and unassign them."""
    try:
        # Fetch open issues with assignees
        issues_resp = await github_api(
            "GET",
            f"/repos/{owner}/{repo}/issues?state=open&per_page=100",
            token
        )
        
        if issues_resp.status != 200:
            return
        
        issues = json.loads(await issues_resp.text())
        
        # Filter issues that have assignees and are not pull requests
        assigned_issues = [
            issue for issue in issues
            if issue.get("assignees") and "pull_request" not in issue
        ]
        
        if not assigned_issues:
            return
        
        console.log(f"[CRON] Found {len(assigned_issues)} assigned issues in {owner}/{repo}")
        
        current_time = time.time()
        deadline_seconds = ASSIGNMENT_DURATION_HOURS * 3600
        
        for issue in assigned_issues:
            issue_number = issue["number"]
            assignees = issue.get("assignees", [])
            
            # Check if issue has linked PRs
            timeline_resp = await github_api(
                "GET",
                f"/repos/{owner}/{repo}/issues/{issue_number}/timeline",
                token
            )
            
            if timeline_resp.status != 200:
                continue
            
            timeline = json.loads(await timeline_resp.text())
            
            # Look for assignment events and cross-referenced PRs
            assignment_time = None
            has_linked_pr = False
            
            for event in timeline:
                event_type = event.get("event")
                
                # Track the most recent assignment
                if event_type == "assigned":
                    created_at = event.get("created_at", "")
                    if created_at:
                        event_timestamp = _parse_github_timestamp(created_at)
                        if event_timestamp:
                            assignment_time = event_timestamp
                
                # Check for cross-referenced PRs
                if event_type == "cross-referenced":
                    source = event.get("source", {})
                    if source.get("type") == "issue" and "pull_request" in source.get("issue", {}):
                        has_linked_pr = True
                        break
            
            # If no assignment time found in timeline, use updated_at as fallback
            if assignment_time is None:
                updated_at = issue.get("updated_at", "")
                if updated_at:
                    assignment_time = _parse_github_timestamp(updated_at)
            
            # Skip if we couldn't determine assignment time
            if assignment_time is None:
                continue
            
            time_elapsed = current_time - assignment_time
            
            # Unassign if deadline passed and no linked PR
            if time_elapsed > deadline_seconds and not has_linked_pr:
                hours_elapsed = int(time_elapsed / 3600)
                
                console.log(
                    f"[CRON] Unassigning stale issue {owner}/{repo}#{issue_number} "
                    f"(assigned {hours_elapsed}h ago, no PR)"
                )
                
                # Unassign all assignees
                assignee_logins = [a["login"] for a in assignees]
                await github_api(
                    "DELETE",
                    f"/repos/{owner}/{repo}/issues/{issue_number}/assignees",
                    token,
                    {"assignees": assignee_logins}
                )
                
                # Post a comment explaining the unassignment
                assignee_mentions = ", ".join(f"@{login}" for login in assignee_logins)
                await create_comment(
                    owner, repo, issue_number,
                    f"{assignee_mentions} This issue has been automatically unassigned because "
                    f"the {ASSIGNMENT_DURATION_HOURS}-hour deadline has passed without a linked pull request.\n\n"
                    f"The issue is now available for others to claim. If you'd still like to work on this, "
                    f"please comment `{ASSIGN_COMMAND}` again.\n\n"
                    "Thank you for your interest! 🙏 — [OWASP BLT-Pool](https://pool.owaspblt.org)",
                    token
                )
    
    except Exception as e:
        console.error(f"[CRON] Error checking {owner}/{repo}: {e}")
