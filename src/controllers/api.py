import json
import re

from js import fetch, Headers, console
from views.pages import _json
from services.admin import has_merged_pr_in_org
from models.mentor import _d1_add_mentor, _NAME_RE, _GH_USERNAME_RE, _SPECIALTY_RE, _TIMEZONE_RE, _MENTOR_MIN_MENTEES_CAP, _MENTOR_MAX_MENTEES_CAP
from models.leaderboard import _ensure_leaderboard_schema, _reset_leaderboard_month
from core.github_client import _gh_headers, github_api
from core.db import _d1_binding, _d1_all


async def _verify_gh_user_exists(username: str, env=None) -> bool:
    """Return True if the GitHub username exists on GitHub.

    Uses GITHUB_TOKEN from env if available (5,000 req/h); falls back to
    unauthenticated requests (60 req/h per IP) when no token is set.
    Returns True on network/API error so a transient outage does not block
    legitimate submissions (fail-open policy).
    """
    token = getattr(env, "GITHUB_TOKEN", "") if env else ""
    try:
        resp = await github_api("GET", f"/users/{username}", token)
        return resp.status == 200
    except Exception:
        return True  # Fail open: don't block when GitHub API is temporarily unavailable

async def _handle_add_mentor(request, env) -> "Response":
    """POST /api/mentors — insert a new mentor into the D1 mentors table.

    Expected JSON body::

        {
            "name": "Jane Doe",
            "github_username": "janedoe",
            "specialties": ["frontend", "python"],   // optional
            "max_mentees": 3,                         // optional, 1-10
            "timezone": "UTC+5:30",                   // optional
            "referred_by": "referrer"                 // optional
        }

    Returns 201 on success, 400 on validation failure, 500 on DB error.
    """
    try:
        body = json.loads(await request.text())
    except Exception:
        return _json({"error": "Invalid JSON body"}, 400)

    name = (body.get("name") or "").strip()
    github_username = (body.get("github_username") or "").strip().lstrip("@")
    specialties_raw = body.get("specialties") or []
    max_mentees = body.get("max_mentees", 3)
    timezone = (body.get("timezone") or "").strip()
    referred_by = (body.get("referred_by") or "").strip().lstrip("@")

    if not name:
        return _json({"error": "Field 'name' is required"}, 400)
    if not _NAME_RE.match(name):
        return _json({"error": "Display name contains invalid characters (HTML and scripting are not allowed)"}, 400)
    if not github_username:
        return _json({"error": "Field 'github_username' is required"}, 400)
    if not _GH_USERNAME_RE.match(github_username):
        return _json({"error": "Invalid GitHub username format"}, 400)

    # Verify the GitHub username actually exists.
    if not await _verify_gh_user_exists(github_username, env):
        return _json({"error": f"GitHub username '{github_username}' was not found on GitHub"}, 400)

    # Normalise specialties — accept a list or a comma-separated string.
    if isinstance(specialties_raw, str):
        specialties = [s.strip() for s in specialties_raw.split(",") if s.strip()]
    elif isinstance(specialties_raw, list):
        specialties = [str(s).strip() for s in specialties_raw if str(s).strip()]
    else:
        specialties = []
    # Validate each specialty tag.
    for spec in specialties:
        if not _SPECIALTY_RE.match(spec):
            return _json({"error": f"Invalid specialty tag: {spec!r}"}, 400)

    try:
        max_mentees = max(_MENTOR_MIN_MENTEES_CAP, min(_MENTOR_MAX_MENTEES_CAP, int(max_mentees)))
    except (TypeError, ValueError):
        max_mentees = 3

    if timezone and not _TIMEZONE_RE.match(timezone):
        return _json({"error": "Timezone contains invalid characters (HTML and scripting are not allowed)"}, 400)

    if referred_by and not _GH_USERNAME_RE.match(referred_by):
        return _json({"error": "Invalid referred_by username format"}, 400)

    # Verify the referrer's GitHub username exists (if provided).
    if referred_by and not await _verify_gh_user_exists(referred_by, env):
        return _json({"error": f"Referred-by username '{referred_by}' was not found on GitHub"}, 400)

    db = _d1_binding(env)
    if not db:
        return _json({"error": "Database not available"}, 500)

    mentor_is_active = await has_merged_pr_in_org(
        env,
        github_username,
        getattr(env, "GITHUB_ORG", "OWASP-BLT"),
    )

    try:
        existing = await _d1_all(
            db,
            "SELECT github_username FROM mentors WHERE github_username = ?",
            (github_username,),
        )
        if existing:
            return _json({"error": f"GitHub user '{github_username}' is already in the mentor pool"}, 409)
    except Exception as exc:
        import traceback; traceback.print_exc()
        console.error(f"[MentorPool] Failed to check duplicate mentor {github_username}: {exc}")
        return _json({"error": "Failed to validate mentor. Please try again later."}, 500)

    try:
        await _ensure_leaderboard_schema(db)
        await _d1_add_mentor(
            db,
            github_username=github_username,
            name=name,
            specialties=specialties,
            max_mentees=max_mentees,
            active=mentor_is_active,
            timezone=timezone,
            referred_by=referred_by,
        )
    except Exception as exc:
        import traceback; traceback.print_exc()
        console.error(f"[MentorPool] Failed to add mentor {github_username}: {exc}")
        return _json({"error": "Failed to save mentor"}, 500)

    console.log(
        f"[MentorPool] Added mentor {github_username} via API active={mentor_is_active}"
    )
    return _json(
        {"ok": True, "github_username": github_username, "active": mentor_is_active},
        201,
    )



async def _handle_admin_reset(request, env):
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
            {"error": "Missing required field: month_key (e.g. '2026-03'). Provide an explicit month to prevent accidental resets."},
            400,
        )
    if not re.fullmatch(r"\d{4}-\d{2}", month_key):
        return _json({"error": "month_key must be in YYYY-MM format (e.g. '2026-03')"}, 400)
    db = _d1_binding(env)
    if not db:
        return _json({"error": "No D1 binding available"}, 500)
        
    try:
        deleted = await _reset_leaderboard_month(org, month_key, db)
        return _json({
            "ok": True,
            "org": org,
            "month_key": month_key,
            "deleted": deleted
        })
    except Exception as exc:
        console.error(f"[BLT] Reset leaderboard error: {exc}")
        return _json({"error": "Internal server error"}, 500)
