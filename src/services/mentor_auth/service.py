"""Mentor self-service authentication and settings for BLT-Pool.

Allows mentors to log in with GitHub OAuth, edit their own profile
settings, or permanently delete their entry from the mentor pool.

Routes handled
--------------
GET  /mentor/login      — Redirect to GitHub OAuth authorization page.
GET  /mentor/callback   — Exchange OAuth code for a user token, create session.
GET  /mentor/settings   — Self-service settings form (requires login).
POST /mentor/settings   — Persist updated mentor settings (requires login).
POST /mentor/delete     — Permanently remove own mentor record (requires login).
GET  /mentor/logout     — Invalidate session and redirect to homepage.
"""

import hashlib
import html as _html_mod
import json
import re
import secrets
import time
from typing import Optional
from urllib.parse import parse_qs, urlencode, urlparse

from js import Headers, Response, console, fetch


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MENTOR_COOKIE = "blt_mentor_session"
_SESSION_TTL_SECONDS = 7 * 24 * 60 * 60  # 7 days
_STATE_TTL_SECONDS = 600  # 10 minutes for CSRF state

# Validation regexes (kept in sync with worker.py)
_GH_USERNAME_RE = re.compile(r"^[a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,37}[a-zA-Z0-9])?$")
_SPECIALTY_RE = re.compile(r"^[a-z0-9][a-z0-9+#.\-]{0,29}$")
_NAME_RE = re.compile(r"^[^<>&\"\x00-\x1f]{1,100}$")
_TIMEZONE_RE = re.compile(r"^[^<>&\"\x00-\x1f]{1,60}$")
_MENTOR_MIN_MENTEES = 1
_MENTOR_MAX_MENTEES = 10


def _escape(value: str) -> str:
    return _html_mod.escape(value or "", quote=True)


def _cookie_value(cookie_header: str, name: str) -> str:
    if not cookie_header:
        return ""
    for item in cookie_header.split(";"):
        part = item.strip()
        if not part or "=" not in part:
            continue
        key, value = part.split("=", 1)
        if key.strip() == name:
            return value.strip()
    return ""


def _session_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


class MentorAuthService:
    """GitHub-OAuth-backed mentor self-service portal."""

    def __init__(self, env):
        self.env = env
        self.db = getattr(env, "LEADERBOARD_DB", None) if env else None

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    async def handle(self, request) -> Optional[Response]:
        """Handle /mentor/* routes; return None when path is unrelated."""
        path = urlparse(str(request.url)).path.rstrip("/") or "/"
        if not path.startswith("/mentor"):
            return None
        if not self.db:
            return self._html(
                self._shell(
                    "Mentor portal unavailable",
                    "<p class='text-sm text-gray-600'>The database is not available right now. "
                    "Please try again later.</p>",
                ),
                500,
            )

        await self._ensure_tables()

        method = request.method

        if path == "/mentor/login" and method == "GET":
            return await self._handle_login_get(request)

        if path == "/mentor/callback" and method == "GET":
            return await self._handle_callback_get(request)

        if path == "/mentor/logout" and method == "GET":
            return await self._handle_logout(request)

        if path == "/mentor/settings":
            if method == "POST":
                return await self._handle_settings_post(request)
            return await self._handle_settings_get(request)

        if path == "/mentor/delete" and method == "POST":
            return await self._handle_delete_post(request)

        return self._json({"error": "Not found"}, 404)

    # ------------------------------------------------------------------
    # D1 helpers
    # ------------------------------------------------------------------

    async def _d1_run(self, sql: str, params: tuple = ()):
        stmt = self.db.prepare(sql)
        if params:
            stmt = stmt.bind(*params)
        return await stmt.run()

    async def _d1_all(self, sql: str, params: tuple = ()) -> list:
        stmt = self.db.prepare(sql)
        if params:
            stmt = stmt.bind(*params)
        raw_result = await stmt.all()

        try:
            from js import JSON as JS_JSON  # noqa: PLC0415
            parsed = json.loads(str(JS_JSON.stringify(raw_result)))
            rows = parsed.get("results") if isinstance(parsed, dict) else None
            if isinstance(rows, list):
                return rows
        except Exception:
            pass

        try:
            from pyodide.ffi import to_py  # noqa: PLC0415
            result = to_py(raw_result)
        except Exception:
            result = raw_result

        rows = None
        if isinstance(result, dict):
            rows = result.get("results")
        else:
            rows = getattr(result, "results", None)

        if rows is None:
            return []
        try:
            return list(rows)
        except Exception:
            return []

    async def _d1_first(self, sql: str, params: tuple = ()):
        rows = await self._d1_all(sql, params)
        return rows[0] if rows else None

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    async def _ensure_tables(self) -> None:
        await self._d1_run(
            """
            CREATE TABLE IF NOT EXISTS mentor_sessions (
                session_hash TEXT PRIMARY KEY,
                github_username TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL
            )
            """
        )
        await self._d1_run(
            """
            CREATE TABLE IF NOT EXISTS mentor_oauth_states (
                state TEXT PRIMARY KEY,
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL
            )
            """
        )
        # Purge expired rows
        now = int(time.time())
        await self._d1_run(
            "DELETE FROM mentor_sessions WHERE expires_at <= ?", (now,)
        )
        await self._d1_run(
            "DELETE FROM mentor_oauth_states WHERE expires_at <= ?", (now,)
        )

    # ------------------------------------------------------------------
    # Session management
    # ------------------------------------------------------------------

    async def _create_session(self, github_username: str) -> str:
        token = secrets.token_urlsafe(32)
        now = int(time.time())
        await self._d1_run(
            """
            INSERT INTO mentor_sessions (session_hash, github_username, created_at, expires_at)
            VALUES (?, ?, ?, ?)
            """,
            (_session_hash(token), github_username, now, now + _SESSION_TTL_SECONDS),
        )
        return token

    async def _current_mentor(self, request) -> Optional[str]:
        """Return the logged-in mentor's GitHub username, or None."""
        cookie = _cookie_value(
            request.headers.get("Cookie") or "", _MENTOR_COOKIE
        )
        if not cookie:
            return None
        hashed = _session_hash(cookie)
        row = await self._d1_first(
            "SELECT github_username, expires_at FROM mentor_sessions WHERE session_hash = ?",
            (hashed,),
        )
        if not row:
            return None
        if int(row.get("expires_at") or 0) <= int(time.time()):
            await self._d1_run(
                "DELETE FROM mentor_sessions WHERE session_hash = ?", (hashed,)
            )
            return None
        return row.get("github_username")

    async def _delete_session(self, request) -> None:
        cookie = _cookie_value(
            request.headers.get("Cookie") or "", _MENTOR_COOKIE
        )
        if cookie:
            await self._d1_run(
                "DELETE FROM mentor_sessions WHERE session_hash = ?",
                (_session_hash(cookie),),
            )

    # ------------------------------------------------------------------
    # OAuth state (CSRF protection)
    # ------------------------------------------------------------------

    async def _create_state(self) -> str:
        state = secrets.token_urlsafe(24)
        now = int(time.time())
        await self._d1_run(
            "INSERT INTO mentor_oauth_states (state, created_at, expires_at) VALUES (?, ?, ?)",
            (state, now, now + _STATE_TTL_SECONDS),
        )
        return state

    async def _consume_state(self, state: str) -> bool:
        """Return True and delete the state if it's valid and unexpired."""
        if not state:
            return False
        row = await self._d1_first(
            "SELECT expires_at FROM mentor_oauth_states WHERE state = ?", (state,)
        )
        if not row:
            return False
        await self._d1_run(
            "DELETE FROM mentor_oauth_states WHERE state = ?", (state,)
        )
        return int(row.get("expires_at") or 0) > int(time.time())

    # ------------------------------------------------------------------
    # OAuth handlers
    # ------------------------------------------------------------------

    async def _handle_login_get(self, request) -> Response:
        client_id = getattr(self.env, "GITHUB_CLIENT_ID", "") or ""
        if not client_id:
            return self._html(
                self._shell(
                    "Login unavailable",
                    "<p class='text-sm text-gray-600'>GitHub OAuth is not configured for this "
                    "deployment. Please contact the administrator.</p>",
                ),
                503,
            )
        # Redirect already-logged-in users directly to settings
        username = await self._current_mentor(request)
        if username:
            return self._redirect("/mentor/settings")

        state = await self._create_state()
        redirect_uri = self._callback_uri(request)
        params = urlencode({
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "scope": "read:user",
            "state": state,
        })
        return self._redirect(f"https://github.com/login/oauth/authorize?{params}")

    async def _handle_callback_get(self, request) -> Response:
        client_id = getattr(self.env, "GITHUB_CLIENT_ID", "") or ""
        client_secret = getattr(self.env, "GITHUB_CLIENT_SECRET", "") or ""
        if not client_id or not client_secret:
            return self._html(
                self._shell(
                    "Login unavailable",
                    "<p class='text-sm text-gray-600'>GitHub OAuth is not fully configured. "
                    "Please contact the administrator.</p>",
                ),
                503,
            )

        qs = dict(
            pair.split("=", 1)
            for pair in (urlparse(str(request.url)).query or "").split("&")
            if "=" in pair
        )
        code = qs.get("code", "")
        state = qs.get("state", "")

        if not code:
            return self._html(
                self._shell(
                    "Login failed",
                    "<p class='text-sm text-gray-600'>No authorization code received from GitHub. "
                    "Please <a href='/mentor/login' class='text-red-600 hover:underline'>try again</a>.</p>",
                ),
                400,
            )

        if not await self._consume_state(state):
            return self._html(
                self._shell(
                    "Login failed",
                    "<p class='text-sm text-gray-600'>Invalid or expired login attempt. "
                    "Please <a href='/mentor/login' class='text-red-600 hover:underline'>try again</a>.</p>",
                ),
                400,
            )

        # Exchange code for access token
        access_token = await self._exchange_code(client_id, client_secret, code, request)
        if not access_token:
            return self._html(
                self._shell(
                    "Login failed",
                    "<p class='text-sm text-gray-600'>Could not obtain an access token from GitHub. "
                    "Please <a href='/mentor/login' class='text-red-600 hover:underline'>try again</a>.</p>",
                ),
                502,
            )

        # Get GitHub username from the token
        github_username = await self._get_gh_username(access_token)
        if not github_username:
            return self._html(
                self._shell(
                    "Login failed",
                    "<p class='text-sm text-gray-600'>Could not verify your GitHub identity. "
                    "Please <a href='/mentor/login' class='text-red-600 hover:underline'>try again</a>.</p>",
                ),
                502,
            )

        # Check the user is actually in the mentor pool
        mentor = await self._d1_first(
            "SELECT github_username FROM mentors WHERE github_username = ?",
            (github_username,),
        )
        if not mentor:
            return self._html(
                self._shell(
                    "Not a mentor",
                    f"<p class='text-sm text-gray-600'>Your GitHub account "
                    f"(<strong>@{_escape(github_username)}</strong>) is not in the mentor pool. "
                    f"If you'd like to become a mentor, please register first via the "
                    f"<a href='/' class='text-red-600 hover:underline'>homepage</a>.</p>",
                ),
                403,
            )

        session_token = await self._create_session(github_username)
        return self._redirect(
            "/mentor/settings",
            set_cookie=self._session_cookie(session_token),
        )

    async def _handle_logout(self, request) -> Response:
        await self._delete_session(request)
        return self._redirect("/", set_cookie=self._clear_session_cookie())

    # ------------------------------------------------------------------
    # Settings handlers
    # ------------------------------------------------------------------

    async def _handle_settings_get(self, request) -> Response:
        username = await self._current_mentor(request)
        if not username:
            return self._redirect("/mentor/login")

        mentor = await self._d1_first(
            "SELECT * FROM mentors WHERE github_username = ?", (username,)
        )
        if not mentor:
            # They were a mentor but their record was deleted
            await self._delete_session(request)
            return self._redirect("/", set_cookie=self._clear_session_cookie())

        try:
            specialties = json.loads(mentor.get("specialties") or "[]")
        except Exception:
            specialties = []

        return self._html(
            self._shell(
                "Edit your mentor profile",
                self._settings_form(
                    username=username,
                    name=mentor.get("name", ""),
                    specialties=specialties,
                    max_mentees=int(mentor.get("max_mentees") or 3),
                    timezone=mentor.get("timezone") or "",
                    active=bool(int(mentor.get("active") or 1)),
                ),
                user=username,
            )
        )

    async def _handle_settings_post(self, request) -> Response:
        username = await self._current_mentor(request)
        if not username:
            return self._redirect("/mentor/login")

        form = await self._form_data(request)
        name = (form.get("name") or "").strip()
        specialties_raw = (form.get("specialties") or "").strip()
        max_mentees_raw = form.get("max_mentees", "3")
        timezone = (form.get("timezone") or "").strip()
        active = form.get("active", "") == "1"

        # Validate name
        if not name:
            return self._html(
                self._shell(
                    "Edit your mentor profile",
                    self._settings_form(
                        username=username,
                        name=name,
                        specialties=[s.strip() for s in specialties_raw.split(",") if s.strip()],
                        max_mentees=3,
                        timezone=timezone,
                        active=active,
                        error="Display name is required.",
                    ),
                    user=username,
                ),
                400,
            )
        if not _NAME_RE.match(name):
            return self._html(
                self._shell(
                    "Edit your mentor profile",
                    self._settings_form(
                        username=username,
                        name=name,
                        specialties=[s.strip() for s in specialties_raw.split(",") if s.strip()],
                        max_mentees=3,
                        timezone=timezone,
                        active=active,
                        error="Display name contains invalid characters.",
                    ),
                    user=username,
                ),
                400,
            )

        # Validate specialties
        specialties = [s.strip() for s in specialties_raw.split(",") if s.strip()]
        for spec in specialties:
            if not _SPECIALTY_RE.match(spec):
                return self._html(
                    self._shell(
                        "Edit your mentor profile",
                        self._settings_form(
                            username=username,
                            name=name,
                            specialties=specialties,
                            max_mentees=3,
                            timezone=timezone,
                            active=active,
                            error=f"Invalid specialty tag: {spec!r}. "
                                  "Use lowercase letters, digits, +, #, . or - only.",
                        ),
                        user=username,
                    ),
                    400,
                )

        try:
            max_mentees = max(
                _MENTOR_MIN_MENTEES, min(_MENTOR_MAX_MENTEES, int(max_mentees_raw))
            )
        except (TypeError, ValueError):
            max_mentees = 3

        if timezone and not _TIMEZONE_RE.match(timezone):
            return self._html(
                self._shell(
                    "Edit your mentor profile",
                    self._settings_form(
                        username=username,
                        name=name,
                        specialties=specialties,
                        max_mentees=max_mentees,
                        timezone=timezone,
                        active=active,
                        error="Timezone contains invalid characters.",
                    ),
                    user=username,
                ),
                400,
            )

        try:
            await self._d1_run(
                """
                UPDATE mentors
                SET name = ?, specialties = ?, max_mentees = ?, timezone = ?, active = ?
                WHERE github_username = ?
                """,
                (
                    name,
                    json.dumps(specialties),
                    max_mentees,
                    timezone,
                    1 if active else 0,
                    username,
                ),
            )
        except Exception as exc:
            console.error(f"[MentorAuth] Failed to update mentor {username}: {exc}")
            return self._html(
                self._shell(
                    "Edit your mentor profile",
                    self._settings_form(
                        username=username,
                        name=name,
                        specialties=specialties,
                        max_mentees=max_mentees,
                        timezone=timezone,
                        active=active,
                        error="Failed to save changes. Please try again.",
                    ),
                    user=username,
                ),
                500,
            )

        console.log(f"[MentorAuth] Mentor @{username} updated their profile")
        return self._html(
            self._shell(
                "Profile updated",
                self._settings_form(
                    username=username,
                    name=name,
                    specialties=specialties,
                    max_mentees=max_mentees,
                    timezone=timezone,
                    active=active,
                    success="Your profile has been updated successfully.",
                ),
                user=username,
            )
        )

    async def _handle_delete_post(self, request) -> Response:
        username = await self._current_mentor(request)
        if not username:
            return self._redirect("/mentor/login")

        form = await self._form_data(request)
        confirm = form.get("confirm_username", "").strip().lstrip("@")
        if confirm.lower() != username.lower():
            mentor = await self._d1_first(
                "SELECT * FROM mentors WHERE github_username = ?", (username,)
            )
            try:
                specialties = json.loads((mentor or {}).get("specialties") or "[]")
            except Exception:
                specialties = []
            return self._html(
                self._shell(
                    "Edit your mentor profile",
                    self._settings_form(
                        username=username,
                        name=(mentor or {}).get("name", ""),
                        specialties=specialties,
                        max_mentees=int((mentor or {}).get("max_mentees") or 3),
                        timezone=(mentor or {}).get("timezone") or "",
                        active=bool(int((mentor or {}).get("active") or 1)),
                        error="Username confirmation did not match. Your profile was not deleted.",
                    ),
                    user=username,
                ),
                400,
            )

        try:
            await self._d1_run(
                "DELETE FROM mentor_assignments WHERE mentor_login = ?", (username,)
            )
            await self._d1_run(
                "DELETE FROM mentors WHERE github_username = ?", (username,)
            )
        except Exception as exc:
            console.error(f"[MentorAuth] Failed to delete mentor {username}: {exc}")
            return self._html(
                self._shell(
                    "Edit your mentor profile",
                    "<p class='text-sm text-red-600'>Failed to delete your profile. Please try again later.</p>",
                    user=username,
                ),
                500,
            )

        await self._delete_session(request)
        console.log(f"[MentorAuth] Mentor @{username} deleted their own profile")
        return self._redirect("/", set_cookie=self._clear_session_cookie())

    # ------------------------------------------------------------------
    # GitHub OAuth helpers
    # ------------------------------------------------------------------

    def _callback_uri(self, request) -> str:
        """Build the absolute OAuth callback URI from the incoming request URL."""
        parsed = urlparse(str(request.url))
        return f"{parsed.scheme}://{parsed.netloc}/mentor/callback"

    async def _exchange_code(
        self, client_id: str, client_secret: str, code: str, request
    ) -> str:
        """Exchange an OAuth authorization code for an access token.

        Returns the access token string, or empty string on failure.
        """
        redirect_uri = self._callback_uri(request)
        try:
            resp = await fetch(
                "https://github.com/login/oauth/access_token",
                method="POST",
                headers=Headers.new({
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "User-Agent": "BLT-Pool/1.0",
                }.items()),
                body=json.dumps({
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "code": code,
                    "redirect_uri": redirect_uri,
                }),
            )
            data = json.loads(await resp.text())
            return data.get("access_token") or ""
        except Exception as exc:
            console.error(f"[MentorAuth] Token exchange failed: {exc}")
            return ""

    async def _get_gh_username(self, access_token: str) -> str:
        """Return the GitHub username associated with an access token."""
        try:
            resp = await fetch(
                "https://api.github.com/user",
                method="GET",
                headers=Headers.new({
                    "Accept": "application/vnd.github+json",
                    "Authorization": f"Bearer {access_token}",
                    "User-Agent": "BLT-Pool/1.0",
                    "X-GitHub-Api-Version": "2022-11-28",
                }.items()),
            )
            if resp.status != 200:
                return ""
            data = json.loads(await resp.text())
            return (data.get("login") or "").strip()
        except Exception as exc:
            console.error(f"[MentorAuth] GitHub user lookup failed: {exc}")
            return ""

    # ------------------------------------------------------------------
    # Form helpers
    # ------------------------------------------------------------------

    async def _form_data(self, request) -> dict:
        body = await request.text()
        parsed = parse_qs(body, keep_blank_values=False)
        return {key: values[0].strip() if values else "" for key, values in parsed.items()}

    # ------------------------------------------------------------------
    # HTML rendering
    # ------------------------------------------------------------------

    def _settings_form(
        self,
        *,
        username: str,
        name: str,
        specialties: list,
        max_mentees: int,
        timezone: str,
        active: bool,
        error: str = "",
        success: str = "",
    ) -> str:
        error_html = ""
        if error:
            error_html = (
                f"<p class='mb-4 rounded-lg border border-red-200 bg-red-50 px-4 py-3 "
                f"text-sm font-medium text-red-700'>{_escape(error)}</p>"
            )
        success_html = ""
        if success:
            success_html = (
                f"<p class='mb-4 rounded-lg border border-emerald-200 bg-emerald-50 px-4 py-3 "
                f"text-sm font-medium text-emerald-700'>{_escape(success)}</p>"
            )
        specialties_str = ", ".join(specialties)
        active_checked = "checked" if active else ""
        mentee_options = "\n".join(
            f'<option value="{i}"{" selected" if i == max_mentees else ""}>{i}</option>'
            for i in range(_MENTOR_MIN_MENTEES, _MENTOR_MAX_MENTEES + 1)
        )
        return f"""
        <div class="mx-auto max-w-2xl space-y-8">
          {error_html}
          {success_html}

          <!-- Settings form -->
          <form method="POST" action="/mentor/settings" class="space-y-5">
            <div class="flex items-center gap-4">
              <img src="https://github.com/{_escape(username)}.png"
                   alt="{_escape(username)}"
                   class="h-16 w-16 rounded-full border border-[#E5E5E5] bg-white object-cover">
              <div>
                <p class="font-semibold text-[#111827]">@{_escape(username)}</p>
                <p class="text-xs text-gray-500">GitHub account used to log in</p>
              </div>
            </div>

            <div>
              <label for="name" class="mb-1 block text-sm font-semibold text-gray-700">
                Display name <span class="text-red-600">*</span>
              </label>
              <input id="name" name="name" type="text" required maxlength="100"
                     value="{_escape(name)}"
                     class="w-full rounded-md border border-gray-300 px-4 py-2 text-sm
                            text-gray-900 focus:border-red-600 focus:ring-1 focus:ring-red-600
                            focus:outline-none">
            </div>

            <div>
              <label for="specialties" class="mb-1 block text-sm font-semibold text-gray-700">
                Specialties
              </label>
              <input id="specialties" name="specialties" type="text"
                     value="{_escape(specialties_str)}"
                     placeholder="python, frontend, security"
                     class="w-full rounded-md border border-gray-300 px-4 py-2 text-sm
                            text-gray-900 focus:border-red-600 focus:ring-1 focus:ring-red-600
                            focus:outline-none">
              <p class="mt-1 text-xs text-gray-500">
                Comma-separated list. Each tag: lowercase, letters/digits/+#.-,  max 30 chars.
              </p>
            </div>

            <div>
              <label for="max_mentees" class="mb-1 block text-sm font-semibold text-gray-700">
                Maximum concurrent mentees
              </label>
              <select id="max_mentees" name="max_mentees"
                      class="rounded-md border border-gray-300 px-3 py-2 text-sm text-gray-900
                             focus:border-red-600 focus:ring-1 focus:ring-red-600 focus:outline-none">
                {mentee_options}
              </select>
            </div>

            <div>
              <label for="timezone" class="mb-1 block text-sm font-semibold text-gray-700">
                Timezone
              </label>
              <input id="timezone" name="timezone" type="text" maxlength="60"
                     value="{_escape(timezone)}"
                     placeholder="UTC+5:30"
                     class="w-full rounded-md border border-gray-300 px-4 py-2 text-sm
                            text-gray-900 focus:border-red-600 focus:ring-1 focus:ring-red-600
                            focus:outline-none">
            </div>

            <div class="flex items-center gap-3">
              <input id="active" name="active" type="checkbox" value="1" {active_checked}
                     class="h-4 w-4 rounded border-gray-300 text-red-600 focus:ring-red-600">
              <label for="active" class="text-sm font-semibold text-gray-700">
                Visible in the public mentor pool (published)
              </label>
            </div>

            <div class="flex gap-3 pt-2">
              <button type="submit"
                      class="inline-flex items-center gap-2 rounded-md bg-[#E10101] px-5 py-3
                             text-sm font-semibold text-white transition hover:bg-red-700
                             focus:outline-none focus:ring-2 focus:ring-red-600 focus:ring-offset-2">
                <i class="fa-solid fa-floppy-disk" aria-hidden="true"></i>
                Save changes
              </button>
              <a href="/"
                 class="inline-flex items-center gap-2 rounded-md border border-[#E5E5E5] px-5 py-3
                        text-sm font-semibold text-gray-700 transition hover:border-gray-400
                        focus:outline-none focus:ring-2 focus:ring-gray-400 focus:ring-offset-2">
                Back to homepage
              </a>
            </div>
          </form>

          <!-- Danger zone -->
          <div class="rounded-2xl border border-red-200 bg-red-50 p-6">
            <h3 class="text-base font-bold text-red-700">
              <i class="fa-solid fa-triangle-exclamation mr-1" aria-hidden="true"></i>
              Danger zone
            </h3>
            <p class="mt-2 text-sm text-red-600">
              Deleting your profile is permanent and cannot be undone. All of your active
              mentoring assignments will also be removed.
            </p>
            <details class="mt-4">
              <summary class="cursor-pointer text-sm font-semibold text-red-700 hover:underline">
                Delete my profile
              </summary>
              <form method="POST" action="/mentor/delete" class="mt-4 space-y-3">
                <p class="text-sm text-red-600">
                  Type your GitHub username (<strong>@{_escape(username)}</strong>) below to confirm.
                </p>
                <input name="confirm_username" type="text" required
                       placeholder="{_escape(username)}"
                       class="w-full rounded-md border border-red-300 px-4 py-2 text-sm
                              text-gray-900 focus:border-red-600 focus:ring-1 focus:ring-red-600
                              focus:outline-none">
                <button type="submit"
                        class="inline-flex items-center gap-2 rounded-md border border-red-200
                               bg-white px-4 py-2 text-sm font-semibold text-red-700
                               transition hover:bg-red-50">
                  <i class="fa-solid fa-trash" aria-hidden="true"></i>
                  Permanently delete my profile
                </button>
              </form>
            </details>
          </div>
        </div>
        """

    def _shell(self, title: str, content: str, user: str = "") -> str:
        user_chip = ""
        if user:
            user_chip = (
                f'<div class="inline-flex items-center gap-2 rounded-full border border-[#E5E5E5] '
                f'bg-white px-3 py-1 text-xs font-semibold text-gray-600">'
                f'<img src="https://github.com/{_escape(user)}.png" '
                f'alt="{_escape(user)}" class="h-5 w-5 rounded-full">'
                f'@{_escape(user)}</div>'
            )
        logout_link = (
            "<a href='/mentor/logout' "
            "class='inline-flex items-center gap-2 rounded-md border border-[#E10101] "
            "px-4 py-2 text-sm font-semibold text-[#E10101] transition "
            "hover:bg-[#E10101] hover:text-white'>"
            "<i class='fa-solid fa-right-from-bracket' aria-hidden='true'></i>Logout</a>"
            if user else ""
        )
        return f"""<!DOCTYPE html>
<html lang="en" class="scroll-smooth">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{_escape(title)} | BLT-Pool</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap" rel="stylesheet">
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css" crossorigin="anonymous" referrerpolicy="no-referrer">
  <script>
    tailwind.config = {{
      theme: {{
        extend: {{
          colors: {{ 'blt-primary': '#E10101', 'blt-border': '#E5E5E5' }},
          fontFamily: {{ sans: ['Plus Jakarta Sans', 'ui-sans-serif', 'system-ui', 'sans-serif'] }}
        }}
      }}
    }}
  </script>
  <style>
    body {{
      background:
        radial-gradient(circle at 0% 0%, rgba(225, 1, 1, 0.09), transparent 32%),
        radial-gradient(circle at 95% 4%, rgba(225, 1, 1, 0.05), transparent 28%),
        #f8fafc;
    }}
  </style>
</head>
<body class="min-h-screen font-sans text-gray-900 antialiased">
  <header class="sticky top-0 z-40 border-b border-[#E5E5E5] bg-white/90 backdrop-blur">
    <div class="mx-auto flex max-w-7xl items-center justify-between gap-3 px-4 py-4 sm:px-6 lg:px-8">
      <a href="/" class="flex items-center gap-3" aria-label="BLT-Pool home">
        <img src="/logo-sm.png" alt="OWASP BLT logo" class="h-10 w-10 rounded-xl border border-[#E5E5E5] bg-white object-contain p-1">
        <div>
          <p class="text-sm font-semibold uppercase tracking-wide text-gray-500">OWASP BLT</p>
          <h1 class="text-lg font-extrabold text-[#111827]">BLT-Pool</h1>
        </div>
      </a>
      <div class="flex items-center gap-3">
        {user_chip}
        {logout_link}
      </div>
    </div>
  </header>
  <main class="mx-auto w-full max-w-7xl px-4 py-10 sm:px-6 lg:px-8">
    <section class="overflow-hidden rounded-3xl border border-[#E5E5E5] bg-white p-7 shadow-[0_14px_40px_rgba(225,1,1,0.10)] sm:p-10">
      <div class="mb-8">
        <span class="inline-flex items-center gap-2 rounded-full border border-[#E5E5E5] bg-gray-50 px-3 py-1 text-xs font-semibold text-gray-700">
          <i class="fa-brands fa-github text-[#E10101]" aria-hidden="true"></i>
          Mentor self-service
        </span>
        <h2 class="mt-4 text-3xl font-extrabold text-[#111827] sm:text-4xl">{_escape(title)}</h2>
      </div>
      {content}
    </section>
  </main>
</body>
</html>"""

    # ------------------------------------------------------------------
    # Response helpers
    # ------------------------------------------------------------------

    def _json(self, payload, status: int = 200) -> Response:
        return Response.new(
            json.dumps(payload),
            status=status,
            headers=Headers.new({"Content-Type": "application/json"}.items()),
        )

    def _html(self, body: str, status: int = 200, set_cookie: str = "") -> Response:
        headers = {"Content-Type": "text/html; charset=utf-8"}
        if set_cookie:
            headers["Set-Cookie"] = set_cookie
        return Response.new(body, status=status, headers=Headers.new(headers.items()))

    def _redirect(self, location: str, set_cookie: str = "") -> Response:
        headers = {"Location": location}
        if set_cookie:
            headers["Set-Cookie"] = set_cookie
        return Response.new("", status=302, headers=Headers.new(headers.items()))

    def _session_cookie(self, token: str) -> str:
        return (
            f"{_MENTOR_COOKIE}={token}; Path=/; HttpOnly; SameSite=Lax; "
            f"Max-Age={_SESSION_TTL_SECONDS}"
        )

    def _clear_session_cookie(self) -> str:
        return f"{_MENTOR_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0"
