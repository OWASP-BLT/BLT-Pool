"""GitHub API client functions and utilities."""

import calendar
import json
import time
from typing import Optional

from js import Headers, Response, console, fetch

from constants import (
    ASSIGN_COMMAND, UNASSIGN_COMMAND, LEADERBOARD_COMMAND,
    MENTOR_COMMAND, UNMENTOR_COMMAND, MENTOR_PAUSE_COMMAND,
    HANDOFF_COMMAND, REMATCH_COMMAND,
)
from crypto import create_github_jwt


# ---------------------------------------------------------------------------
# GitHub API helpers
# ---------------------------------------------------------------------------


def _gh_headers(token: str) -> Headers:
    h = {
        "Accept": "application/vnd.github+json",
        "Content-Type": "application/json",
        "User-Agent": "BLT-Pool/1.0",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        h["Authorization"] = f"Bearer {token}"
    return Headers.new(h.items())


async def github_api(method: str, path: str, token: str, body=None, timeout_seconds: Optional[float] = None):
    """Make an authenticated request to the GitHub REST API."""
    url = f"https://api.github.com{path}"
    kwargs = {"method": method, "headers": _gh_headers(token)}
    if body is not None:
        kwargs["body"] = json.dumps(body)
    if timeout_seconds is not None and float(timeout_seconds) > 0:
        try:
            from js import AbortController, setTimeout, clearTimeout  # noqa: PLC0415 - runtime import
        except Exception:
            return await fetch(url, **kwargs)

        controller = AbortController.new()
        kwargs["signal"] = controller.signal
        timeout_ms = max(1, int(float(timeout_seconds) * 1000))
        timer_id = setTimeout(lambda: controller.abort(), timeout_ms)
        try:
            return await fetch(url, **kwargs)
        finally:
            clearTimeout(timer_id)

    return await fetch(url, **kwargs)


async def _sleep_seconds(seconds: float) -> None:
    """Async sleep helper that works in Workers runtime and local tests."""
    delay = max(0.0, float(seconds or 0.0))
    if delay <= 0:
        return
    try:
        from js import Promise, setTimeout  # noqa: PLC0415 - runtime import
        await Promise.new(lambda resolve, reject: setTimeout(resolve, int(delay * 1000)))
    except Exception:
        # Local-test fallback.
        time.sleep(delay)


def _retry_after_seconds(resp) -> int:
    """Parse Retry-After from a GitHub response, defaulting to 1s."""
    try:
        raw = None
        headers = getattr(resp, "headers", None)
        if headers is not None:
            raw = headers.get("Retry-After")
            if raw is None:
                raw = headers.get("retry-after")
        value = int(raw) if raw is not None else 1
        return max(1, value)
    except Exception:
        return 1


def _response_header(resp, name: str) -> Optional[str]:
    try:
        headers = getattr(resp, "headers", None)
        if headers is None:
            return None
        value = headers.get(name)
        if value is None:
            value = headers.get(name.lower())
        return str(value) if value is not None else None
    except Exception:
        return None


def _rate_limit_reset_wait_seconds(resp) -> Optional[int]:
    """Return wait time derived from X-RateLimit-Reset header if available."""
    raw = _response_header(resp, "X-RateLimit-Reset")
    if not raw:
        return None
    try:
        reset_ts = int(raw)
        wait_s = max(1, reset_ts - int(time.time()))
        return wait_s
    except Exception:
        return None


def _is_rate_limited_response(resp) -> bool:
    """Detect GitHub rate limiting across 429 and 403 variants."""
    status = int(getattr(resp, "status", 0) or 0)
    if status == 429:
        return True
    if status != 403:
        return False
    retry_after = _response_header(resp, "Retry-After")
    remaining = _response_header(resp, "X-RateLimit-Remaining")
    reset = _response_header(resp, "X-RateLimit-Reset")
    return bool(retry_after or (remaining == "0" and reset))


async def _github_search_issues_paged(owner: str, token: str, query: str, max_pages: int = 3) -> list:
    """Fetch paginated GitHub Search issues results for an org query."""
    items = []
    page = 1
    while page <= max_pages:
        resp = await github_api(
            "GET",
            f"/search/issues?q={query}+org:{owner}&per_page=100&page={page}",
            token,
        )
        if resp.status != 200:
            break
        data = json.loads(await resp.text())
        page_items = data.get("items", [])
        if not page_items:
            break
        items.extend(page_items)
        if len(page_items) < 100:
            break
        page += 1
    return items


async def _fetch_issue_comments_paged(owner: str, repo: str, issue_number: int, token: str, sort_query: str = "") -> tuple[list, bool]:
    """Fetch all issue comments with pagination; returns (comments, list_failed)."""
    comments = []
    page = 1
    list_failed = False
    while True:
        sep = "&" if sort_query else ""
        resp = await github_api(
            "GET",
            f"/repos/{owner}/{repo}/issues/{issue_number}/comments?per_page=100{sep}{sort_query}&page={page}",
            token,
        )
        if resp.status != 200:
            list_failed = True
            break
        page_comments = json.loads(await resp.text())
        comments.extend(page_comments)
        if len(page_comments) < 100:
            break
        page += 1
    return comments, list_failed


def _pr_state_upsert_sql() -> str:
    return """
        INSERT INTO leaderboard_pr_state (org, repo, pr_number, author_login, state, merged, closed_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(org, repo, pr_number) DO UPDATE SET
            author_login = excluded.author_login,
            state = excluded.state,
            merged = excluded.merged,
            closed_at = excluded.closed_at,
            updated_at = excluded.updated_at
    """


async def _d1_upsert_pr_state(
    db,
    org: str,
    repo: str,
    pr_number: int,
    author_login: str,
    state: str,
    merged: int,
    closed_at: Optional[int],
    updated_at: int,
) -> None:
    await _d1_run(
        db,
        _pr_state_upsert_sql(),
        (org, repo, pr_number, author_login, state, int(merged), closed_at, updated_at),
    )


async def get_installation_token(
    installation_id: int, app_id: str, private_key: str
) -> Optional[str]:
    """Exchange a GitHub App JWT for an installation access token."""
    jwt = await create_github_jwt(app_id, private_key)
    resp = await fetch(
        f"https://api.github.com/app/installations/{installation_id}/access_tokens",
        method="POST",
        headers=Headers.new({
            "Authorization": f"Bearer {jwt}",
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
            "User-Agent": "BLT-Pool/1.0",
            "X-GitHub-Api-Version": "2022-11-28",
        }.items()),
    )
    if resp.status != 201:
        console.error(f"[BLT] Failed to get installation token: {resp.status}")
        return None
    data = json.loads(await resp.text())
    return data.get("token")


async def get_installation_access_token(installation_id: int, jwt_token: str) -> Optional[str]:
    """Exchange a prebuilt GitHub App JWT for an installation access token."""
    resp = await fetch(
        f"https://api.github.com/app/installations/{installation_id}/access_tokens",
        method="POST",
        headers=Headers.new({
            "Authorization": f"Bearer {jwt_token}",
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
            "User-Agent": "BLT-Pool/1.0",
            "X-GitHub-Api-Version": "2022-11-28",
        }.items()),
    )
    if resp.status != 201:
        console.error(f"[BLT] Failed to get installation access token: {resp.status}")
        return None
    data = json.loads(await resp.text())
    return data.get("token")


async def create_comment(
    owner: str, repo: str, number: int, body: str, token: str, raise_on_error: bool = False
) -> bool:
    """Post a comment on a GitHub issue or pull request."""
    resp = await github_api(
        "POST",
        f"/repos/{owner}/{repo}/issues/{number}/comments",
        token,
        {"body": body},
    )
    if resp.status not in (200, 201):
        try:
            err_text = await resp.text()
        except Exception:
            err_text = "<no response body>"
        console.error(
            f"[GitHub] Failed to create comment on {owner}/{repo}#{number}: "
            f"status={resp.status} body={err_text[:300]}"
        )
        if raise_on_error:
            raise RuntimeError(
                f"create_comment failed for {owner}/{repo}#{number} status={resp.status}"
            )
        return False
    return True


async def _create_comment_best_effort(owner: str, repo: str, number: int, body: str, token: str) -> bool:
    """Post a comment in best-effort mode and retain explicit success/failure semantics."""
    try:
        return await create_comment(owner, repo, number, body, token)
    except Exception as exc:
        console.error(
            f"[GitHub] Best-effort comment failed for {owner}/{repo}#{number}: {exc}"
        )
        return False


async def _create_comment_strict(owner: str, repo: str, number: int, body: str, token: str) -> bool:
    """Create a comment and raise on failure, with test-double compatibility."""
    try:
        return await create_comment(owner, repo, number, body, token, raise_on_error=True)
    except TypeError as exc:
        # Some tests patch create_comment with a 5-arg lambda. Retry without the
        # optional kwarg so strict call-sites remain compatible with those doubles.
        if "raise_on_error" in str(exc):
            result = await create_comment(owner, repo, number, body, token)
            if result is False:
                raise RuntimeError(f"create_comment failed for {owner}/{repo}#{number}")
            return True
        raise


async def create_reaction(
    owner: str, repo: str, comment_id: int, reaction: str, token: str
) -> None:
    """Add a reaction to a comment. Common reactions: +1, -1, laugh, confused, heart, hooray, rocket, eyes."""
    resp = await github_api(
        "POST",
        f"/repos/{owner}/{repo}/issues/comments/{comment_id}/reactions",
        token,
        {"content": reaction},
    )
    if resp.status not in (200, 201):
        try:
            err_text = await resp.text()
        except Exception:
            err_text = "<no response body>"
        console.error(
            f"[GitHub] Failed to create reaction on {owner}/{repo} comment={comment_id}: "
            f"status={resp.status} body={err_text[:300]}"
        )


# ---------------------------------------------------------------------------
# BLT API helper
# ---------------------------------------------------------------------------


async def report_bug_to_blt(blt_api_url: str, issue_data: dict):
    """Report a bug to the BLT API; returns the created bug object or None."""
    try:
        payload = {
            "url": issue_data.get("url") or issue_data.get("github_url"),
            "description": issue_data.get("description", ""),
            "github_url": issue_data.get("github_url", ""),
            "label": issue_data.get("label", "general"),
            "status": "open",
        }
        resp = await fetch(
            f"{blt_api_url}/bugs",
            method="POST",
            headers=Headers.new({"Content-Type": "application/json"}.items()),
            body=json.dumps(payload),
        )
        data = json.loads(await resp.text())
        return data.get("data") if data.get("success") else None
    except Exception as exc:
        console.error(f"[BLT] Failed to report bug: {exc}")
        return None


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------


def _is_human(user: dict) -> bool:
    """Return True for human GitHub users (not bots or apps).

    'Mannequin' is a placeholder user type GitHub assigns to contributions
    imported from external version-control systems (e.g. SVN migrations).
    """
    return bool(user and user.get("type") in ("User", "Mannequin"))


def _is_bot(user: dict) -> bool:
    """Return True if the user is a bot account.
    
    Returns True for None or malformed user objects to safely filter them out.
    """
    if not user or not user.get("login"):
        return True  # Treat invalid/missing users as bots for safety
    login_lower = user["login"].lower()
    bot_patterns = [
        "copilot", "[bot]", "dependabot", "github-actions",
        "renovate", "actions-user", "coderabbitai", "coderabbit",
        "sentry-autofix"
    ]
    return user.get("type") == "Bot" or any(p in login_lower for p in bot_patterns)


def _is_coderabbit_ping(body: str) -> bool:
    """Return True if the comment body mentions coderabbit."""
    if not body:
        return False
    lower = body.lower()
    return "coderabbit" in lower or "@coderabbitai" in lower


async def _is_maintainer(owner: str, repo: str, login: str, token: str) -> bool:
    """Return True if ``login`` has admin or maintain permission in the repo.

    Uses the GitHub collaborator permission endpoint.  Returns False on any
    API error (fail-closed).
    """
    try:
        resp = await github_api(
            "GET",
            f"/repos/{owner}/{repo}/collaborators/{login}/permission",
            token,
        )
        if resp.status != 200:
            return False
        data = json.loads(await resp.text())
        return data.get("permission", "") in ("admin", "maintain")
    except Exception:
        return False


def _extract_command(body: str) -> Optional[str]:
    """Extract a supported slash command from comment body (case-insensitive)."""
    if not body:
        return None
    tokens = body.strip().split()
    if not tokens:
        return None
    supported = {
        ASSIGN_COMMAND,
        UNASSIGN_COMMAND,
        LEADERBOARD_COMMAND,
        MENTOR_COMMAND,
        UNMENTOR_COMMAND,
        MENTOR_PAUSE_COMMAND,
        HANDOFF_COMMAND,
        REMATCH_COMMAND,
    }
    for t in tokens:
        tok = t.strip().lower().rstrip(".,!?:;")
        if tok in supported:
            return tok
    return None



def _parse_github_timestamp(ts_str: str) -> int:
    """Parse GitHub ISO 8601 timestamp to Unix timestamp."""
    # GitHub timestamps are like: 2024-03-05T12:34:56Z
    import re
    match = re.match(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z", ts_str)
    if match:
        year, month, day, hour, minute, second = map(int, match.groups())
        dt = time.struct_time((year, month, day, hour, minute, second, 0, 0, 0))
        return int(calendar.timegm(dt))
    return 0



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

