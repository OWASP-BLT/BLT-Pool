"""core/github_client.py — GitHub REST API wrapper for BLT-Pool.

Provides:
- Authenticated HTTP helpers (github_api, _gh_headers)
- Installation token exchange
- Comment / reaction creation
- BLT bug reporting
- User-type predicates (_is_human, _is_bot, _is_maintainer, …)
- Slash-command extraction (_extract_command)
"""

import json
from typing import Optional
from urllib.parse import quote

from js import Headers, console, fetch  # Cloudflare Workers JS bindings

from core.crypto import create_github_jwt

# ---------------------------------------------------------------------------
# Slash-command constants (imported by controllers that need them)
# ---------------------------------------------------------------------------

ASSIGN_COMMAND = "/assign"
UNASSIGN_COMMAND = "/unassign"
APPROVE_COMMAND = "/approve"
DENY_COMMAND = "/deny"
LEADERBOARD_COMMAND = "/leaderboard"
MENTOR_COMMAND = "/mentor"
UNMENTOR_COMMAND = "/unmentor"
MENTOR_PAUSE_COMMAND = "/mentor-pause"
HANDOFF_COMMAND = "/handoff"
REMATCH_COMMAND = "/rematch"


# ---------------------------------------------------------------------------
# Low-level HTTP helpers
# ---------------------------------------------------------------------------


def _gh_headers(token: str) -> Headers:
    pairs = [
        ["Accept", "application/vnd.github+json"],
        ["Content-Type", "application/json"],
        ["User-Agent", "BLT-Pool/1.0"],
        ["X-GitHub-Api-Version", "2022-11-28"],
    ]
    if token:
        pairs.append(["Authorization", f"Bearer {token}"])
    return Headers.new(pairs)


async def github_api(method: str, path: str, token: str, body=None):
    """Make an authenticated request to the GitHub REST API."""
    url = f"https://api.github.com{path}"
    kwargs = {"method": method, "headers": _gh_headers(token)}
    if body is not None:
        kwargs["body"] = json.dumps(body)
    return await fetch(url, **kwargs)


# ---------------------------------------------------------------------------
# Installation token exchange
# ---------------------------------------------------------------------------


async def get_installation_token(
    installation_id: int, app_id: str, private_key: str
) -> Optional[str]:
    """Exchange a GitHub App JWT for an installation access token."""
    jwt = await create_github_jwt(app_id, private_key)
    resp = await fetch(
        f"https://api.github.com/app/installations/{installation_id}/access_tokens",
        method="POST",
        headers=Headers.new([
            ["Authorization", f"Bearer {jwt}"],
            ["Accept", "application/vnd.github+json"],
            ["Content-Type", "application/json"],
            ["User-Agent", "BLT-Pool/1.0"],
            ["X-GitHub-Api-Version", "2022-11-28"],
        ]),
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
        headers=Headers.new([
            ["Authorization", f"Bearer {jwt_token}"],
            ["Accept", "application/vnd.github+json"],
            ["Content-Type", "application/json"],
            ["User-Agent", "BLT-Pool/1.0"],
            ["X-GitHub-Api-Version", "2022-11-28"],
        ]),
    )
    if resp.status != 201:
        console.error(f"[BLT] Failed to get installation access token: {resp.status}")
        return None
    data = json.loads(await resp.text())
    return data.get("token")


# ---------------------------------------------------------------------------
# Comment / reaction helpers
# ---------------------------------------------------------------------------


async def create_comment(
    owner: str, repo: str, number: int, body: str, token: str
) -> None:
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
            headers=Headers.new([["Content-Type", "application/json"]]),
            body=json.dumps(payload),
        )
        data = json.loads(await resp.text())
        return data.get("data") if data.get("success") else None
    except Exception as exc:
        console.error(f"[BLT] Failed to report bug: {exc}")
        return None


# ---------------------------------------------------------------------------
# User-type predicates
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


# ---------------------------------------------------------------------------
# Slash-command extraction
# ---------------------------------------------------------------------------


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
        APPROVE_COMMAND,
        DENY_COMMAND,
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

async def _ensure_label_exists(
    owner: str, repo: str, name: str, color: str, token: str
) -> None:
    """Create a label if it does not already exist, or update its colour."""
    resp = await github_api(
        "GET",
        f"/repos/{owner}/{repo}/labels/{quote(name, safe='')}",
        token,
    )
    if resp is None or resp.status == 404:
        await github_api(
            "POST",
            f"/repos/{owner}/{repo}/labels",
            token,
            {"name": name, "color": color},
        )
    elif resp.status == 200:
        data = json.loads(await resp.text())
        if data.get("color") != color:
            await github_api(
                "PATCH",
                f"/repos/{owner}/{repo}/labels/{quote(name, safe='')}",
                token,
                {"color": color},
            )

