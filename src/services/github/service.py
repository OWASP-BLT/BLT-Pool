"""GitHub and auth helpers for BLT-Pool."""

import base64
import hashlib
import hmac as _hmac
import json
import time
from typing import Optional

try:
    from js import Array, Headers, Object, console, fetch
except ImportError:
    # Fallback for testing outside Cloudflare Workers
    class _MockConsole:
        def log(self, *args, **kwargs): pass
        def error(self, *args, **kwargs): pass
    console = _MockConsole()
    Array = Headers = Object = fetch = None


class GitHubService:
    """Encapsulate GitHub auth, API calls, and common user helpers."""

    _RSA_OID_SEQ = bytes([
        0x30, 0x0D,
        0x06, 0x09, 0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x01,
        0x05, 0x00,
    ])

    def __init__(self, supported_commands: Optional[set[str]] = None):
        self.supported_commands = supported_commands or set()

    def der_len(self, n: int) -> bytes:
        """Encode a DER length field."""
        if n < 0x80:
            return bytes([n])
        if n < 0x100:
            return bytes([0x81, n])
        return bytes([0x82, (n >> 8) & 0xFF, n & 0xFF])

    def wrap_pkcs1_as_pkcs8(self, pkcs1_der: bytes) -> bytes:
        """Wrap a PKCS#1 RSAPrivateKey DER blob into a PKCS#8 PrivateKeyInfo."""
        version = bytes([0x02, 0x01, 0x00])
        octet = bytes([0x04]) + self.der_len(len(pkcs1_der)) + pkcs1_der
        content = version + self._RSA_OID_SEQ + octet
        return bytes([0x30]) + self.der_len(len(content)) + content

    def pem_to_pkcs8_der(self, pem: str) -> bytes:
        """Convert a PEM private key (PKCS#1 or PKCS#8) to PKCS#8 DER bytes."""
        lines = pem.strip().splitlines()
        is_pkcs1 = lines[0].strip() == "-----BEGIN RSA PRIVATE KEY-----"
        b64 = "".join(line for line in lines if not line.startswith("-----"))
        der = base64.b64decode(b64)
        return self.wrap_pkcs1_as_pkcs8(der) if is_pkcs1 else der

    def b64url(self, data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")

    def verify_signature(self, payload: bytes, signature: str, secret: str) -> bool:
        """Return True when the X-Hub-Signature-256 header matches the payload."""
        if not signature or not signature.startswith("sha256="):
            return False
        expected = "sha256=" + _hmac.new(
            secret.encode("utf-8"), payload, hashlib.sha256
        ).hexdigest()
        return _hmac.compare_digest(expected, signature)

    async def create_github_jwt(self, app_id: str, private_key_pem: str) -> str:
        """Create a signed GitHub App JWT using the Web Crypto SubtleCrypto API."""
        from js import Array, Uint8Array, crypto  # noqa: PLC0415
        from pyodide.ffi import to_js  # noqa: PLC0415

        now = int(time.time())
        header_b64 = self.b64url(
            json.dumps({"alg": "RS256", "typ": "JWT"}, separators=(",", ":")).encode()
        )
        payload_b64 = self.b64url(
            json.dumps(
                {"iat": now - 60, "exp": now + 600, "iss": str(app_id)},
                separators=(",", ":"),
            ).encode()
        )
        signing_input = f"{header_b64}.{payload_b64}"

        pkcs8_der = self.pem_to_pkcs8_der(private_key_pem)
        key_array = Uint8Array.new(len(pkcs8_der))
        for i, b in enumerate(pkcs8_der):
            key_array[i] = b

        key_usages = getattr(Array, "from")(["sign"])
        crypto_key = await crypto.subtle.importKey(
            "pkcs8",
            key_array.buffer,
            to_js({"name": "RSASSA-PKCS1-v1_5", "hash": "SHA-256"}, dict_converter=Object.fromEntries),
            False,
            key_usages,
        )

        msg_bytes = signing_input.encode("ascii")
        msg_array = Uint8Array.new(len(msg_bytes))
        for i, b in enumerate(msg_bytes):
            msg_array[i] = b

        sig_buf = await crypto.subtle.sign("RSASSA-PKCS1-v1_5", crypto_key, msg_array.buffer)
        sig_bytes = bytes(Uint8Array.new(sig_buf))
        return f"{signing_input}.{self.b64url(sig_bytes)}"

    def gh_headers(self, token: str) -> Headers:
        headers = {
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
            "User-Agent": "BLT-Pool/1.0",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return Headers.new(headers.items())

    async def github_api(self, method: str, path: str, token: str, body=None):
        """Make an authenticated request to the GitHub REST API."""
        url = f"https://api.github.com{path}"
        kwargs = {"method": method, "headers": self.gh_headers(token)}
        if body is not None:
            kwargs["body"] = json.dumps(body)
        return await fetch(url, **kwargs)

    async def get_installation_token(
        self, installation_id: int, app_id: str, private_key: str
    ) -> Optional[str]:
        """Exchange a GitHub App JWT for an installation access token."""
        jwt = await self.create_github_jwt(app_id, private_key)
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

    async def get_installation_access_token(
        self, installation_id: int, jwt_token: str
    ) -> Optional[str]:
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
        self, owner: str, repo: str, number: int, body: str, token: str
    ) -> None:
        """Post a comment on a GitHub issue or pull request."""
        resp = await self.github_api(
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
        self, owner: str, repo: str, comment_id: int, reaction: str, token: str
    ) -> None:
        """Add a reaction to a comment."""
        resp = await self.github_api(
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

    async def report_bug_to_blt(self, blt_api_url: str, issue_data: dict):
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

    def is_human(self, user: dict) -> bool:
        """Return True for human GitHub users (not bots or apps)."""
        return bool(user and user.get("type") in ("User", "Mannequin"))

    def is_bot(self, user: dict) -> bool:
        """Return True if the user is a bot account."""
        if not user or not user.get("login"):
            return True
        login_lower = user["login"].lower()
        bot_patterns = [
            "copilot", "[bot]", "dependabot", "github-actions",
            "renovate", "actions-user", "coderabbitai", "coderabbit",
            "sentry-autofix",
        ]
        return user.get("type") == "Bot" or any(p in login_lower for p in bot_patterns)

    def is_coderabbit_ping(self, body: str) -> bool:
        """Return True if the comment body mentions coderabbit."""
        if not body:
            return False
        lower = body.lower()
        return "coderabbit" in lower or "@coderabbitai" in lower

    async def is_maintainer(self, owner: str, repo: str, login: str, token: str) -> bool:
        """Return True if ``login`` has admin or maintain permission in the repo."""
        try:
            resp = await self.github_api(
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

    def extract_command(self, body: str) -> Optional[str]:
        """Extract a supported slash command from comment body (case-insensitive)."""
        if not body:
            return None
        tokens = body.strip().split()
        if not tokens:
            return None
        for token in tokens:
            normalized = token.strip().lower().rstrip(".,!?:;")
            if normalized in self.supported_commands:
                return normalized
        return None
