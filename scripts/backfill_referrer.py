"""
Backfill referred_by data for mentors in the BLT-Pool D1 database.

Closes: https://github.com/OWASP-BLT/BLT-Pool/issues/52

Usage:
    python3 scripts/backfill_referrer.py

Environment variables required:
    CF_ACCOUNT_ID   - Cloudflare account ID
    CF_API_TOKEN    - Cloudflare API token with D1 write access
    CF_D1_DB_ID     - Cloudflare D1 database ID

The referral data below maps each mentor's GitHub username to the
contributor who referred them. Edit REFERRAL_DATA to match your actual data.
"""

import os
import sys
import json
import re
import urllib.request
import urllib.error

REFERRAL_DATA = [
   
    {"github_username": "example-mentor-1", "referred_by": "referrer-a"},
    {"github_username": "example-mentor-2", "referred_by": "referrer-b"},
]


_GH_RE = re.compile(r"^[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,37}[a-zA-Z0-9])?$")


def validate_username(name: str) -> str:
    """Strip leading '@' and validate GitHub username format."""
    name = name.lstrip("@").strip()
    if not _GH_RE.match(name):
        raise ValueError(f"Invalid GitHub username: {name!r}")
    return name


def cf_d1_query(account_id: str, db_id: str, token: str, sql: str, params: list) -> dict:
    """Execute a single SQL statement against Cloudflare D1 via REST API."""
    url = (
        f"https://api.cloudflare.com/client/v4/accounts/{account_id}"
        f"/d1/database/{db_id}/query"
    )
    payload = json.dumps({"sql": sql, "params": params}).encode()
    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def main() -> None:
    account_id = os.environ.get("CF_ACCOUNT_ID", "").strip()
    api_token = os.environ.get("CF_API_TOKEN", "").strip()
    db_id = os.environ.get("CF_D1_DB_ID", "").strip()

    missing = [k for k, v in {
        "CF_ACCOUNT_ID": account_id,
        "CF_API_TOKEN": api_token,
        "CF_D1_DB_ID": db_id,
    }.items() if not v]

    if missing:
        print(f"ERROR: Missing environment variables: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    if not REFERRAL_DATA:
        print("No entries in REFERRAL_DATA — nothing to do.")
        return

    updated, skipped, errors = [], [], []

    for entry in REFERRAL_DATA:
        try:
            mentor = validate_username(entry["github_username"])
            referrer = validate_username(entry["referred_by"])
        except (KeyError, ValueError) as exc:
            errors.append({"entry": entry, "reason": str(exc)})
            continue

        sql = "UPDATE mentors SET referred_by = ? WHERE lower(github_username) = lower(?)"
        try:
            result = cf_d1_query(account_id, db_id, api_token, sql, [referrer, mentor])
        except urllib.error.HTTPError as exc:
            body = exc.read().decode(errors="replace")
            errors.append({"entry": entry, "reason": f"HTTP {exc.code}: {body}"})
            continue
        except Exception as exc:  
            errors.append({"entry": entry, "reason": str(exc)})
            continue

        
        try:
            changes = result["result"][0]["meta"]["changes"]
        except (KeyError, IndexError, TypeError):
            changes = 0

        if changes == 0:
            skipped.append({"github_username": mentor, "reason": "mentor not found in DB"})
        else:
            updated.append({"github_username": mentor, "referred_by": referrer})

    print(f"\n✅ Updated : {len(updated)}")
    for u in updated:
        print(f"   {u['github_username']} ← referred by {u['referred_by']}")

    print(f"\n⏭  Skipped : {len(skipped)}")
    for s in skipped:
        print(f"   {s['github_username']} — {s['reason']}")

    print(f"\n❌ Errors  : {len(errors)}")
    for e in errors:
        print(f"   {e['entry']} — {e['reason']}")

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
