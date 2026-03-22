import json
import time
from urllib.parse import quote
import re
from typing import Optional

from js import console

from core.db import _d1_all, _d1_binding, _d1_run
from core.github_client import github_api, create_comment, _is_human, _is_bot
from services.mentor_seed import INITIAL_MENTORS
import calendar
import time

def _parse_github_timestamp(ts_str: str) -> int:
    if not ts_str:
        return 0
    try:
        if ts_str.endswith("Z"):
            ts_str = ts_str[:-1]
        dt = time.strptime(ts_str, "%Y-%m-%dT%H:%M:%S")
        return calendar.timegm(dt)
    except Exception:
        return 0
from models.assignment import _d1_get_mentor_loads
from models.leaderboard import _ensure_leaderboard_schema

MENTOR_ASSIGNED_LABEL = "mentor-assigned"
MENTOR_ASSIGNED_LABEL_COLOR = "0075ca"
NEEDS_MENTOR_LABEL = "needs-mentor"
MENTOR_LABEL_COLOR = "7057ff"
MENTOR_MAX_MENTEES = 3
SECURITY_BYPASS_LABELS = {"security", "vulnerability", "security-sensitive", "private-security"}
_MENTOR_STATS_CACHE_TTL = 86400

_SECONDS_PER_DAY = 86400
MENTOR_STALE_DAYS = 14

_GH_USERNAME_RE = re.compile(r"^[a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,37}[a-zA-Z0-9])?$")
_SPECIALTY_RE = re.compile(r"^[a-z0-9][a-z0-9+#.\-]{0,29}$")
_NAME_RE = re.compile(r"^[^<>&\"\x00-\x1f]{1,100}$")
_TIMEZONE_RE = re.compile(r"^[^<>&\"\x00-\x1f]{1,60}$")
_MENTOR_MIN_MENTEES_CAP = 1
_MENTOR_MAX_MENTEES_CAP = 10


async def _populate_mentors_table(db) -> None:
    """Seed the mentors table with the initial mentor list (idempotent).

    Uses INSERT OR IGNORE so that existing rows are never overwritten; safe
    to call on every cold start.
    """
    for m in INITIAL_MENTORS:
        try:
            await _d1_run(
                db,
                """
                INSERT OR IGNORE INTO mentors
                    (github_username, name, specialties, max_mentees, active, timezone, referred_by)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    m["github_username"],
                    m["name"],
                    json.dumps(m.get("specialties") or []),
                    m.get("max_mentees", 3),
                    1 if m.get("active", True) else 0,
                    m.get("timezone", "") or "",
                    m.get("referred_by", "") or "",
                ),
            )
        except Exception as exc:
            console.error(f"[MentorPool] Failed to seed mentor {m['github_username']}: {exc}")

async def _load_mentors_from_d1(db) -> list:
    """Load the mentor list from the D1 ``mentors`` table.

    Returns a list of mentor dicts compatible with the rest of the codebase
    (same keys as the old YAML format).  Returns ``[]`` on error.
    """
    try:
        await _ensure_leaderboard_schema(db)
        rows = await _d1_all(
            db,
            "SELECT github_username, name, specialties, max_mentees, active, timezone, referred_by FROM mentors",
        )
        mentors = []
        for row in rows:
            try:
                specialties = json.loads(row.get("specialties") or "[]")
            except Exception:
                specialties = []
            mentors.append({
                "github_username": row["github_username"],
                "name": row["name"],
                "specialties": specialties,
                "max_mentees": int(row.get("max_mentees") or 3),
                "active": bool(row.get("active", 1)),
                "timezone": row.get("timezone") or "",
                "referred_by": row.get("referred_by") or "",
            })
        console.log(f"[MentorPool] Loaded {len(mentors)} mentors from D1")
        return mentors
    except Exception as exc:
        import traceback; traceback.print_exc()
        console.error(f"[MentorPool] Failed to load mentors from D1: {exc}")
        return []

async def _d1_add_mentor(
    db,
    github_username: str,
    name: str,
    specialties: list,
    max_mentees: int = 3,
    active: bool = True,
    timezone: str = "",
    referred_by: str = "",
) -> None:
    """Insert or replace a mentor row in the D1 ``mentors`` table."""
    await _d1_run(
        db,
        """
        INSERT INTO mentors
            (github_username, name, specialties, max_mentees, active, timezone, referred_by)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(github_username) DO UPDATE SET
            name        = excluded.name,
            specialties = excluded.specialties,
            max_mentees = excluded.max_mentees,
            active      = excluded.active,
            timezone    = excluded.timezone,
            referred_by = excluded.referred_by
        """,
        (
            github_username,
            name,
            json.dumps(specialties),
            max_mentees,
            1 if active else 0,
            timezone or "",
            referred_by or "",
        ),
    )

def _parse_yaml_scalar(s: str):
    """Convert a YAML scalar string to an appropriate Python value."""
    if s.lower() in ("true", "yes", "on"):
        return True
    if s.lower() in ("false", "no", "off"):
        return False
    if s.lower() in ("null", "~", ""):
        return None
    try:
        return int(s)
    except ValueError:
        pass
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        return s[1:-1]
    return s

def _parse_mentors_yaml(content: str) -> list:
    """Parse a simple mentors YAML file into a list of mentor dicts.

    Handles the specific format used in ``src/mentors.yml``:

    .. code-block:: yaml

        mentors:
          - github_username: alice
            name: Alice Smith
            specialties:
              - frontend
            max_mentees: 3
            active: true
    """
    mentors: list = []
    current: Optional[dict] = None
    current_list_key: Optional[str] = None

    for raw_line in content.splitlines():
        line = raw_line.rstrip()
        if not line.strip() or line.strip().startswith("#"):
            continue
        indent = len(line) - len(line.lstrip())
        stripped = line.strip()

        if stripped == "mentors:":
            continue

        if stripped.startswith("- ") and indent == 2:
            # New mentor entry
            if current is not None:
                mentors.append(current)
            current = {}
            current_list_key = None
            kv = stripped[2:]
            if ":" in kv:
                k, _, v = kv.partition(":")
                current[k.strip()] = _parse_yaml_scalar(v.strip())
        elif stripped.startswith("- ") and indent >= 6 and current is not None and current_list_key:
            # List item (e.g. a specialty entry)
            current[current_list_key].append(stripped[2:].strip())
        elif ":" in stripped and not stripped.startswith("-") and current is not None:
            k, _, v = stripped.partition(":")
            k = k.strip()
            v = v.strip()
            if v == "":
                current_list_key = k
                current[k] = []
            else:
                current_list_key = None
                current[k] = _parse_yaml_scalar(v)

    if current is not None:
        mentors.append(current)

    return mentors

async def _fetch_mentors_config(env=None, owner: str = "", repo: str = "", token: str = "") -> list:
    """Load the mentor list, preferring the D1 database when available.

    Falls back to an empty list when D1 is unavailable.  The ``owner``,
    ``repo``, and ``token`` parameters are retained for call-site compatibility
    but are no longer used — mentors are stored in and served from D1.
    """
    db = _d1_binding(env) if env is not None else None
    if db:
        mentors = await _load_mentors_from_d1(db)
        if mentors:
            return mentors
            
    mentors = await _load_mentors_local(env)
    if mentors:
        return mentors
        
    console.error("[MentorPool] No D1 binding or empty mentors table; returning []")
    return []

async def _load_mentors_local(env=None) -> list:
    """Load the mentor list from D1 or fallback to INITIAL_MENTORS."""
    db = _d1_binding(env) if env is not None else None
    if db:
        mentors = await _load_mentors_from_d1(db)
        if mentors:
            return mentors
    return list(INITIAL_MENTORS)

async def _fetch_mentor_stats_from_d1(env, org: str, mentors: Optional[list] = None, token: str = "") -> dict:
    """Return per-mentor all-time PR/review totals for homepage display.

    When ``mentors`` and ``token`` are provided, fetches accurate all-time
    counts directly from the GitHub Search API (using ``total_count``), caching
    results in the ``mentor_stats_cache`` D1 table with a 24-hour TTL.  This
    reflects the full lifespan of the organisation rather than only the period
    since the webhook was first deployed.

    Falls back to aggregating ``leaderboard_monthly_stats`` across all months
    when no GitHub token is available or D1 is not configured.

    Returns a mapping of ``github_username → {"merged_prs": int, "reviews": int}``.
    Returns ``{}`` when D1 is unavailable and the GitHub API path is not used.
    """
    db = _d1_binding(env)

    # ------------------------------------------------------------------
    # Path A: GitHub Search API with D1 cache (accurate all-time counts).
    # ------------------------------------------------------------------
    if mentors and token and db:
        try:
            await _ensure_leaderboard_schema(db)
            now_ts = int(time.time())
            fresh_cutoff = now_ts - _MENTOR_STATS_CACHE_TTL

            # Load all cached stats for this org in one query.
            cached_rows = await _d1_all(
                db,
                "SELECT github_username, merged_prs, reviews, fetched_at FROM mentor_stats_cache WHERE org = ?",
                (org,),
            )
            cache = {
                row["github_username"]: row
                for row in (cached_rows or [])
                if row.get("github_username")
            }

            stats: dict = {}
            for mentor in mentors:
                username = mentor.get("github_username", "")
                if not username:
                    continue
                cached = cache.get(username)
                if cached and int(cached.get("fetched_at") or 0) >= fresh_cutoff:
                    # Cache hit — return stored values directly.
                    stats[username] = {
                        "merged_prs": int(cached.get("merged_prs") or 0),
                        "reviews": int(cached.get("reviews") or 0),
                    }
                    continue

                # Cache miss or stale — fetch from GitHub Search API.
                merged_prs = 0
                reviews = 0
                safe_org = quote(org, safe="")
                safe_user = quote(username, safe="")
                try:
                    pr_resp = await github_api(
                        "GET",
                        f"/search/issues?q=is:pr+is:merged+org:{safe_org}+author:{safe_user}&per_page=1",
                        token,
                    )
                    if pr_resp.status == 200:
                        pr_data = json.loads(await pr_resp.text())
                        merged_prs = int(pr_data.get("total_count") or 0)
                except Exception as exc:
                    console.error(f"[MentorPool] GitHub PR count failed for {username}: {exc}")
                try:
                    rev_resp = await github_api(
                        "GET",
                        f"/search/issues?q=is:pr+org:{safe_org}+reviewed-by:{safe_user}+-author:{safe_user}&per_page=1",
                        token,
                    )
                    if rev_resp.status == 200:
                        rev_data = json.loads(await rev_resp.text())
                        reviews = int(rev_data.get("total_count") or 0)
                except Exception as exc:
                    console.error(f"[MentorPool] GitHub review count failed for {username}: {exc}")

                stats[username] = {"merged_prs": merged_prs, "reviews": reviews}

                # Persist into cache for future requests.
                try:
                    await _d1_run(
                        db,
                        """
                        INSERT INTO mentor_stats_cache (org, github_username, merged_prs, reviews, fetched_at)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(org, github_username) DO UPDATE SET
                            merged_prs = excluded.merged_prs,
                            reviews    = excluded.reviews,
                            fetched_at = excluded.fetched_at
                        """,
                        (org, username, merged_prs, reviews, now_ts),
                    )
                except Exception as exc:
                    console.error(f"[MentorPool] Failed to cache mentor stats for {username}: {exc}")

            return stats
        except Exception as exc:
            import traceback; traceback.print_exc()
            console.error(f"[MentorPool] Failed to fetch mentor stats from GitHub: {exc}")
            # Fall through to the D1 monthly-aggregation path below.

    # ------------------------------------------------------------------
    # Path B: Aggregate from D1 monthly stats (fallback / no token).
    # ------------------------------------------------------------------
    if not db:
        console.log("[MentorPool] No D1 binding available for mentor stats; stats will be hidden")
        return {}
    try:
        await _ensure_leaderboard_schema(db)
        rows = await _d1_all(
            db,
            """
            SELECT user_login,
                   COALESCE(SUM(merged_prs), 0) AS total_prs,
                   COALESCE(SUM(reviews),    0) AS total_reviews
            FROM leaderboard_monthly_stats
            WHERE org = ?
            GROUP BY user_login
            """,
            (org,),
        )
        return {
            row["user_login"]: {
                "merged_prs": int(row.get("total_prs") or 0),
                "reviews": int(row.get("total_reviews") or 0),
            }
            for row in rows
            if row.get("user_login")
        }
    except Exception as exc:
        import traceback; traceback.print_exc()
        console.error(f"[MentorPool] Failed to fetch mentor stats from D1: {exc}")
        return {}

async def _get_mentor_load_map(owner: str, token: str, env=None) -> dict:
    """Return a mapping of mentor_username → open mentored issue count.

    Tries D1 first (``mentor_assignments`` table) when a D1 binding is
    available; falls back to the GitHub Search API for compatibility with
    environments where D1 is not configured.
    """
    db = _d1_binding(env)
    if db:
        try:
            await _ensure_leaderboard_schema(db)
            d1_loads = await _d1_get_mentor_loads(db, owner)
            # d1_loads is a dict (possibly empty when no assignments exist); always use
            # D1 when available — an empty dict is a valid state (no active assignments).
            console.log(f"[MentorPool] Using D1 mentor loads for {owner}: {len(d1_loads)} entries")
            return d1_loads
        except Exception as exc:
            console.error(f"[MentorPool] D1 mentor load lookup failed, falling back to GitHub API: {exc}")

    # ---------------------------------------------------------------------------
    # Fallback: query GitHub Search API (original behaviour).
    # ---------------------------------------------------------------------------
    # Limit pagination to avoid excessive subrequests.
    max_pages = 5
    per_page = 100
    load_map: dict = {}

    for page in range(1, max_pages + 1):
        resp = await github_api(
            "GET",
            f"/search/issues?q=org:{owner}+is:issue+is:open+label:{MENTOR_ASSIGNED_LABEL}"
            f"&per_page={per_page}&page={page}",
            token,
        )
        if resp.status != 200:
            if page == 1:
                console.log(
                    f"[MentorPool] _get_mentor_load_map: GitHub search API returned {resp.status} "
                    f"on page {page} — returning empty load map (all mentors appear at zero load)."
                )
                return {}
            console.log(
                f"[MentorPool] _get_mentor_load_map: GitHub search API returned {resp.status} "
                f"on page {page} — using load counts collected so far."
            )
            break

        data = json.loads(await resp.text())
        items = data.get("items", [])
        if not items:
            break

        for item in items:
            for assignee in item.get("assignees", []):
                login = assignee.get("login", "")
                if login:
                    load_map[login] = load_map.get(login, 0) + 1

        if len(items) < per_page:
            break

    return load_map

async def _select_mentor(
    owner: str,
    token: str,
    issue_labels: Optional[list] = None,
    mentors_config: Optional[list] = None,
    exclude: Optional[str] = None,
    env=None,
) -> Optional[dict]:
    """Select the best available mentor using capacity-aware round-robin.

    The algorithm:
    1. Filter to active mentors with a GitHub username (optionally excluding one).
    2. If the issue has labels that match any mentor's specialties, prefer those mentors.
    3. Fetch the current load map (D1 if available, GitHub Search API otherwise).
    4. Skip mentors who are at or over their ``max_mentees`` cap.
    5. Return the mentor with the fewest active issues; break ties alphabetically.

    Returns ``None`` when no mentor is available.
    """
    pool = mentors_config if mentors_config is not None else []
    active = [
        m for m in pool
        if m.get("active", True)
        and m.get("github_username")
        and (exclude is None or m["github_username"].lower() != exclude.lower())
    ]
    if not active:
        return None

    # Specialty matching: narrow to mentors who match the issue's labels.
    if issue_labels:
        label_set = {lb.lower() for lb in issue_labels}
        specialty_matched = [
            m for m in active
            if any(s.lower() in label_set for s in m.get("specialties", []))
        ]
        if specialty_matched:
            active = specialty_matched

    load_map = await _get_mentor_load_map(owner, token, env=env)

    # Normalize load_map keys to lowercase: GitHub usernames are case-insensitive
    # but config entries and API responses may differ in casing.
    normalized_load = {k.lower(): v for k, v in load_map.items()}

    # Build candidates filtered by capacity.
    candidates = []
    for m in active:
        username = m["github_username"]
        load = normalized_load.get(username.lower(), 0)
        cap = m.get("max_mentees", MENTOR_MAX_MENTEES)
        if load < cap:
            candidates.append((m, load))

    if not candidates:
        return None

    # Pick mentor with fewest active issues; break ties alphabetically.
    candidates.sort(key=lambda x: (x[1], x[0]["github_username"].lower()))
    return candidates[0][0]

async def _find_assigned_mentor_from_comments(
    owner: str, repo: str, issue_number: int, token: str
) -> Optional[str]:
    """Scan issue comments for the ``blt-mentor-assigned`` hidden marker.

    Paginates through all comments (100 per page) so the marker is found even
    on issues with many comments.  Returns the mentor's GitHub username from the
    most recent marker found, or ``None`` if no marker exists.
    """
    marker = "<!-- blt-mentor-assigned:"
    per_page = 100
    page = 1
    last_mentor: Optional[str] = None
    while True:
        resp = await github_api(
            "GET",
            f"/repos/{owner}/{repo}/issues/{issue_number}/comments"
            f"?per_page={per_page}&page={page}",
            token,
        )
        if resp.status != 200:
            return None
        comments = json.loads(await resp.text())
        if not comments:
            break
        # Iterate in forward order, tracking the last match so the most recent
        # assignment marker wins without needing to reverse the full list.
        for comment in comments:
            body = comment.get("body", "")
            if marker in body:
                start = body.find(marker) + len(marker)
                end = body.find("-->", start)
                if end > start:
                    last_mentor = body[start:end].strip().lstrip("@")
        if len(comments) < per_page:
            break
        page += 1
    return last_mentor

async def _get_last_human_activity_ts(
    owner: str, repo: str, issue_number: int, issue: dict, token: str
) -> float:
    """Return the timestamp (epoch seconds) of the most recent non-bot activity.

    Fetches the most recently created page of issue comments and returns the
    timestamp of the latest comment posted by a non-bot human.  If no human
    comments are found the issue's ``created_at`` value is used as a fallback so
    that newly opened issues without any comments are still eligible for stale
    checks after ``MENTOR_STALE_DAYS`` days.
    """
    fallback = _parse_github_timestamp(issue.get("created_at", "")) or 0.0

    resp = await github_api(
        "GET",
        f"/repos/{owner}/{repo}/issues/{issue_number}/comments"
        f"?sort=created&direction=desc&per_page=100",
        token,
    )
    if resp.status != 200:
        return fallback

    comments = json.loads(await resp.text())
    for comment in comments:
        user = comment.get("user") or {}
        if _is_human(user) and not _is_bot(user):
            ts = _parse_github_timestamp(comment.get("created_at", ""))
            if ts:
                return ts

    return fallback

def _is_security_issue(issue: dict) -> bool:
    """Return ``True`` if the issue carries any security-sensitive label."""
    labels = {lb.get("name", "").lower() for lb in issue.get("labels", [])}
    return bool(labels & SECURITY_BYPASS_LABELS)

