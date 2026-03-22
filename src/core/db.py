import json
import time
import calendar
from typing import Optional, Any, Tuple
from js import console

def _month_key(ts: Optional[int] = None) -> str:
    """Return YYYY-MM month key for UTC timestamp (or now)."""
    if ts is None:
        ts = int(time.time())
    return time.strftime("%Y-%m", time.gmtime(ts))

def _month_window(month_key: str) -> Tuple[int, int]:
    """Return start/end timestamps (UTC) for a YYYY-MM key."""
    year, month = month_key.split("-")
    y = int(year)
    m = int(month)
    start_struct = time.struct_time((y, m, 1, 0, 0, 0, 0, 0, 0))
    start_ts = int(calendar.timegm(start_struct))
    if m == 12:
        next_struct = time.struct_time((y + 1, 1, 1, 0, 0, 0, 0, 0, 0))
    else:
        next_struct = time.struct_time((y, m + 1, 1, 0, 0, 0, 0, 0, 0))
    end_ts = int(calendar.timegm(next_struct)) - 1
    return start_ts, end_ts

def _d1_binding(env):
    """Return D1 binding object if configured, otherwise None."""
    db = getattr(env, "LEADERBOARD_DB", None) if env else None
    return db

async def _d1_run(db, sql: str, params: tuple = ()):
    try:
        stmt = db.prepare(sql)
        if params:
            stmt = stmt.bind(*params)
        result = await stmt.run()
        return result
    except Exception as e:
        console.error(f"[D1.run] Error executing {sql[:60]}: {e}")
        raise

def _to_py(value):
    """Best-effort conversion for JS proxy values returned by Workers runtime."""
    try:
        from pyodide.ffi import to_py  # noqa: PLC0415 - runtime import
        return to_py(value)
    except Exception:
        return value

async def _d1_all(db, sql: str, params: tuple = ()) -> list:
    stmt = db.prepare(sql)
    if params:
        stmt = stmt.bind(*params)
    raw_result = await stmt.all()

    # Cloudflare D1 returns JS proxy objects at runtime; serialize through JS JSON
    # first to reliably convert to Python dict/list structures.
    try:
        from js import JSON as JS_JSON  # noqa: PLC0415 - runtime import
        js_json = JS_JSON.stringify(raw_result)
        parsed = json.loads(str(js_json))
        rows = parsed.get("results") if isinstance(parsed, dict) else None
        if isinstance(rows, list):
            return rows
    except Exception:
        pass

    # Fallback path for local tests or non-JS proxy values.
    result = _to_py(raw_result)
    if isinstance(result, dict) and "results" in result:
        return result["results"]
    elif hasattr(result, "results"):
        return result.results
    
    # If it's already a list and none of the above matches
    if isinstance(result, list):
        return result
        
    return []

async def _d1_first(db, sql: str, params: tuple = ()):
    rows = await _d1_all(db, sql, params)
    return rows[0] if rows else None

def _time_ago(ts: int) -> str:
    """Return a human-readable 'X time ago' string for a Unix timestamp."""
    diff = int(time.time()) - ts
    if diff < 60:
        return "just now"
    if diff < 3600:
        m = diff // 60
        return f"{m} minute{'s' if m != 1 else ''} ago"
    if diff < 86400:
        h = diff // 3600
        return f"{h} hour{'s' if h != 1 else ''} ago"
    if diff < 86400 * 30:
        d = diff // 86400
        return f"{d} day{'s' if d != 1 else ''} ago"
    if diff < 86400 * 365:
        mo = diff // (86400 * 30)
        return f"{mo} month{'s' if mo != 1 else ''} ago"
    y = diff // (86400 * 365)
    return f"{y} year{'s' if y != 1 else ''} ago"
