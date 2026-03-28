"""Time and date utilities for BLT-Pool."""

import calendar
import time
from typing import Optional, Tuple


class TimeUtils:
    """Time-related utilities for timestamps, date ranges, and formatting."""

    SECONDS_PER_DAY = 86400

    @staticmethod
    def month_key(ts: Optional[int] = None) -> str:
        """Return YYYY-MM month key for UTC timestamp (or now)."""
        if ts is None:
            ts = int(time.time())
        return time.strftime("%Y-%m", time.gmtime(ts))

    @staticmethod
    def month_window(month_key: str) -> Tuple[int, int]:
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

    @staticmethod
    def time_ago(ts: int) -> str:
        """Return a human-readable 'X time ago' string for a Unix timestamp."""
        diff = int(time.time()) - ts
        if diff < 60:
            return "just now"
        if diff < 3600:
            minutes = diff // 60
            return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
        if diff < 86400:
            hours = diff // 3600
            return f"{hours} hour{'s' if hours != 1 else ''} ago"
        if diff < 86400 * 30:
            days = diff // 86400
            return f"{days} day{'s' if days != 1 else ''} ago"
        if diff < 86400 * 365:
            months = diff // (86400 * 30)
            return f"{months} month{'s' if months != 1 else ''} ago"
        years = diff // (86400 * 365)
        return f"{years} year{'s' if years != 1 else ''} ago"

    @staticmethod
    def parse_github_timestamp(ts_str: str) -> int:
        """Parse GitHub ISO 8601 timestamp to Unix timestamp."""
        try:
            # GitHub returns RFC 3339 format: 2024-01-15T10:30:00Z
            ts_str = ts_str.rstrip("Z")
            if "." in ts_str:
                ts_str = ts_str.split(".")[0]
            parsed = time.strptime(ts_str, "%Y-%m-%dT%H:%M:%S")
            return int(calendar.timegm(parsed))
        except Exception:
            return int(time.time())
