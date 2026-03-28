"""Validators and regex patterns for BLT-Pool."""

import re
from typing import Optional


class Validators:
    """Validation utilities for usernames, specialties, and other inputs."""

    # GitHub username: 1-39 alphanumeric/hyphen characters, cannot start or end with a hyphen.
    GH_USERNAME_RE = re.compile(r"^[a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,37}[a-zA-Z0-9])?$")
    
    # Specialty tag: 1-30 chars; lowercase letters, digits, +, #, dot, hyphen allowed.
    SPECIALTY_RE = re.compile(r"^[a-z0-9][a-z0-9+#.\-]{0,29}$")
    
    # Display name: 1-100 printable characters, no HTML angle brackets, ampersands,
    # double quotes, or ASCII control characters (prevents script injection).
    NAME_RE = re.compile(r"^[^<>&\"\x00-\x1f]{1,100}$")
    
    # Timezone: optional free-form label, same restrictions as name but max 60 chars.
    TIMEZONE_RE = re.compile(r"^[^<>&\"\x00-\x1f]{1,60}$")

    # Bounds for the max_mentees field in the mentor form.
    MENTOR_MIN_MENTEES_CAP = 1
    MENTOR_MAX_MENTEES_CAP = 10

    @classmethod
    def validate_github_username(cls, username: str) -> bool:
        """Validate GitHub username format."""
        return bool(cls.GH_USERNAME_RE.match(username))

    @classmethod
    def validate_specialty(cls, specialty: str) -> bool:
        """Validate specialty tag format."""
        return bool(cls.SPECIALTY_RE.match(specialty))

    @classmethod
    def validate_name(cls, name: str) -> bool:
        """Validate display name format."""
        return bool(cls.NAME_RE.match(name))

    @classmethod
    def validate_timezone(cls, timezone: str) -> bool:
        """Validate timezone format."""
        return bool(cls.TIMEZONE_RE.match(timezone)) if timezone else True

    @classmethod
    def validate_max_mentees(cls, max_mentees: int) -> bool:
        """Validate max_mentees value."""
        return cls.MENTOR_MIN_MENTEES_CAP <= max_mentees <= cls.MENTOR_MAX_MENTEES_CAP

    @staticmethod
    def parse_yaml_scalar(s: str):
        """Parse a simple YAML scalar value (string, int, float, bool, null)."""
        if s in ("true", "True", "yes", "Yes", "on", "On"):
            return True
        if s in ("false", "False", "no", "No", "off", "Off"):
            return False
        if s in ("null", "Null", "~", ""):
            return None
        # Try numeric
        try:
            if "." in s or "e" in s or "E" in s:
                return float(s)
            return int(s)
        except ValueError:
            pass
        # Return as string (strip quotes if present)
        if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
            return s[1:-1]
        return s
