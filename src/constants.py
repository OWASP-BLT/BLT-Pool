"""Constants used throughout the BLT-Pool worker."""

from services.mentor_seed import INITIAL_MENTORS

# Backward-compatible alias kept for tests and older imports after mentor seed
# data moved to ``services.mentor_seed``.
_INITIAL_MENTORS = INITIAL_MENTORS

ASSIGN_COMMAND = "/assign"
UNASSIGN_COMMAND = "/unassign"
LEADERBOARD_COMMAND = "/leaderboard"
MAX_ASSIGNEES = 1
ASSIGNMENT_DURATION_HOURS = 8
BUG_LABELS = {"bug", "vulnerability", "security"}
HELP_WANTED_LABEL = "help wanted"
TRIAGE_REVIEWER = "donnieblt"

# ---------------------------------------------------------------------------
# Mentor pool — slash commands and label names
# ---------------------------------------------------------------------------

MENTOR_COMMAND = "/mentor"
UNMENTOR_COMMAND = "/unmentor"
MENTOR_PAUSE_COMMAND = "/mentor-pause"
HANDOFF_COMMAND = "/handoff"
REMATCH_COMMAND = "/rematch"
NEEDS_MENTOR_LABEL = "needs-mentor"
MENTOR_ASSIGNED_LABEL = "mentor-assigned"
MENTOR_MAX_MENTEES = 3
MENTOR_STALE_DAYS = 14
MENTOR_LABEL_COLOR = "7057ff"
MENTOR_ASSIGNED_LABEL_COLOR = "0075ca"
# Issues with these labels bypass mentor auto-assignment (go to core maintainers).
SECURITY_BYPASS_LABELS = {"security", "vulnerability", "security-sensitive", "private-security"}
# Seconds in a day — used for stale-assignment threshold calculations.
_SECONDS_PER_DAY = 86400
# TTL for cached GitHub-sourced all-time mentor stats in D1 (24 hours).
_MENTOR_STATS_CACHE_TTL = 86400
# When True, one active mentor is auto-requested as a reviewer on every newly
# opened PR using a deterministic round-robin order (PR number mod pool size).
# Set to False (default) to keep the existing behaviour of only requesting the
# mentor when the PR explicitly closes a mentored issue.
# This default can also be overridden at runtime by setting the Cloudflare
# Worker environment variable ``MENTOR_AUTO_PR_REVIEWER_ENABLED=true``.
MENTOR_AUTO_PR_REVIEWER_ENABLED = False

# DER OID sequence for rsaEncryption (used when wrapping PKCS#1 → PKCS#8)
_RSA_OID_SEQ = bytes([
    0x30, 0x0D,
    0x06, 0x09, 0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x01,
    0x05, 0x00,
])

# Leaderboard configuration constants
LEADERBOARD_MARKER = "<!-- leaderboard-bot -->"
REVIEWER_LEADERBOARD_MARKER = "<!-- reviewer-leaderboard-bot -->"
MERGED_PR_COMMENT_MARKER = "<!-- merged-pr-comment-bot -->"
MAX_OPEN_PRS_PER_AUTHOR = 50
LEADERBOARD_COMMENT_MARKER = LEADERBOARD_MARKER
