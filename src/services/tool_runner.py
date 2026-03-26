"""Async tool runner with timeout, retry, and neutral-safe fallback semantics."""

from __future__ import annotations

import asyncio
import logging
import math
import re
from dataclasses import dataclass
from typing import Awaitable, Callable, Literal, Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ToolRunResult:
    """Represents the outcome of a single tool execution workflow."""

    name: str
    status: Literal["success", "timeout", "error"]
    attempt_count: int
    timed_out: bool
    error: Optional[str]
    output: Optional[dict]
    conclusion: Literal["success", "neutral"]


def _default_timeout_summary(name: str, timeout_seconds: float, attempts: int) -> dict:
    return {
        "title": f"{name} timed out",
        "summary": (
            f"Execution exceeded timeout ({timeout_seconds}s) after "
            f"{attempts} attempt(s). Marked neutral for safe fallback."
        ),
    }


def _default_error_summary(name: str, error_message: str, attempts: int) -> dict:
    return {
        "title": f"{name} failed",
        "summary": (
            f"Execution failed after {attempts} attempt(s): {error_message}. "
            "Marked neutral for safe fallback."
        ),
    }


def _sanitize_error_message(message: str, *, max_len: int = 160) -> str:
    """Sanitize exception text before exposing it in checks output or logs."""
    cleaned = " ".join((message or "").split())

    redaction_rules = [
        # Windows and Unix/home path patterns.
        (re.compile(r"(^|[\s=:(\[\{])[A-Za-z]:\\[^\s]+"), r"\1[REDACTED_PATH]"),
        (re.compile(r"(^|[\s=:(\[\{])\/[^\s]+"), r"\1[REDACTED_PATH]"),
        (re.compile(r"(^|[\s=:(\[\{])~\/[^\s]+"), r"\1[REDACTED_PATH]"),
        # Email addresses
        (
            re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
            "[REDACTED_EMAIL]",
        ),
        # Key-value style secrets and token-like identifiers
        (
            re.compile(
                r"(?i)\b(api[_-]?key|token|secret|password)\b\s*[:=]\s*[^\s,;]+"
            ),
            "[REDACTED_SECRET]",
        ),
        (
            re.compile(r"(?i)\b(access_token|id_token|refresh_token)=([^&\s]+)"),
            r"\1=[REDACTED_SECRET]",
        ),
        (re.compile(r"\bgh[pousr]_[A-Za-z0-9_]{20,}\b"), "[REDACTED_SECRET]"),
        (
            re.compile(
                r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}\b"
            ),
            "[REDACTED_SECRET]",
        ),
        (
            re.compile(r"\b[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b"),
            "[REDACTED_SECRET]",
        ),
        (re.compile(r"\b[A-Za-z0-9+/_-]{40,}={0,2}\b"), "[REDACTED_SECRET]"),
        (re.compile(r"\b[A-Fa-f0-9]{32,}\b"), "[REDACTED_SECRET]"),
    ]
    for pattern, replacement in redaction_rules:
        cleaned = pattern.sub(replacement, cleaned)

    if not cleaned:
        return "internal error"
    if len(cleaned) > max_len:
        return f"{cleaned[:max_len]}..."
    return cleaned


async def run_tool_with_retries(
    *,
    name: str,
    runner: Callable[[], Awaitable[dict]],
    timeout_seconds: float,
    max_retries: int = 0,
    retry_delay_seconds: float = 0.0,
) -> ToolRunResult:
    """Run a tool with per-attempt timeout and retry behavior.

    Notes:
    - `max_retries=0` means one attempt total.
    - Timeout and unexpected exceptions are converted to neutral fallback results.
    - Callers can use `conclusion` directly when mapping to Checks API updates.
    """
    if not isinstance(timeout_seconds, (int, float)) or isinstance(timeout_seconds, bool):
        raise ValueError("timeout_seconds must be a finite number")
    if not math.isfinite(timeout_seconds):
        raise ValueError("timeout_seconds must be a finite number")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be greater than zero")
    if not isinstance(max_retries, int) or isinstance(max_retries, bool):
        if isinstance(max_retries, (int, float)) and not isinstance(max_retries, bool):
            if not math.isfinite(max_retries):
                raise ValueError("max_retries must be a finite integer")
        raise ValueError("max_retries must be an integer")
    if max_retries < 0:
        raise ValueError("max_retries cannot be negative")
    if not isinstance(retry_delay_seconds, (int, float)) or isinstance(
        retry_delay_seconds, bool
    ):
        raise ValueError("retry_delay_seconds must be a finite number")
    if not math.isfinite(retry_delay_seconds):
        raise ValueError("retry_delay_seconds must be a finite number")
    if retry_delay_seconds < 0:
        raise ValueError("retry_delay_seconds cannot be negative")

    attempts_allowed = max_retries + 1

    for attempt in range(1, attempts_allowed + 1):
        try:
            output = await asyncio.wait_for(runner(), timeout=timeout_seconds)
            if not isinstance(output, dict):
                output_repr = _sanitize_error_message(repr(output), max_len=300)
                err = _sanitize_error_message(
                    f"invalid runner return type: {type(output).__name__}"
                )
                return ToolRunResult(
                    name=name,
                    status="error",
                    attempt_count=attempt,
                    timed_out=False,
                    error=err,
                    output={
                        "title": f"{name} failed",
                        "summary": (
                            f"Runner returned an invalid payload type ({type(output).__name__}). "
                            "Marked neutral for safe fallback."
                        ),
                        "raw_output_preview": output_repr,
                    },
                    conclusion="neutral",
                )
            return ToolRunResult(
                name=name,
                status="success",
                attempt_count=attempt,
                timed_out=False,
                error=None,
                output=output,
                conclusion="success",
            )
        except (TimeoutError, asyncio.TimeoutError):
            logger.warning(
                "tool-runner: timeout name=%s attempt=%s/%s timeout=%ss",
                name,
                attempt,
                attempts_allowed,
                timeout_seconds,
            )
            if attempt < attempts_allowed:
                if retry_delay_seconds:
                    await asyncio.sleep(retry_delay_seconds)
                continue
            return ToolRunResult(
                name=name,
                status="timeout",
                attempt_count=attempt,
                timed_out=True,
                error=f"timed out after {timeout_seconds}s",
                output=_default_timeout_summary(name, timeout_seconds, attempt),
                conclusion="neutral",
            )
        except Exception as exc:
            err = _sanitize_error_message(str(exc) or exc.__class__.__name__)
            logger.error(
                "tool-runner: error name=%s attempt=%s/%s error_type=%s error=%s",
                name,
                attempt,
                attempts_allowed,
                exc.__class__.__name__,
                err,
            )
            if attempt < attempts_allowed:
                if retry_delay_seconds:
                    await asyncio.sleep(retry_delay_seconds)
                continue
            return ToolRunResult(
                name=name,
                status="error",
                attempt_count=attempt,
                timed_out=False,
                error=err,
                output=_default_error_summary(name, err, attempt),
                conclusion="neutral",
            )

    # Defensive fallback; loop always returns before this point.
    return ToolRunResult(
        name=name,
        status="error",
        attempt_count=attempts_allowed,
        timed_out=False,
        error="unreachable state",
        output=_default_error_summary(name, "unreachable state", attempts_allowed),
        conclusion="neutral",
    )
