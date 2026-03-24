"""Async tool runner with timeout, retry, and neutral-safe fallback semantics."""

from __future__ import annotations

import asyncio
import logging
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
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be greater than zero")
    if max_retries < 0:
        raise ValueError("max_retries cannot be negative")
    if retry_delay_seconds < 0:
        raise ValueError("retry_delay_seconds cannot be negative")

    attempts_allowed = max_retries + 1

    for attempt in range(1, attempts_allowed + 1):
        try:
            output = await asyncio.wait_for(runner(), timeout=timeout_seconds)
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
