"""Unit tests for src/services/tool_runner.py."""

import asyncio
import pytest

from services.tool_runner import run_tool_with_retries


def _run(coro):
    return asyncio.run(coro)


def test_run_tool_with_retries_success_first_attempt():
    async def runner():
        return {"title": "ok", "summary": "done"}

    result = _run(
        run_tool_with_retries(
            name="Gitleaks",
            runner=runner,
            timeout_seconds=1,
            max_retries=2,
        )
    )

    assert result.status == "success"
    assert result.attempt_count == 1
    assert result.conclusion == "success"
    assert result.output["title"] == "ok"


def test_run_tool_with_retries_timeout_neutral_fallback():
    async def runner():
        await asyncio.sleep(0.2)
        return {"title": "late", "summary": "too late"}

    result = _run(
        run_tool_with_retries(
            name="Semgrep",
            runner=runner,
            timeout_seconds=0.05,
            max_retries=1,
        )
    )

    assert result.status == "timeout"
    assert result.timed_out is True
    assert result.attempt_count == 2
    assert result.conclusion == "neutral"
    assert "exceeded timeout" in result.output["summary"].lower()


def test_run_tool_with_retries_timeout_then_success():
    state = {"count": 0}

    async def runner():
        state["count"] += 1
        if state["count"] == 1:
            await asyncio.sleep(0.2)
        return {"title": "ok", "summary": "recovered"}

    result = _run(
        run_tool_with_retries(
            name="Bandit",
            runner=runner,
            timeout_seconds=0.05,
            max_retries=1,
        )
    )

    assert result.status == "success"
    assert result.attempt_count == 2
    assert result.conclusion == "success"


def test_run_tool_with_retries_exception_then_success():
    state = {"count": 0}

    async def runner():
        state["count"] += 1
        if state["count"] == 1:
            raise RuntimeError("transient")
        return {"title": "ok", "summary": "recovered"}

    result = _run(
        run_tool_with_retries(
            name="Checkov",
            runner=runner,
            timeout_seconds=1,
            max_retries=2,
        )
    )

    assert result.status == "success"
    assert result.attempt_count == 2
    assert result.conclusion == "success"


def test_run_tool_with_retries_exception_neutral_after_exhaustion():
    async def runner():
        raise RuntimeError("boom")

    result = _run(
        run_tool_with_retries(
            name="Ruff",
            runner=runner,
            timeout_seconds=1,
            max_retries=1,
        )
    )

    assert result.status == "error"
    assert result.attempt_count == 2
    assert result.conclusion == "neutral"
    assert result.error == "boom"
    assert "boom" in result.output["summary"]


def test_run_tool_with_retries_invalid_output_type_returns_error():
    async def runner():
        return "not-a-dict"

    result = _run(
        run_tool_with_retries(
            name="Trivy",
            runner=runner,
            timeout_seconds=1,
            max_retries=1,
        )
    )

    assert result.status == "error"
    assert result.attempt_count == 1
    assert result.conclusion == "neutral"
    assert "invalid runner return type" in result.error
    assert result.output["raw_output_preview"] == "'not-a-dict'"


def test_run_tool_with_retries_redacts_sensitive_exception_text():
    async def runner():
        raise RuntimeError(
            "token=ghp_abcdefghijklmnopqrstuvwxyz123456 "
            "path=C:\\Users\\alice\\secrets.txt "
            "home=~/private/key.txt "
            "email=alice@example.com "
            "hash=abcdefabcdefabcdefabcdefabcdefab"
        )

    result = _run(
        run_tool_with_retries(
            name="Semgrep",
            runner=runner,
            timeout_seconds=1,
            max_retries=0,
        )
    )

    assert result.status == "error"
    assert "[REDACTED_SECRET]" in result.error
    assert "[REDACTED_EMAIL]" in result.error
    assert "[REDACTED_PATH]" in result.error
    assert "alice@example.com" not in result.error
    assert "C:\\Users\\alice\\secrets.txt" not in result.error
    assert "~/private/key.txt" not in result.error
    assert "ghp_abcdefghijklmnopqrstuvwxyz123456" not in result.error


def test_run_tool_with_retries_rejects_invalid_settings():
    async def runner():
        return {"title": "ok", "summary": "ok"}

    with pytest.raises(ValueError):
        _run(
            run_tool_with_retries(
                name="ESLint",
                runner=runner,
                timeout_seconds=0,
            )
        )

    with pytest.raises(ValueError):
        _run(
            run_tool_with_retries(
                name="ESLint",
                runner=runner,
                timeout_seconds=-1,
            )
        )

    with pytest.raises(ValueError):
        _run(
            run_tool_with_retries(
                name="ESLint",
                runner=runner,
                timeout_seconds=1,
                max_retries=-1,
            )
        )

    with pytest.raises(ValueError):
        _run(
            run_tool_with_retries(
                name="ESLint",
                runner=runner,
                timeout_seconds=1,
                retry_delay_seconds=-1,
            )
        )
