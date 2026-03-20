"""Leaderboard calculation, reconciliation, and display functions."""

import calendar
import json
import secrets
import time
from typing import Optional
from urllib.parse import quote

from js import console


# ---------------------------------------------------------------------------
# Leaderboard functions
# ---------------------------------------------------------------------------


async def _calculate_leaderboard_stats_from_d1(owner: str, env) -> Optional[dict]:
    """Read current-month leaderboard stats from D1 if configured."""
    db = _d1_binding(env)
    if not db:
        console.error("[D1] No D1 binding available")
        return None

    await _ensure_leaderboard_schema(db)
    mk = _month_key()
    start_timestamp, end_timestamp = _month_window(mk)

    monthly_rows = await _d1_all(
        db,
        """
        SELECT user_login, merged_prs, closed_prs, reviews, comments
        FROM leaderboard_monthly_stats
        WHERE org = ? AND month_key = ?
        """,
        (owner, mk),
    )
    open_rows = await _d1_all(
        db,
        """
        SELECT user_login, open_prs
        FROM leaderboard_open_prs
        WHERE org = ?
        """,
        (owner,),
    )

    console.log(f"[D1] Queried org={owner} mk={mk}: {len(monthly_rows or [])} monthly, {len(open_rows or [])} open")
    if not monthly_rows and not open_rows:
        console.log(f"[D1] WARNING: No D1 data found for org={owner}")

    user_stats = {}

    def ensure(login: str):
        if login not in user_stats:
            user_stats[login] = {
                "openPrs": 0,
                "mergedPrs": 0,
                "closedPrs": 0,
                "reviews": 0,
                "comments": 0,
                "total": 0,
            }

    for row in monthly_rows:
        login = row.get("user_login")
        if not login:
            continue
        ensure(login)
        user_stats[login]["mergedPrs"] = int(row.get("merged_prs") or 0)
        user_stats[login]["closedPrs"] = int(row.get("closed_prs") or 0)
        user_stats[login]["reviews"] = int(row.get("reviews") or 0)
        user_stats[login]["comments"] = int(row.get("comments") or 0)

    for row in open_rows:
        login = row.get("user_login")
        if not login:
            continue
        ensure(login)
        user_stats[login]["openPrs"] = int(row.get("open_prs") or 0)

    for login in user_stats:
        s = user_stats[login]
        s["total"] = (s["openPrs"] * 1) + (s["mergedPrs"] * 10) + (s["closedPrs"] * -2) + (s["reviews"] * 5) + (s["comments"] * 2)

    sorted_users = sorted(
        [{"login": login, **stats} for login, stats in user_stats.items()],
        key=lambda u: (-u["total"], -u["mergedPrs"], -u["reviews"], u["login"].lower())
    )

    return {
        "users": user_stats,
        "sorted": sorted_users,
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp,
    }


def _is_ts_in_month(ts: int, start_ts: int, end_ts: int) -> bool:
    return bool(ts and start_ts <= ts <= end_ts)


def _reconcile_lock_holder(org: str) -> str:
    return f"{org}:{int(time.time() * 1000)}:{secrets.token_hex(8)}"


async def _log_reconcile_config_if_needed(db, settings: dict) -> None:
    """Log reconcile config when changed or stale, deduped across isolates via D1."""
    try:
        cfg_value = json.dumps(settings, sort_keys=True)
        now_ts = int(time.time())
        row = await _d1_first(
            db,
            "SELECT value, updated_at FROM leaderboard_runtime_meta WHERE key = ?",
            ("reconcile_config",),
        )
        last_value = (row or {}).get("value") if isinstance(row, dict) else None
        last_updated = int((row or {}).get("updated_at") or 0) if isinstance(row, dict) else 0
        if last_value == cfg_value and (now_ts - last_updated) < 86400:
            return

        await _d1_run(
            db,
            """
            INSERT INTO leaderboard_runtime_meta (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            """,
            ("reconcile_config", cfg_value, now_ts),
        )
        console.log(
            "[LeaderboardReconcile] Config "
            f"repos_per_page={settings['repos_per_page']} "
            f"prs_per_page={settings['prs_per_page']} "
            f"max_closed_pages={settings['max_closed_pages']} "
            f"max_open_pages={settings['max_open_pages']} "
            f"lock_lease_s={settings['lock_lease_seconds']} "
            f"timeout_s={settings['timeout_seconds']}"
        )
    except Exception as exc:
        console.error(f"[LeaderboardReconcile] Failed config log dedupe check: {exc}")


async def _acquire_reconcile_lock(db, org: str, holder: str, lease_seconds: int) -> bool:
    now_ts = int(time.time())
    lock_until = now_ts + max(1, lease_seconds)
    try:
        result = await _d1_run(
            db,
            """
            INSERT INTO leaderboard_reconcile_locks (org, holder, lock_until, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(org) DO UPDATE SET
                holder = excluded.holder,
                lock_until = excluded.lock_until,
                updated_at = excluded.updated_at
            WHERE leaderboard_reconcile_locks.lock_until < ?
               OR leaderboard_reconcile_locks.holder = ?
            """,
            (org, holder, lock_until, now_ts, now_ts, holder),
        )
        parsed = _d1_result_to_dict(result)
        meta = parsed.get("meta") if isinstance(parsed, dict) else None
        changes = int(meta.get("changes") or 0) if isinstance(meta, dict) else 0
        return changes > 0
    except Exception as exc:
        console.error(f"[LeaderboardReconcile] Failed to acquire lock for {org}: {exc}")
        return False


async def _release_reconcile_lock(db, org: str, holder: str) -> None:
    try:
        await _d1_run(
            db,
            "DELETE FROM leaderboard_reconcile_locks WHERE org = ? AND holder = ?",
            (org, holder),
        )
    except Exception as exc:
        console.error(f"[LeaderboardReconcile] Failed to release lock for {org}: {exc}")


async def _refresh_reconcile_lock(db, org: str, holder: str, lease_seconds: int) -> bool:
    now_ts = int(time.time())
    lock_until = now_ts + max(1, lease_seconds)
    try:
        result = await _d1_run(
            db,
            """
            UPDATE leaderboard_reconcile_locks
            SET lock_until = ?, updated_at = ?
            WHERE org = ? AND holder = ? AND lock_until >= ?
            """,
            (lock_until, now_ts, org, holder, now_ts),
        )
        parsed = _d1_result_to_dict(result)
        meta = parsed.get("meta") if isinstance(parsed, dict) else None
        changes = int(meta.get("changes") or 0) if isinstance(meta, dict) else 0
        return changes > 0
    except Exception as exc:
        console.error(f"[LeaderboardReconcile] Failed to refresh lock for {org}: {exc}")
        return False


async def _reconcile_github_api(
    method: str,
    path: str,
    token: str,
    deadline_ts: float,
    *,
    max_retries: int = 2,
):
    """GitHub API call with bounded retries for 429/5xx under a reconcile deadline."""
    attempt = 0
    while True:
        if time.time() >= deadline_ts:
            raise TimeoutError(f"deadline exceeded before request {path}")

        remaining = deadline_ts - time.time()
        if remaining <= 0:
            raise TimeoutError(f"deadline exceeded before request {path}")
        try:
            timeout_s = max(0.05, float(remaining))
            try:
                resp = await github_api(method, path, token, timeout_seconds=timeout_s)
            except TypeError as exc:
                if "timeout_seconds" in str(exc):
                    resp = await github_api(method, path, token)
                else:
                    raise
        except Exception as exc:
            msg = str(exc).lower()
            if "abort" in msg or "timeout" in msg:
                raise TimeoutError(f"request timeout for {path}") from exc
            raise
        status = int(getattr(resp, "status", 0) or 0)

        is_rate_limited = _is_rate_limited_response(resp)
        retryable = is_rate_limited or status in (500, 502, 503, 504)
        if not retryable or attempt >= max_retries:
            return resp

        if is_rate_limited:
            retry_after = _retry_after_seconds(resp)
            reset_wait = _rate_limit_reset_wait_seconds(resp)
            if reset_wait is not None:
                retry_after = max(retry_after, reset_wait)
        else:
            retry_after = attempt + 1
        remaining = deadline_ts - time.time()
        if remaining <= 0:
            return resp
        wait_s = min(float(retry_after), max(0.0, remaining - 0.05))
        if wait_s <= 0:
            return resp

        console.error(
            f"[LeaderboardReconcile] Retryable GitHub response status={status} path={path} "
            f"attempt={attempt + 1}/{max_retries}; waiting {wait_s:.2f}s"
        )
        await _sleep_seconds(wait_s)
        attempt += 1


async def _d1_batch_chunked(db, statements: list, chunk_size: int, continue_or_abort=None) -> bool:
    """Execute D1 batches in chunks to avoid oversized batch payloads."""
    if not statements:
        return True

    # Prefer a single db.batch call when supported, because D1 guarantees
    # atomicity at the individual batch invocation boundary.
    batch_fn = getattr(db, "batch", None)
    if callable(batch_fn):
        if continue_or_abort is not None:
            ok = await continue_or_abort()
            if not ok:
                return False
        try:
            await _d1_batch(db, statements)
            return True
        except Exception as exc:
            console.error(f"[D1.batch] Transactional batch failed: {exc}")
            return False

    size = max(1, int(chunk_size or 1))
    try:
        for i in range(0, len(statements), size):
            if continue_or_abort is not None:
                ok = await continue_or_abort()
                if not ok:
                    return False
            await _d1_batch(db, statements[i : i + size])
        return True
    except Exception as exc:
        console.error(f"[D1.batch] Chunked fallback batch failed: {exc}")
        return False


async def _reconcile_org_leaderboard_from_github(owner: str, token: str, env, deadline_ts: Optional[float] = None) -> bool:
    """Rebuild current-month leaderboard PR stats from live GitHub state.

    This is the anti-drift path for /leaderboard:
    - Always recomputes org-wide open/merged/closed PR counts from GitHub.
    - Replaces D1 snapshot rows for the current month.
    - Clears stale historical/backfill artifacts for this org.
    """
    db = _d1_binding(env)
    if not db:
        return False

    settings = _reconcile_settings(env)
    if deadline_ts is None:
        deadline_ts = time.time() + settings["timeout_seconds"]
    holder = _reconcile_lock_holder(owner)

    await _ensure_leaderboard_schema(db)
    await _log_reconcile_config_if_needed(db, settings)
    acquired = await _acquire_reconcile_lock(db, owner, holder, settings["lock_lease_seconds"])
    if not acquired:
        console.log(f"[LeaderboardReconcile] Lock busy for org={owner}; skipping reconcile")
        return False

    lease_refresh_interval = max(1, settings["lock_lease_seconds"] // 3)
    next_lease_refresh_ts = time.time() + lease_refresh_interval

    def _deadline_exceeded() -> bool:
        return deadline_ts is not None and time.time() >= deadline_ts

    async def _continue_or_abort() -> bool:
        nonlocal next_lease_refresh_ts
        if _deadline_exceeded():
            console.error(f"[LeaderboardReconcile] Reconcile timeout reached for org={owner}; aborting")
            return False
        now = time.time()
        if now >= next_lease_refresh_ts:
            ok = await _refresh_reconcile_lock(db, owner, holder, settings["lock_lease_seconds"])
            if not ok:
                console.error(f"[LeaderboardReconcile] Lock renewal failed for org={owner}; aborting reconcile")
                return False
            next_lease_refresh_ts = now + lease_refresh_interval
        return True

    try:
        month_key = _month_key()
        start_ts, end_ts = _month_window(month_key)
        reconcile_ts = int(time.time())
        max_tracked_pr_entries = _env_int(env, "RECONCILE_MAX_TRACKED_PRS", 120000)
        max_batch_statements = _env_int(env, "RECONCILE_MAX_BATCH_STATEMENTS", 900)

        existing_rows = await _d1_all(
            db,
            """
            SELECT user_login, comments
            FROM leaderboard_monthly_stats
            WHERE org = ? AND month_key = ?
            """,
            (owner, month_key),
        )
        review_counts = {}
        review_credit_rows = []
        preserved_comments = {
            row.get("user_login"): int(row.get("comments") or 0)
            for row in (existing_rows or [])
            if row.get("user_login")
        }

        open_by_user = {}
        merged_by_user = {}
        closed_by_user = {}
        pr_state_map = {}
        seen_open_prs = {}

        repo_page = 1
        while True:
            if not await _continue_or_abort():
                return False
            repos_resp = await _reconcile_github_api(
                "GET",
                f"/orgs/{owner}/repos?sort=full_name&direction=asc&per_page={settings['repos_per_page']}&page={repo_page}",
                token,
                deadline_ts,
            )
            if repos_resp.status != 200:
                console.error(
                    f"[LeaderboardReconcile] Failed to list repos for {owner}: status={repos_resp.status} page={repo_page}"
                )
                return False
            repos = json.loads(await repos_resp.text())
            if not repos:
                break

            for repo_obj in repos:
                repo_name = repo_obj.get("name")
                if not repo_name:
                    continue

                # Snapshot all open PRs in this repo.
                open_page = 1
                while True:
                    if not await _continue_or_abort():
                        return False
                    open_resp = await _reconcile_github_api(
                        "GET",
                        f"/repos/{owner}/{repo_name}/pulls?state=open&per_page={settings['prs_per_page']}&page={open_page}",
                        token,
                        deadline_ts,
                    )
                    if open_resp.status != 200:
                        console.error(
                            f"[LeaderboardReconcile] Failed open PR fetch {owner}/{repo_name}: status={open_resp.status} page={open_page}"
                        )
                        return False
                    open_prs = json.loads(await open_resp.text())
                    if not open_prs:
                        break

                    for pr in open_prs:
                        if not await _continue_or_abort():
                            return False
                        user = pr.get("user") or {}
                        if _is_bot(user):
                            continue
                        login = user.get("login")
                        pr_number = pr.get("number")
                        if not (login and pr_number):
                            continue
                        key = (repo_name, pr_number)
                        if key in seen_open_prs:
                            continue
                        if len(seen_open_prs) >= max_tracked_pr_entries:
                            console.error(
                                f"[LeaderboardReconcile] Tracked PR cap reached for org={owner} while scanning open PRs; aborting"
                            )
                            return False
                        seen_open_prs[key] = login
                        open_by_user[login] = open_by_user.get(login, 0) + 1
                        pr_state_map[key] = (owner, repo_name, pr_number, login, "open", 0, None, reconcile_ts)

                    if len(open_prs) < settings["prs_per_page"]:
                        break
                    if open_page >= settings["max_open_pages"]:
                        console.error(
                            f"[LeaderboardReconcile] Open PR pagination cap reached for {owner}/{repo_name} at page={open_page}; "
                            "aborting reconcile to avoid open PR undercount"
                        )
                        return False
                    open_page += 1

                # Snapshot current-month closed/merged PR outcomes in this repo.
                closed_page = 1
                while closed_page <= settings["max_closed_pages"]:
                    if not await _continue_or_abort():
                        return False
                    closed_resp = await _reconcile_github_api(
                        "GET",
                        f"/repos/{owner}/{repo_name}/pulls?state=closed&sort=updated&direction=desc&per_page={settings['prs_per_page']}&page={closed_page}",
                        token,
                        deadline_ts,
                    )
                    if closed_resp.status != 200:
                        console.error(
                            f"[LeaderboardReconcile] Failed closed PR fetch {owner}/{repo_name}: status={closed_resp.status} page={closed_page}"
                        )
                        return False
                    closed_prs = json.loads(await closed_resp.text())
                    if not closed_prs:
                        break

                    for pr in closed_prs:
                        if not await _continue_or_abort():
                            return False
                        user = pr.get("user") or {}
                        if _is_bot(user):
                            continue
                        login = user.get("login")
                        pr_number = pr.get("number")
                        if not (login and pr_number):
                            continue
                        key = (repo_name, pr_number)
                        if len(pr_state_map) >= max_tracked_pr_entries and key not in pr_state_map:
                            console.error(
                                f"[LeaderboardReconcile] Tracked PR cap reached for org={owner} while scanning closed PRs; aborting"
                            )
                            return False

                        # If this PR was seen open earlier in this reconcile pass, undo
                        # that open snapshot before recording its closed/merged state.
                        open_login = seen_open_prs.pop(key, None)
                        if open_login:
                            open_by_user[open_login] = max(0, open_by_user.get(open_login, 0) - 1)
                            if open_by_user[open_login] == 0:
                                open_by_user.pop(open_login, None)
                            existing_open = pr_state_map.get(key)
                            if existing_open and existing_open[4] == "open":
                                pr_state_map.pop(key, None)

                        merged_at = pr.get("merged_at")
                        closed_at = pr.get("closed_at")
                        merged_ts = _parse_github_timestamp(merged_at) if merged_at else 0
                        closed_ts = _parse_github_timestamp(closed_at) if closed_at else 0

                        if _is_ts_in_month(merged_ts, start_ts, end_ts):
                            merged_by_user[login] = merged_by_user.get(login, 0) + 1
                            pr_state_map[key] = (owner, repo_name, pr_number, login, "closed", 1, closed_ts or merged_ts, reconcile_ts)
                        elif _is_ts_in_month(closed_ts, start_ts, end_ts):
                            closed_by_user[login] = closed_by_user.get(login, 0) + 1
                            pr_state_map[key] = (owner, repo_name, pr_number, login, "closed", 0, closed_ts, reconcile_ts)

                    if len(closed_prs) < settings["prs_per_page"]:
                        break
                    # Results are sorted by updated desc. If the least recently updated
                    # PR on this page is older than the month window start, deeper pages
                    # cannot contain in-window PR updates.
                    last_pr = closed_prs[-1]
                    updated_ts = _parse_github_timestamp(last_pr.get("updated_at")) if last_pr.get("updated_at") else 0
                    if updated_ts and updated_ts < start_ts:
                        break
                    if (
                        closed_page >= settings["max_closed_pages"]
                        and updated_ts
                        and updated_ts >= start_ts
                    ):
                        console.error(
                            f"[LeaderboardReconcile] Closed PR pagination cap reached for {owner}/{repo_name}; "
                            "aborting reconcile to avoid partial month undercount"
                        )
                        return False
                    closed_page += 1

            if len(repos) < settings["repos_per_page"]:
                break
            repo_page += 1

        # Recompute review credits from live PR reviews for all discovered PRs
        # so credits on still-open PRs are preserved and stale rows self-heal.
        for row in pr_state_map.values():
            if not await _continue_or_abort():
                return False

            repo_name = row[1]
            pr_number = row[2]
            reviews_resp = await _reconcile_github_api(
                "GET",
                f"/repos/{owner}/{repo_name}/pulls/{pr_number}/reviews?per_page=100",
                token,
                deadline_ts,
            )
            if reviews_resp.status != 200:
                console.error(
                    f"[LeaderboardReconcile] Failed review fetch {owner}/{repo_name}#{pr_number}: "
                    f"status={reviews_resp.status}"
                )
                return False

            reviews = json.loads(await reviews_resp.text())
            first_review_ts_by_user = {}
            for review in reviews:
                reviewer = review.get("user") or {}
                if _is_bot(reviewer):
                    continue
                reviewer_login = reviewer.get("login")
                submitted_at = review.get("submitted_at")
                submitted_ts = _parse_github_timestamp(submitted_at) if submitted_at else 0
                if not reviewer_login or not _is_ts_in_month(submitted_ts, start_ts, end_ts):
                    continue

                prev_ts = first_review_ts_by_user.get(reviewer_login)
                if prev_ts is None or submitted_ts < prev_ts:
                    first_review_ts_by_user[reviewer_login] = submitted_ts

            first_two = sorted(first_review_ts_by_user.items(), key=lambda item: item[1])[:2]
            for reviewer_login, _submitted_ts in first_two:
                review_counts[reviewer_login] = review_counts.get(reviewer_login, 0) + 1
                review_credit_rows.append(
                    (owner, repo_name, pr_number, month_key, reviewer_login, reconcile_ts + 1, reconcile_ts)
                )

        try:
            all_logins = set(open_by_user.keys()) | set(merged_by_user.keys()) | set(closed_by_user.keys()) | set(review_counts.keys()) | set(preserved_comments.keys())
            destructive_stmts = [
                ("DELETE FROM leaderboard_open_prs WHERE org = ? AND updated_at <= ?", (owner, reconcile_ts)),
                (
                    """
                    DELETE FROM leaderboard_pr_state
                    WHERE org = ?
                      AND updated_at <= ?
                      AND (
                            state = 'open'
                            OR (state = 'closed' AND closed_at BETWEEN ? AND ?)
                          )
                    """,
                    (owner, reconcile_ts, start_ts, end_ts),
                ),
                (
                    """
                    UPDATE leaderboard_monthly_stats
                    SET merged_prs = 0,
                        closed_prs = 0,
                        pr_updated_at = ?
                    WHERE org = ? AND month_key = ? AND pr_updated_at <= ?
                    """,
                    (reconcile_ts, owner, month_key, reconcile_ts),
                ),
                (
                    "DELETE FROM leaderboard_review_credits WHERE org = ? AND month_key = ? AND created_at <= ?",
                    (owner, month_key, reconcile_ts),
                ),
                ("DELETE FROM leaderboard_backfill_state WHERE org = ?", (owner,)),
                ("DELETE FROM leaderboard_backfill_repo_done WHERE org = ?", (owner,)),
            ]
            batch_stmts = []

            for rc_row in review_credit_rows:
                batch_stmts.append(
                    (
                        """
                        INSERT INTO leaderboard_review_credits (org, repo, pr_number, month_key, reviewer_login, created_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(org, repo, pr_number, month_key, reviewer_login) DO UPDATE SET
                            created_at = excluded.created_at
                        WHERE leaderboard_review_credits.created_at <= ?
                        """,
                        rc_row,
                    )
                )

            for login, count in open_by_user.items():
                batch_stmts.append(
                    (
                        """
                        INSERT INTO leaderboard_open_prs (org, user_login, open_prs, updated_at)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(org, user_login) DO UPDATE SET
                            open_prs = excluded.open_prs,
                            updated_at = excluded.updated_at
                        WHERE leaderboard_open_prs.updated_at <= ?
                        """,
                        (owner, login, count, reconcile_ts + 1, reconcile_ts),
                    )
                )

            for row in pr_state_map.values():
                batch_stmts.append(
                    (
                        """
                        INSERT INTO leaderboard_pr_state (org, repo, pr_number, author_login, state, merged, closed_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(org, repo, pr_number) DO UPDATE SET
                            author_login = excluded.author_login,
                            state = excluded.state,
                            merged = excluded.merged,
                            closed_at = excluded.closed_at,
                            updated_at = excluded.updated_at
                        WHERE leaderboard_pr_state.updated_at <= ?
                        """,
                        (row[0], row[1], row[2], row[3], row[4], row[5], row[6], reconcile_ts + 1, reconcile_ts),
                    )
                )

            for login in all_logins:
                batch_stmts.append(
                    (
                        """
                        INSERT INTO leaderboard_monthly_stats
                            (org, month_key, user_login, merged_prs, closed_prs, reviews, comments, updated_at, pr_updated_at, review_updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(org, month_key, user_login) DO UPDATE SET
                            merged_prs = CASE
                                WHEN leaderboard_monthly_stats.pr_updated_at <= excluded.pr_updated_at
                                THEN excluded.merged_prs
                                ELSE leaderboard_monthly_stats.merged_prs
                            END,
                            closed_prs = CASE
                                WHEN leaderboard_monthly_stats.pr_updated_at <= excluded.pr_updated_at
                                THEN excluded.closed_prs
                                ELSE leaderboard_monthly_stats.closed_prs
                            END,
                            pr_updated_at = CASE
                                WHEN leaderboard_monthly_stats.pr_updated_at <= excluded.pr_updated_at
                                THEN excluded.pr_updated_at
                                ELSE leaderboard_monthly_stats.pr_updated_at
                            END,
                            reviews = CASE
                                WHEN leaderboard_monthly_stats.review_updated_at <= excluded.review_updated_at
                                THEN excluded.reviews
                                ELSE leaderboard_monthly_stats.reviews
                            END,
                            review_updated_at = CASE
                                WHEN leaderboard_monthly_stats.review_updated_at <= excluded.review_updated_at
                                THEN excluded.review_updated_at
                                ELSE leaderboard_monthly_stats.review_updated_at
                            END
                        WHERE leaderboard_monthly_stats.pr_updated_at <= ?
                           OR leaderboard_monthly_stats.review_updated_at <= ?
                        """,
                        (
                            owner,
                            month_key,
                            login,
                            merged_by_user.get(login, 0),
                            closed_by_user.get(login, 0),
                            int(review_counts.get(login) or 0),
                            int(preserved_comments.get(login) or 0),
                            reconcile_ts + 1,
                            reconcile_ts + 1,
                            reconcile_ts + 1,
                            reconcile_ts,
                            reconcile_ts,
                        ),
                    )
                )

            if not await _continue_or_abort():
                return False
            if not await _d1_batch_chunked(db, batch_stmts, max_batch_statements, _continue_or_abort):
                return False
            if not await _continue_or_abort():
                return False
            if not await _d1_batch_chunked(db, destructive_stmts, max_batch_statements, _continue_or_abort):
                return False
        except Exception as exc:
            console.error(f"[LeaderboardReconcile] Batch write failed for {owner}: {exc}")
            return False

        console.log(
            f"[LeaderboardReconcile] Completed org={owner} month={month_key} users={len(all_logins)} repos_scanned_page_end={repo_page}"
        )
        return True
    finally:
        await _release_reconcile_lock(db, owner, holder)


async def _get_backfill_state(db, owner: str, month_key: str) -> dict:
    row = await _d1_first(
        db,
        """
        SELECT next_page, completed FROM leaderboard_backfill_state
        WHERE org = ? AND month_key = ?
        """,
        (owner, month_key),
    )
    if row:
        return {
            "next_page": int(row.get("next_page") or 1),
            "completed": bool(int(row.get("completed") or 0)),
        }
    return {"next_page": 1, "completed": False}


async def _set_backfill_state(db, owner: str, month_key: str, next_page: int, completed: bool) -> None:
    try:
        await _d1_run(
            db,
            """
            INSERT INTO leaderboard_backfill_state (org, month_key, next_page, completed, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(org, month_key) DO UPDATE SET
                next_page = excluded.next_page,
                completed = excluded.completed,
                updated_at = excluded.updated_at
            """,
            (owner, month_key, next_page, 1 if completed else 0, int(time.time())),
        )
        console.log(f"[Backfill] State updated: org={owner} month={month_key} next_page={next_page} completed={completed}")
    except Exception as e:
        console.error(f"[Backfill] Failed to update state: {e}")


async def _run_incremental_backfill(owner: str, token: str, env, repos_per_request: int = 5) -> Optional[dict]:
    """Backfill leaderboard data in small chunks and report progress for user-facing notes."""
    db = _d1_binding(env)
    if not db:
        console.error("[Backfill] No D1 binding available")
        return None

    await _ensure_leaderboard_schema(db)
    month_key = _month_key()
    start_ts, end_ts = _month_window(month_key)
    start_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(start_ts))

    state = await _get_backfill_state(db, owner, month_key)
    console.log(f"[Backfill] Current state: page={state['next_page']}, completed={state['completed']}")
    if state["completed"]:
        console.log(f"[Backfill] Already completed for {owner}/{month_key}")
        return {"ran": False, "completed": True, "processed": 0, "next_page": state["next_page"]}

    page = state["next_page"]
    console.log(f"[Backfill] Fetching repos page {page} for {owner}")
    repos_resp = await github_api(
        "GET",
        f"/orgs/{owner}/repos?sort=full_name&direction=asc&per_page={repos_per_request}&page={page}",
        token,
    )
    if repos_resp.status != 200:
        console.error(f"[Backfill] Failed to fetch repo page {page}: status={repos_resp.status}")
        return {"ran": False, "completed": False, "processed": 0, "next_page": page}

    repos = json.loads(await repos_resp.text())
    console.log(f"[Backfill] Got {len(repos)} repos on page {page}")
    if not repos:
        console.log(f"[Backfill] No more repos, marking backfill complete")
        await _set_backfill_state(db, owner, month_key, page, True)
        return {"ran": False, "completed": True, "processed": 0, "next_page": page}

    processed = 0
    for repo_obj in repos:
        repo_name = repo_obj.get("name")
        if not repo_name:
            continue
        console.log(f"[Backfill] Backfilling repo {owner}/{repo_name}")
        seeded = await _backfill_repo_month_if_needed(owner, repo_name, token, env, month_key, start_ts, end_ts)
        if seeded:
            processed += 1
            console.log(f"[Backfill] Seeded {owner}/{repo_name} (total processed this run: {processed})")
        else:
            console.log(f"[Backfill] Skipped {owner}/{repo_name} (already seeded or failed)")

    done = len(repos) < repos_per_request
    console.log(f"[Backfill] Processed {processed} repos, done={done}")
    await _set_backfill_state(db, owner, month_key, page + 1, done)
    return {
        "ran": True,
        "completed": done,
        "processed": processed,
        "next_page": page + 1,
        "month_key": month_key,
        "since": start_iso,
    }


async def _backfill_repo_month_if_needed(
    owner: str,
    repo_name: str,
    token: str,
    env,
    month_key: Optional[str] = None,
    start_ts: Optional[int] = None,
    end_ts: Optional[int] = None,
) -> bool:
    """Backfill leaderboard stats for one repo once per month. Returns True if newly seeded."""
    db = _d1_binding(env)
    if not db:
        console.error(f"[Backfill] No D1 binding available for {owner}/{repo_name}")
        return False

    await _ensure_leaderboard_schema(db)
    mk = month_key or _month_key()
    if start_ts is None or end_ts is None:
        start_ts, end_ts = _month_window(mk)

    already = await _d1_first(
        db,
        """
        SELECT 1 FROM leaderboard_backfill_repo_done
        WHERE org = ? AND month_key = ? AND repo = ?
        """,
        (owner, mk, repo_name),
    )
    if already:
        console.log(f"[Backfill] Repo {owner}/{repo_name} already seeded for {mk}")
        return False

    console.log(f"[Backfill] Starting backfill for {owner}/{repo_name} month={mk}")

    # Load all PR numbers already tracked via webhooks for this repo to avoid
    # double-counting PRs that were already processed by webhook event handlers.
    # Also load the recorded state so we can self-heal PRs that were tracked as
    # 'open' but whose close/merge webhook was missed.
    tracked_rows = await _d1_all(
        db,
        "SELECT pr_number, state FROM leaderboard_pr_state WHERE org = ? AND repo = ?",
        (owner, repo_name),
    )
    already_tracked_state = {int(row["pr_number"]): row.get("state", "") for row in (tracked_rows or [])}
    already_tracked = set(already_tracked_state.keys())
    console.log(f"[Backfill] {len(already_tracked)} PRs already tracked for {owner}/{repo_name}")

    now_ts = int(time.time())

    # Open PRs snapshot for this repo.
    open_resp = await github_api(
        "GET",
        f"/repos/{owner}/{repo_name}/pulls?state=open&per_page=100",
        token,
    )
    if open_resp.status == 200:
        open_prs = json.loads(await open_resp.text())
        open_by_user = {}
        for pr in open_prs:
            user = pr.get("user") or {}
            if _is_bot(user):
                continue
            login = user.get("login")
            pr_number = pr.get("number")
            if not login or not pr_number:
                continue
            if pr_number in already_tracked:
                console.log(f"[Backfill] Skipping open PR #{pr_number} (already tracked via webhook)")
                continue
            open_by_user[login] = open_by_user.get(login, 0) + 1
            # Record in pr_state so webhook handlers can coordinate future state changes.
            await _d1_run(
                db,
                """
                INSERT INTO leaderboard_pr_state (org, repo, pr_number, author_login, state, merged, closed_at, updated_at)
                VALUES (?, ?, ?, ?, 'open', 0, NULL, ?)
                ON CONFLICT(org, repo, pr_number) DO NOTHING
                """,
                (owner, repo_name, pr_number, login, now_ts),
            )
            already_tracked.add(pr_number)
            already_tracked_state[pr_number] = "open"
        console.log(f"[Backfill] Found {len(open_prs)} open PRs, {len(open_by_user)} unique users (new)")
        for login, cnt in open_by_user.items():
            console.log(f"[Backfill] User {login}: {cnt} open PRs")
            await _d1_inc_open_pr(db, owner, login, cnt)
    else:
        console.error(f"[Backfill] Failed to fetch open PRs: status={open_resp.status}")

    # Closed/merged monthly stats for this repo.
    # Paginate up to 3 pages to catch repos with more than 100 closed PRs in the month.
    merged_count = 0
    closed_count = 0
    closed_page = 1
    # Collect merged PRs for review backfill (capped to limit extra API calls).
    merged_prs_for_review = []
    MAX_REVIEW_BACKFILL = 20
    while closed_page <= 3:
        closed_resp = await github_api(
            "GET",
            f"/repos/{owner}/{repo_name}/pulls?state=closed&per_page=100&sort=updated&direction=desc&page={closed_page}",
            token,
        )
        if closed_resp.status != 200:
            console.error(f"[Backfill] Failed to fetch closed PRs page {closed_page}: status={closed_resp.status}")
            break
        closed_prs = json.loads(await closed_resp.text())
        if not closed_prs:
            break
        for pr in closed_prs:
            user = pr.get("user") or {}
            if _is_bot(user):
                continue
            login = user.get("login")
            pr_number = pr.get("number")
            if not login or not pr_number:
                continue
            tracked_state = already_tracked_state.get(pr_number)
            if tracked_state == "closed":
                # Already properly tracked as closed — skip to avoid double-counting.
                console.log(f"[Backfill] Skipping closed PR #{pr_number} (already tracked via webhook)")
                continue
            # Self-heal: PR was recorded as 'open' in the database but GitHub now shows it
            # as closed/merged, meaning the close/merge webhook was missed.  Undo the open
            # count that was previously recorded and fall through to count it correctly.
            is_self_heal = tracked_state == "open"
            if is_self_heal:
                console.log(f"[Backfill] Self-healing PR #{pr_number} for {login}: was 'open', now closed")
                await _d1_inc_open_pr(db, owner, login, -1)
            merged_at = pr.get("merged_at")
            closed_at = pr.get("closed_at")
            if merged_at:
                merged_ts = _parse_github_timestamp(merged_at)
                if start_ts <= merged_ts <= end_ts:
                    console.log(f"[Backfill] User {login}: merged PR (#{pr_number})")
                    merged_count += 1
                    await _d1_inc_monthly(db, owner, mk, login, "merged_prs", 1)
                    # Use closed_at for the stored timestamp to match the idempotency check
                    # in _track_pr_closed_in_d1, falling back to merged_ts if absent.
                    pr_closed_ts = _parse_github_timestamp(closed_at) if closed_at else merged_ts
                    await _d1_run(
                        db,
                        """
                        INSERT INTO leaderboard_pr_state (org, repo, pr_number, author_login, state, merged, closed_at, updated_at)
                        VALUES (?, ?, ?, ?, 'closed', 1, ?, ?)
                        ON CONFLICT(org, repo, pr_number) DO UPDATE SET
                            state = 'closed',
                            merged = 1,
                            closed_at = excluded.closed_at,
                            updated_at = excluded.updated_at
                        """,
                        (owner, repo_name, pr_number, login, pr_closed_ts, now_ts),
                    )
                    already_tracked.add(pr_number)
                    already_tracked_state[pr_number] = "closed"
                    if len(merged_prs_for_review) < MAX_REVIEW_BACKFILL:
                        merged_prs_for_review.append((pr_number, login))
            elif closed_at:
                closed_ts_val = _parse_github_timestamp(closed_at)
                if start_ts <= closed_ts_val <= end_ts:
                    console.log(f"[Backfill] User {login}: closed PR (#{pr_number})")
                    closed_count += 1
                    await _d1_inc_monthly(db, owner, mk, login, "closed_prs", 1)
                    await _d1_run(
                        db,
                        """
                        INSERT INTO leaderboard_pr_state (org, repo, pr_number, author_login, state, merged, closed_at, updated_at)
                        VALUES (?, ?, ?, ?, 'closed', 0, ?, ?)
                        ON CONFLICT(org, repo, pr_number) DO UPDATE SET
                            state = 'closed',
                            merged = 0,
                            closed_at = excluded.closed_at,
                            updated_at = excluded.updated_at
                        """,
                        (owner, repo_name, pr_number, login, closed_ts_val, now_ts),
                    )
                    already_tracked.add(pr_number)
                    already_tracked_state[pr_number] = "closed"
        # Stop paginating if fewer than 100 results (last page).
        if len(closed_prs) < 100:
            break
        closed_page += 1
    console.log(f"[Backfill] Found {merged_count} merged, {closed_count} closed PRs in month range")

    # Also include webhook-tracked merged PRs whose review webhooks may have been missed
    # (e.g. during app downtime). The leaderboard_review_credits idempotency guard ensures
    # no duplicate credits are awarded even if a PR is processed again.
    if len(merged_prs_for_review) < MAX_REVIEW_BACKFILL:
        tracked_merged_rows = await _d1_all(
            db,
            """
            SELECT pr_number, author_login FROM leaderboard_pr_state
            WHERE org = ? AND repo = ? AND merged = 1
            """,
            (owner, repo_name),
        )
        newly_added = {pr_num for pr_num, _ in merged_prs_for_review}
        for row in (tracked_merged_rows or []):
            if len(merged_prs_for_review) >= MAX_REVIEW_BACKFILL:
                break
            pr_num = row.get("pr_number")
            author = row.get("author_login", "")
            if pr_num and pr_num not in newly_added:
                merged_prs_for_review.append((pr_num, author))
                newly_added.add(pr_num)

    # Backfill review credits for merged PRs in the window (up to MAX_REVIEW_BACKFILL).
    # Mirrors the idempotency logic in _track_review_in_d1: only the first two unique
    # non-bot, non-author reviewers per PR per month get credit.
    if merged_prs_for_review:
        console.log(f"[Backfill] Fetching reviews for {len(merged_prs_for_review)} merged PRs")
    for pr_number, pr_author in merged_prs_for_review:
        try:
            reviews_resp = await github_api(
                "GET",
                f"/repos/{owner}/{repo_name}/pulls/{pr_number}/reviews?per_page=100",
                token,
            )
            if reviews_resp.status == 429:
                console.error(f"[Backfill] GitHub rate limit hit fetching reviews for PR #{pr_number}; skipping remaining review backfill")
                break
            if reviews_resp.status != 200:
                console.error(f"[Backfill] Failed to fetch reviews for PR #{pr_number}: status={reviews_resp.status}")
                continue
            reviews = json.loads(await reviews_resp.text())
            # Load all existing credits for this PR in one query to avoid N+1 SELECTs.
            credit_rows = await _d1_all(
                db,
                """
                SELECT reviewer_login FROM leaderboard_review_credits
                WHERE org = ? AND repo = ? AND pr_number = ? AND month_key = ?
                """,
                (owner, repo_name, pr_number, mk),
            )
            already_credited_set = {row["reviewer_login"] for row in (credit_rows or [])}
            seen_reviewers: set = set()
            for review in reviews:
                reviewer = review.get("user") or {}
                if _is_bot(reviewer):
                    continue
                reviewer_login = reviewer.get("login", "")
                if not reviewer_login or reviewer_login == pr_author:
                    continue
                if reviewer_login in seen_reviewers:
                    continue
                seen_reviewers.add(reviewer_login)
                if reviewer_login in already_credited_set:
                    continue
                # Stop processing once 2 unique reviewers have been credited for this PR.
                if len(already_credited_set) >= 2:
                    break
                await _d1_run(
                    db,
                    """
                    INSERT INTO leaderboard_review_credits (org, repo, pr_number, month_key, reviewer_login, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (owner, repo_name, pr_number, mk, reviewer_login, now_ts),
                )
                await _d1_inc_monthly(db, owner, mk, reviewer_login, "reviews", 1)
                already_credited_set.add(reviewer_login)
                console.log(f"[Backfill] Review credit: {reviewer_login} reviewed PR #{pr_number}")
        except Exception as e:
            console.error(f"[Backfill] Error fetching reviews for PR #{pr_number}: {e}")

    try:
        await _d1_run(
            db,
            """
            INSERT INTO leaderboard_backfill_repo_done (org, month_key, repo, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(org, month_key, repo) DO UPDATE SET
                updated_at = excluded.updated_at
            """,
            (owner, mk, repo_name, int(time.time())),
        )
        console.log(f"[Backfill] Marked {owner}/{repo_name} as complete for {mk}")
        return True
    except Exception as e:
        console.error(f"[Backfill] Failed to mark repo as done: {e}")
        return False


async def _reset_leaderboard_month(org: str, month_key: str, db) -> dict:
    """Clear all leaderboard data for an org/month so a fresh backfill can re-populate it.

    Deletes:
    - leaderboard_monthly_stats       for org + month_key
    - leaderboard_backfill_repo_done  for org + month_key  (allows re-backfill)
    - leaderboard_review_credits      for org + month_key
    - leaderboard_backfill_state      for org + month_key  (allows backfill to restart)
    - leaderboard_pr_state            for org where closed_at falls within the month window
    - leaderboard_open_prs            for org              (open PR counts are recalculated
                                                            fresh on next backfill)

    Returns a dict summarising cleared tables.
    """
    await _ensure_leaderboard_schema(db)
    deleted: dict = {}

    for table, params in [
        ("leaderboard_monthly_stats", (org, month_key)),
        ("leaderboard_backfill_repo_done", (org, month_key)),
        ("leaderboard_review_credits", (org, month_key)),
        ("leaderboard_backfill_state", (org, month_key)),
    ]:
        try:
            await _d1_run(db, f"DELETE FROM {table} WHERE org = ? AND month_key = ?", params)
            deleted[table] = "cleared"
        except Exception as e:
            console.error(f"[AdminReset] Error clearing {table}: {e}")
            deleted[table] = f"error: {e}"

    # Scope the pr_state delete to the target month's timestamp window so that
    # rows for other months (e.g. the current active month) are not destroyed.
    start_ts, end_ts = _month_window(month_key)
    try:
        # Two cases:
        #   1. Closed/merged PRs: closed_at falls within the month window.
        #   2. Open PRs recorded during this month: state='open', no closed_at,
        #      updated_at falls within the month window.
        await _d1_run(
            db,
            """
            DELETE FROM leaderboard_pr_state
            WHERE org = ?
              AND (
                closed_at BETWEEN ? AND ?
                OR (state = 'open' AND closed_at IS NULL AND updated_at BETWEEN ? AND ?)
              )
            """,
            (org, start_ts, end_ts, start_ts, end_ts),
        )
        deleted["leaderboard_pr_state"] = "cleared"
    except Exception as e:
        console.error(f"[AdminReset] Error clearing leaderboard_pr_state: {e}")
        deleted["leaderboard_pr_state"] = f"error: {e}"

    try:
        await _d1_run(db, "DELETE FROM leaderboard_open_prs WHERE org = ?", (org,))
        deleted["leaderboard_open_prs"] = "cleared"
    except Exception as e:
        console.error(f"[AdminReset] Error clearing leaderboard_open_prs: {e}")
        deleted["leaderboard_open_prs"] = f"error: {e}"

    console.log(f"[AdminReset] Cleared leaderboard data for org={org} month={month_key}")
    return deleted


async def _fetch_org_repos(org: str, token: str, limit: int = 10) -> list:
    """Fetch repositories in the organization (most recently updated first).
    
    Args:
        org: Organization name
        token: GitHub API token
        limit: Maximum number of repos to return (default: 10 to prevent subrequest limits)
    """
    # Fetch repos sorted by most recently pushed to reduce API calls for active repos
    resp = await github_api("GET", f"/orgs/{org}/repos?sort=pushed&direction=desc&per_page={limit}", token)
    if resp.status != 200:
        return []
    repos = json.loads(await resp.text())
    return repos[:limit]


async def _calculate_leaderboard_stats(owner: str, repos: list, token: str, window_months: int = 1) -> dict:
    """Calculate leaderboard stats across ALL repositories using GitHub Search API.
    
    This approach uses GitHub's search API to query across all org repos efficiently,
    staying well under Cloudflare's 50 subrequest limit even with 50+ repos.
    
    Args:
        owner: Organization or user name
        repos: List of repository objects (used for repo count, not iteration)
        token: GitHub API token
        window_months: Number of months to look back (default: 1 for monthly)
    
    Returns:
        Dictionary with user stats and sorted leaderboard
    """
    month_key = _month_key()
    start_timestamp, end_timestamp = _month_window(month_key)
    
    # Format date range for search API
    start_date = time.strftime("%Y-%m-%d", time.gmtime(start_timestamp))
    end_date = time.strftime("%Y-%m-%d", time.gmtime(end_timestamp))
    
    user_stats = {}
    
    def ensure_user(login: str):
        if login not in user_stats:
            user_stats[login] = {
                "openPrs": 0,
                "mergedPrs": 0,
                "closedPrs": 0,
                "reviews": 0,
                "comments": 0,
                "total": 0
            }
    
    # Use GitHub Search API to query across ALL repos efficiently
    # This dramatically reduces API calls: ~6 calls total vs 150+ with per-repo approach
    
    # 1. Count open PRs (current state across all repos)
    for pr in await _github_search_issues_paged(owner, token, "is:pr+is:open", max_pages=3):
        if pr.get("user") and not _is_bot(pr["user"]):
            login = pr["user"]["login"]
            ensure_user(login)
            user_stats[login]["openPrs"] += 1
    
    # 2. Fetch merged PRs from this month
    for pr in await _github_search_issues_paged(
        owner,
        token,
        f"is:pr+is:merged+merged:{start_date}..{end_date}",
        max_pages=3,
    ):
        if pr.get("user") and not _is_bot(pr["user"]):
            login = pr["user"]["login"]
            ensure_user(login)
            user_stats[login]["mergedPrs"] += 1
    
    # 3. Fetch closed (not merged) PRs from this month
    for pr in await _github_search_issues_paged(
        owner,
        token,
        f"is:pr+is:closed+is:unmerged+closed:{start_date}..{end_date}",
        max_pages=3,
    ):
        if pr.get("user") and not _is_bot(pr["user"]):
            login = pr["user"]["login"]
            ensure_user(login)
            user_stats[login]["closedPrs"] += 1
    
    # 4. Search for comments in this month across org (optional, budget permitting)
    # Limit to 2 pages to stay under budget
    since_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(start_timestamp))
    # Note: Skipping comment counting to conserve API budget
    # With 50 repos, we need to prioritize PRs and reviews
    
    # 5. Fetch reviews from a sample of merged PRs - budget 15 calls
    # Strategy: Get repo URLs from merged PRs, fetch reviews for top 15 PRs
    max_review_calls = 15
    review_calls_used = 0
    
    # Get merged PRs again (already cached in memory from step 2)
    page = 1
    sampled_prs = []
    while page <= 2 and len(sampled_prs) < max_review_calls:
        resp = await github_api(
            "GET",
            f"/search/issues?q=is:pr+is:merged+org:{owner}+merged:{start_date}..{end_date}&per_page=50&page={page}&sort=updated",
            token
        )
        if resp.status != 200:
            break
        data = json.loads(await resp.text())
        items = data.get("items", [])
        if not items:
            break
        
        # Extract repo and PR number from each PR
        for pr in items:
            if len(sampled_prs) >= max_review_calls:
                break
            # Parse repo from repository_url: /repos/{owner}/{repo}
            repo_url = pr.get("repository_url", "")
            if repo_url:
                parts = repo_url.split("/")
                if len(parts) >= 2:
                    repo_name = parts[-1]
                    pr_number = pr.get("number")
                    if repo_name and pr_number:
                        sampled_prs.append((repo_name, pr_number))
        
        if len(items) < 50:
            break
        page += 1
    
    # Fetch reviews for sampled PRs
    for repo_name, pr_number in sampled_prs:
        if review_calls_used >= max_review_calls:
            break
        
        resp_reviews = await github_api(
            "GET",
            f"/repos/{owner}/{repo_name}/pulls/{pr_number}/reviews",
            token
        )
        review_calls_used += 1
        
        if resp_reviews.status == 200:
            reviews = json.loads(await resp_reviews.text())
            pr_review_count = {}
            
            for review in reviews:
                if review.get("user") and not _is_bot(review["user"]):
                    submitted_at = review.get("submitted_at")
                    if submitted_at:
                        review_ts = _parse_github_timestamp(submitted_at)
                        if start_timestamp <= review_ts <= end_timestamp:
                            login = review["user"]["login"]
                            pr_review_count[login] = pr_review_count.get(login, 0) + 1
            
            # Count only first 2 reviewers per PR to avoid spam
            for login in list(pr_review_count.keys())[:2]:
                ensure_user(login)
                user_stats[login]["reviews"] += 1
    
    # Calculate total scores
    # open: +1, merged: +10, closed: -2, reviews: +5, comments: +2
    for login in user_stats:
        s = user_stats[login]
        s["total"] = (s["openPrs"] * 1) + (s["mergedPrs"] * 10) + (s["closedPrs"] * -2) + (s["reviews"] * 5) + (s["comments"] * 2)
    
    # Sort users by total score, then merged PRs, then reviews, then alphabetically
    sorted_users = sorted(
        [{"login": login, **stats} for login, stats in user_stats.items()],
        key=lambda u: (-u["total"], -u["mergedPrs"], -u["reviews"], u["login"].lower())
    )
    
    return {
        "users": user_stats,
        "sorted": sorted_users,
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp
    }


def _parse_github_timestamp(ts_str: str) -> int:
    """Parse GitHub ISO 8601 timestamp to Unix timestamp."""
    # GitHub timestamps are like: 2024-03-05T12:34:56Z
    import re
    match = re.match(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z", ts_str)
    if match:
        year, month, day, hour, minute, second = map(int, match.groups())
        dt = time.struct_time((year, month, day, hour, minute, second, 0, 0, 0))
        return int(calendar.timegm(dt))
    return 0


def _avatar_img_tag(login: str, size: int = 20) -> str:
    """Return a fixed-size GitHub avatar image tag safe for markdown tables."""
    safe_login = quote(str(login), safe="")
    return (
        f"<img src=\"https://avatars.githubusercontent.com/{safe_login}?size={size}&v=4\" "
        f"width=\"{size}\" height=\"{size}\" alt=\"{login}\" />"
    )


def _format_leaderboard_comment(author_login: str, leaderboard_data: dict, owner: str, note: str = "") -> str:
    """Format a leaderboard comment for a specific user."""
    sorted_users = leaderboard_data["sorted"]
    start_ts = leaderboard_data["start_timestamp"]
    
    # Find author's index
    author_index = -1
    for i, user in enumerate(sorted_users):
        if user["login"] == author_login:
            author_index = i
            break
    
    # Format month display
    month_struct = time.gmtime(start_ts)
    display_month = time.strftime("%B %Y", month_struct)
    
    # Build comment
    comment = LEADERBOARD_MARKER + "\n"
    comment += "## 📊 Monthly Leaderboard\n\n"
    comment += f"Hi @{author_login}! Here's how you rank for {display_month}:\n\n"
    
    # Table header
    comment += "| Rank | User | Open PRs | PRs (merged) | PRs (closed) | Reviews | Comments | Total |\n"
    comment += "| --- | --- | --- | --- | --- | --- | --- | --- |\n"
    
    def row_for(rank: int, u: dict, bold: bool = False, medal: str = "") -> str:
        avatar = _avatar_img_tag(u["login"])
        user_cell = f"{avatar} **`@{u['login']}`** ✨" if bold else f"{avatar} `@{u['login']}`"
        rank_cell = f"{medal} {rank}" if medal else f"{rank}"
        return (f"| {rank_cell} | {user_cell} | {u['openPrs']} | {u['mergedPrs']} | "
                f"{u['closedPrs']} | {u['reviews']} | {u['comments']} | **{u['total']}** |")
    
    # Show context rows around the author
    if not sorted_users:
        # No data yet: show the requesting user with zeroes so the comment is still useful.
        avatar = _avatar_img_tag(author_login)
        comment += f"| - | {avatar} **`@{author_login}`** ✨ | 0 | 0 | 0 | 0 | 0 | **0** |\n"
        comment += "\n_No leaderboard activity has been recorded for this month yet._\n"
    elif author_index == -1:
        # Author not in leaderboard, show top 5
        for i in range(min(5, len(sorted_users))):
            medal = ["🥇", "🥈", "🥉"][i] if i < 3 else ""
            comment += row_for(i + 1, sorted_users[i], False, medal) + "\n"
    else:
        # Show author and neighbors
        if author_index > 0:
            medal = ["🥇", "🥈", "🥉"][author_index - 1] if author_index - 1 < 3 else ""
            comment += row_for(author_index, sorted_users[author_index - 1], False, medal) + "\n"
        
        medal = ["🥇", "🥈", "🥉"][author_index] if author_index < 3 else ""
        comment += row_for(author_index + 1, sorted_users[author_index], True, medal) + "\n"
        
        if author_index < len(sorted_users) - 1:
            comment += row_for(author_index + 2, sorted_users[author_index + 1]) + "\n"
    
    comment += "\n---\n"
    comment += (
        f"**Scoring this month** (across {owner} org): Open PRs (+1 each), Merged PRs (+10), "
        "Closed (not merged) (−2), Reviews (+5; first two per PR in-month), "
        "Comments (+2, excludes CodeRabbit). Run `/leaderboard` on any issue or PR to see your rank!\n"
    )
    if note:
        comment += f"\n> Note: {note}\n"
    
    return comment


def _format_reviewer_leaderboard_comment(leaderboard_data: dict, owner: str, pr_reviewers: list = None) -> str:
    """Format a reviewer leaderboard comment showing top reviewers for the month."""
    sorted_users = leaderboard_data["sorted"]
    start_ts = leaderboard_data["start_timestamp"]

    # Sort users by reviews descending, then alphabetically
    reviewer_sorted = sorted(
        [u for u in sorted_users if u["reviews"] > 0],
        key=lambda u: (-u["reviews"], u["login"].lower()),
    )

    # Format month display
    month_struct = time.gmtime(start_ts)
    display_month = time.strftime("%B %Y", month_struct)

    comment = REVIEWER_LEADERBOARD_MARKER + "\n"
    comment += "## 🔍 Reviewer Leaderboard\n\n"
    comment += f"Top reviewers for {display_month} (across the {owner} org):\n\n"

    medals = ["🥇", "🥈", "🥉"]

    def row_for(rank: int, u: dict, highlight: bool = False) -> str:
        medal = medals[rank - 1] if rank <= 3 else ""
        rank_cell = f"{medal} {rank}" if medal else f"{rank}"
        avatar = _avatar_img_tag(u["login"])
        user_cell = f"{avatar} **`@{u['login']}`** ⭐" if highlight else f"{avatar} `@{u['login']}`"
        return f"| {rank_cell} | {user_cell} | {u['reviews']} |"

    comment += "| Rank | Reviewer | Reviews this month |\n"
    comment += "| --- | --- | --- |\n"

    pr_reviewer_set = set(pr_reviewers or [])

    if not reviewer_sorted:
        comment += "| - | _No review activity recorded yet_ | 0 |\n"
    else:
        total = len(reviewer_sorted)

        # Find the highest-ranked PR reviewer to centre the window on.
        center_idx = None
        if pr_reviewer_set:
            for i, u in enumerate(reviewer_sorted):
                if u["login"] in pr_reviewer_set:
                    center_idx = i
                    break

        if center_idx is not None:
            # Build a window of up to 5 entries with the reviewer in the middle.
            start_idx = center_idx - 2
            end_idx = center_idx + 2
            # Clamp and expand to keep window size = 5 when possible.
            if start_idx < 0:
                end_idx -= start_idx  # shift right
                start_idx = 0
            if end_idx >= total:
                shift = end_idx - total + 1
                start_idx = max(0, start_idx - shift)
                end_idx = total - 1

            if start_idx > 0:
                comment += "| … | … | … |\n"
            for i in range(start_idx, end_idx + 1):
                u = reviewer_sorted[i]
                highlight = u["login"] in pr_reviewer_set
                comment += row_for(i + 1, u, highlight) + "\n"
            if end_idx < total - 1:
                comment += "| … | … | … |\n"
        else:
            # No PR reviewer identified - show top 5.
            for i, u in enumerate(reviewer_sorted[:5]):
                highlight = u["login"] in pr_reviewer_set
                comment += row_for(i + 1, u, highlight) + "\n"

    comment += "\n---\n"
    comment += (
        "Reviews earn **+5 points** each in the monthly leaderboard "
        "(first two reviewers per PR). Thank you to everyone who helps review PRs! 🙏\n"
    )
    return comment


async def _post_reviewer_leaderboard(owner: str, repo: str, pr_number: int, token: str, env=None, pr_reviewers: list = None) -> None:
    """Post or update a reviewer leaderboard comment on a merged PR."""
    leaderboard_data = None
    if env is not None:
        leaderboard_data = await _calculate_leaderboard_stats_from_d1(owner, env)
    if leaderboard_data is None:
        # Fallback: build minimal data from GitHub API is expensive; skip if unavailable.
        console.log(f"[ReviewerLeaderboard] No D1 data available for {owner}; skipping reviewer leaderboard")
        return

    comment_body = _format_reviewer_leaderboard_comment(leaderboard_data, owner, pr_reviewers)

    # Snapshot existing marker comments before posting so cleanup never removes
    # the freshly created comment.
    existing_comments, list_failed = await _fetch_issue_comments_paged(owner, repo, pr_number, token)
    snapshot_ids = []
    if list_failed:
        console.error(
            f"[ReviewerLeaderboard] Failed to list comments for {owner}/{repo}#{pr_number}; skipping cleanup snapshot"
        )
    else:
        for c in existing_comments:
            if REVIEWER_LEADERBOARD_MARKER not in (c.get("body") or ""):
                continue
            comment_id = int(c.get("id") or 0)
            if comment_id > 0:
                snapshot_ids.append(comment_id)

    created = await _create_comment_strict(owner, repo, pr_number, comment_body, token)
    if created is False:
        console.error(f"[ReviewerLeaderboard] Failed to post reviewer leaderboard for {owner}/{repo}#{pr_number}")
        return

    # Delete only snapshot comments so the newly posted one is preserved.
    for comment_id in snapshot_ids:
        delete_resp = await github_api(
            "DELETE",
            f"/repos/{owner}/{repo}/issues/comments/{comment_id}",
            token,
        )
        if delete_resp.status not in (204, 200):
            console.error(
                f"[ReviewerLeaderboard] Failed to delete old reviewer leaderboard comment {comment_id} "
                f"for {owner}/{repo}#{pr_number}: status={delete_resp.status}"
            )

    console.log(f"[ReviewerLeaderboard] Posted reviewer leaderboard for {owner}/{repo}#{pr_number}")


async def _fetch_leaderboard_data(owner: str, repo: str, token: str, env=None) -> tuple:
    """Fetch leaderboard data for *owner*, running D1 backfill when available.

    Returns a ``(leaderboard_data, leaderboard_note, is_org)`` tuple where
    ``leaderboard_data`` is the dict expected by ``_format_leaderboard_comment``
    and ``leaderboard_note`` is an optional informational string about backfill
    progress (may be empty).  ``is_org`` indicates whether *owner* is a GitHub
    organisation (used by callers that need to choose comment wording).
    """
    leaderboard_note = ""
    owner_data = None
    is_org = False
    reconciled = True
    settings = _reconcile_settings(env)

    owner_resp = await github_api("GET", f"/users/{owner}", token)
    if owner_resp.status == 200:
        owner_data = json.loads(await owner_resp.text())
        is_org = owner_data.get("type") == "Organization"
        console.log(f"[Leaderboard] Owner {owner} is_org={is_org}")
    else:
        console.error(f"[Leaderboard] Owner lookup failed for {owner}: status={owner_resp.status}")

    # For org leaderboard requests, always reconcile from GitHub first so stale
    # webhook deltas cannot drift permanently.
    if is_org and _d1_binding(env):
        try:
            reconciled = await _reconcile_org_leaderboard_from_github(
                owner,
                token,
                env,
                deadline_ts=time.time() + settings["timeout_seconds"],
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as exc:
            console.error(f"[Leaderboard] Live reconciliation failed for {owner}: {exc}")
            reconciled = False
        leaderboard_note = "" if reconciled else "Live reconciliation is temporarily unavailable; showing last known snapshot."

    # Prefer D1-backed stats after reconciliation.
    leaderboard_data = await _calculate_leaderboard_stats_from_d1(owner, env)
    console.log(f"[Leaderboard] D1 data ready: {bool(leaderboard_data)}, has_users={bool(leaderboard_data and leaderboard_data.get('sorted')) if leaderboard_data else False}")

    # If reconciliation failed and D1 has no users yet, trigger API fallback below
    # so /leaderboard never appears empty after deleting the command comment.
    if (
        leaderboard_data is not None
        and not leaderboard_data.get("sorted")
        and is_org
        and _d1_binding(env)
        and not reconciled
    ):
        console.log(f"[Leaderboard] D1 snapshot empty after failed reconcile for {owner}; falling back to API stats")
        leaderboard_data = None

    # Fallback to API-based calculation when D1 is unavailable.
    if leaderboard_data is None:
        if owner_data is None:
            resp = await github_api("GET", f"/users/{owner}", token)
            if resp.status == 200:
                owner_data = json.loads(await resp.text())
                is_org = owner_data.get("type") == "Organization"
        if is_org:
            repos = await _fetch_org_repos(owner, token)
        else:
            repos = [{"name": repo}]
        leaderboard_data = await _calculate_leaderboard_stats(owner, repos, token)

    return leaderboard_data, leaderboard_note, is_org


async def _post_or_update_leaderboard(owner: str, repo: str, issue_number: int, author_login: str, token: str, env=None) -> None:
    """Post or update a leaderboard comment on an issue/PR."""
    console.log(f"[Leaderboard] Starting leaderboard post for {owner}/{repo}#{issue_number} by @{author_login}")

    leaderboard_data, leaderboard_note, is_org = await _fetch_leaderboard_data(owner, repo, token, env)

    if leaderboard_data is None:
        console.error(f"[Leaderboard] Owner lookup failed for {owner}; cannot post leaderboard")
        await _create_comment_best_effort(
            owner,
            repo,
            issue_number,
            f"@{author_login} I couldn't load leaderboard data right now (owner lookup failed). Please try again shortly.",
            token,
        )
        return
    
    # Format comment
    comment_body = _format_leaderboard_comment(author_login, leaderboard_data, owner, leaderboard_note)
    
    # Delete existing leaderboard comment(s) and old /leaderboard command comments,
    # then create a fresh leaderboard comment.
    comments, list_failed = await _fetch_issue_comments_paged(owner, repo, issue_number, token)
    if list_failed:
        console.error(
            f"[Leaderboard] Failed to list comments for {owner}/{repo}#{issue_number}; posting new leaderboard anyway"
        )

    created = await _create_comment_strict(owner, repo, issue_number, comment_body, token)
    if not created:
        console.error(
            f"[Leaderboard] New leaderboard comment failed for {owner}/{repo}#{issue_number}; skipping cleanup deletes"
        )
        return

    if comments:
        for c in comments:
            body = c.get("body") or ""
            is_old_board = LEADERBOARD_MARKER in body
            is_command_comment = _extract_command(body) == LEADERBOARD_COMMAND
            if is_old_board or is_command_comment:
                delete_resp = await github_api(
                    "DELETE",
                    f"/repos/{owner}/{repo}/issues/comments/{c['id']}",
                    token,
                )
                if delete_resp.status not in (204, 200):
                    console.error(
                        f"[Leaderboard] Failed to delete old leaderboard/command comment {c['id']} "
                        f"for {owner}/{repo}#{issue_number}: status={delete_resp.status}"
                    )
    elif not list_failed:
        console.log(f"[Leaderboard] No existing comments found for cleanup on {owner}/{repo}#{issue_number}")

    console.log(f"[Leaderboard] Posted leaderboard comment for {owner}/{repo}#{issue_number} (requested by @{author_login})")


async def _check_and_close_excess_prs(owner: str, repo: str, pr_number: int, author_login: str, token: str) -> bool:
    """Check if author has too many open PRs and close if needed.
    
    Returns:
        True if PR was closed, False otherwise
    """
    # Search for open PRs by this author
    resp = await github_api(
        "GET",
        f"/search/issues?q=repo:{owner}/{repo}+is:pr+is:open+author:{author_login}&per_page=100",
        token
    )
    
    if resp.status != 200:
        return False
    
    data = json.loads(await resp.text())
    open_prs = data.get("items", [])
    
    # Exclude the current PR from count
    pre_existing_count = len([pr for pr in open_prs if pr["number"] != pr_number])
    
    if pre_existing_count >= MAX_OPEN_PRS_PER_AUTHOR:
        # Close the PR
        msg = (
            f"Hi @{author_login}, thanks for your contribution!\n\n"
            f"This PR is being auto-closed because you currently have {pre_existing_count} "
            f"open PRs in this repository (limit: {MAX_OPEN_PRS_PER_AUTHOR}).\n"
            "Please finish or close some existing PRs before opening new ones.\n\n"
            "If you believe this was closed in error, please contact the maintainers."
        )
        
        await _create_comment_best_effort(owner, repo, pr_number, msg, token)
        
        await github_api(
            "PATCH",
            f"/repos/{owner}/{repo}/pulls/{pr_number}",
            token,
            {"state": "closed"}
        )
        
        return True
    
    return False


async def _check_rank_improvement(owner: str, repo: str, pr_number: int, author_login: str, token: str) -> None:
    """Check if author's rank improved and post congratulatory message."""
    # Get org repos
    resp = await github_api("GET", f"/users/{owner}", token)
    if resp.status != 200:
        return
    
    owner_data = json.loads(await resp.text())
    is_org = owner_data.get("type") == "Organization"
    
    if is_org:
        repos = await _fetch_org_repos(owner, token)
    else:
        repos = [{"name": repo}]
    
    # Calculate 6-month window
    now = int(time.time())
    six_months_ago = now - (6 * 30 * 24 * 60 * 60)  # Approximate
    
    # Count merged PRs in 6-month window for all users
    merged_prs_per_author = {}
    
    # Limit repos to prevent subrequest errors
    for repo_obj in repos[:10]:
        repo_name = repo_obj["name"]
        resp = await github_api(
            "GET",
            f"/repos/{owner}/{repo_name}/pulls?state=closed&per_page=30&sort=updated&direction=desc",
            token
        )
        
        if resp.status == 200:
            prs = json.loads(await resp.text())
            for pr in prs:
                if pr.get("merged_at"):
                    merged_ts = _parse_github_timestamp(pr["merged_at"])
                    if merged_ts >= six_months_ago:
                        pr_author = pr.get("user")
                        if pr_author and not _is_bot(pr_author):
                            login = pr_author["login"]
                            merged_prs_per_author[login] = merged_prs_per_author.get(login, 0) + 1
    
    author_count = merged_prs_per_author.get(author_login, 0)
    
    if author_count == 0:
        return
    
    # Calculate new rank (number of users with more PRs + 1)
    new_rank = len([c for c in merged_prs_per_author.values() if c > author_count]) + 1
    
    # Calculate old rank (before this merge)
    prev_count = author_count - 1
    old_rank = None
    if prev_count > 0:
        old_rank = len([c for c in merged_prs_per_author.values() if c > prev_count]) + 1
    
    # Check if rank improved
    rank_improved = old_rank is None or new_rank < old_rank
    
    if not rank_improved:
        return
    
    # Post congratulatory message
    if old_rank is None:
        msg = (
            f"🎉 Congratulations @{author_login}! "
            f"You've entered the BLT PR leaderboard at **rank #{new_rank}** with this merged PR! "
            "Keep up the great work! 🚀"
        )
    else:
        msg = (
            f"🎉 Congratulations @{author_login}! "
            f"This merged PR has moved you up to **rank #{new_rank}** on the BLT PR leaderboard "
            f"(up from #{old_rank})! Keep up the great work! 🚀"
        )
    
    await _create_comment_best_effort(owner, repo, pr_number, msg, token)


# ---------------------------------------------------------------------------
# Mentor Pool — Configuration, Selection, and Command Handlers
# ---------------------------------------------------------------------------

