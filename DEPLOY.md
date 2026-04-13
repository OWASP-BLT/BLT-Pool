# Deployment Guide — BLT-Pool

## ⚠️ Required Deployment Order

Since `_ensure_leaderboard_schema()` and `_ensure_tables()` are now **no-op
shims** (schema is managed exclusively via Wrangler D1 migrations), the
database schema **must be applied before** the worker code is deployed.

Deploying the worker first to a fresh D1 instance will cause immediate
failures on all table-dependent operations.

### Correct order

```bash
# 1. Apply all migrations to D1 first
wrangler d1 migrations apply LEADERBOARD_DB

# 2. Then deploy the worker
wrangler deploy
```

### For existing instances (upgrade)

```bash
# Apply any new migrations
wrangler d1 migrations apply LEADERBOARD_DB

# Deploy updated worker
wrangler deploy
```

### Verifying migrations applied

```bash
wrangler d1 execute LEADERBOARD_DB --command "SELECT name FROM sqlite_master WHERE type='table';"
```

All expected tables should appear:
- `leaderboard_monthly_stats`
- `leaderboard_open_prs`
- `leaderboard_pr_state`
- `leaderboard_review_credits`
- `leaderboard_backfill_state`
- `leaderboard_backfill_repo_done`
- `mentor_assignments`
- `mentors`
- `mentor_stats_cache`
- `contributor_referrals`
- `leaderboard_processed_comments`

### Migration files

| File | Purpose |
|------|---------|
| `migrations/0000_initial_schema.sql` | Full baseline schema for all tables |
| `migrations/0001_backfill_referred_by.sql` | Backfill `referred_by` + `contributor_referrals` for 30 contributors/mentors |
