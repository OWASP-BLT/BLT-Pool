# Deployment Guide — BLT-Pool Worker

## ⚠️ Critical Deployment Order

As of the migration introduced in PR #120, the D1 database schema is managed
exclusively via Wrangler migration files under `migrations/`. The worker no
longer creates tables at runtime.

**You must apply migrations BEFORE deploying the worker to any environment
(including fresh D1 instances).** If the worker deploys first, all
table-dependent operations will fail immediately.

### Correct deployment order

```bash
# Step 1 — Apply all pending migrations to D1
wrangler d1 migrations apply LEADERBOARD_DB

# Step 2 — Deploy the worker
wrangler deploy
```

### For fresh environments (first-time setup)

```bash
# Creates all tables via migrations/0000_initial_schema.sql
wrangler d1 migrations apply LEADERBOARD_DB --env production

# Then backfills referred_by and contributor_referrals
# (handled automatically by 0001_backfill_referred_by.sql)

# Then deploy
wrangler deploy --env production
```

### Checking migration status

```bash
# List applied migrations
wrangler d1 migrations list LEADERBOARD_DB

# Check which migrations are pending
wrangler d1 migrations list LEADERBOARD_DB --env production
```

## Migration files

| File | Purpose |
|------|---------|
| `migrations/0000_initial_schema.sql` | Full baseline schema — all 11 tables |
| `migrations/0001_backfill_referred_by.sql` | Backfill `referred_by` for 26 mentors/contributors + `contributor_referrals` rows |

## CI guard

`scripts/check_no_runtime_ddl.sh` enforces that no `CREATE TABLE` or
`ALTER TABLE` statements exist in `src/**/*.py`. Run it in CI before every
deploy to ensure migrations remain the single source of truth.

```bash
bash scripts/check_no_runtime_ddl.sh
```
