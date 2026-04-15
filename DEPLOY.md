# Deployment Guide — BLT-Pool

## Deploying to Cloudflare

```bash
npm run deploy
```

This runs three steps in order:
1. `scripts/check_no_runtime_ddl.sh` — fails fast if runtime DDL has crept back into Python source
2. `scripts/run-migrations.sh` — applies any pending SQL migrations to the D1 database on Cloudflare
3. `wrangler deploy` — deploys the updated worker code

## ⚠️ Why order matters

`_ensure_leaderboard_schema()` and `_ensure_tables()` are no-op shims — schema is managed exclusively via migration files. If the worker deploys before migrations run on a fresh D1 instance, all table operations will fail immediately.

## Other useful commands

```bash
# Apply migrations only
npm run migrations:apply          # or: bash scripts/run-migrations.sh

# List applied/pending migrations
npm run migrations:list

# Check no runtime DDL has crept back into source
npm run ddl:check

# Local development
npm run deploy:local
```

## Required credentials

- `CLOUDFLARE_API_TOKEN`
- `CLOUDFLARE_ACCOUNT_ID`

## Migration files

| File | Purpose |
|------|---------|
| `migrations/0000_initial_schema.sql` | Full baseline schema for all tables |
| `migrations/0001_backfill_referred_by.sql` | Backfill `referred_by` + `contributor_referrals` for all contributors |
