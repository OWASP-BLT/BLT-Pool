# Deployment Guide — BLT-Pool

## Deploying to Cloudflare

Migrations run on Cloudflare's infrastructure via Wrangler — not through
GitHub Actions or any external CI. The `npm run deploy` script handles
both steps in the correct order:

```bash
npm run deploy
```

This runs:
1. `wrangler d1 migrations apply LEADERBOARD_DB --remote` — applies any
   pending SQL migrations to the D1 database on Cloudflare
2. `wrangler deploy` — deploys the updated worker code

## ⚠️ Why order matters

`_ensure_leaderboard_schema()` and `_ensure_tables()` are now **no-op
shims** — they no longer create tables at runtime. Schema is managed
exclusively via migrations. If the worker deploys before migrations run
on a fresh D1 instance, all table operations will fail immediately.

`npm run deploy` guarantees migrations always run first.

## Other useful commands

```bash
# Apply migrations only (without deploying worker)
npm run migrations:apply

# List applied/pending migrations
npm run migrations:list

# Check no runtime DDL has crept back into source
npm run ddl:check

# Local development (applies migrations to local D1 + starts dev server)
npm run deploy:local
```

## Required credentials

Set these environment variables (or use `wrangler login`):
- `CLOUDFLARE_API_TOKEN`
- `CLOUDFLARE_ACCOUNT_ID`

## Migration files

| File | Purpose |
|------|---------|
| `migrations/0000_initial_schema.sql` | Full baseline schema for all tables |
| `migrations/0001_backfill_referred_by.sql` | Backfill `referred_by` + `contributor_referrals` for all contributors |
