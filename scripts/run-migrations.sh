#!/usr/bin/env bash
# run-migrations.sh — apply pending D1 migrations to Cloudflare
set -euo pipefail
wrangler d1 migrations apply LEADERBOARD_DB --remote
