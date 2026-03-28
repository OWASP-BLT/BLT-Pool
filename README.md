# BLT-Pool

BLT-Pool is the OWASP BLT contributor platform running on a Python Cloudflare Worker.

This repository provides two connected products:

- BLT-Pool web experience: mentor directory, active mentorship view, referral leaderboard, and mentor onboarding form.
- GitHub App automation: issue assignment, mentor workflows, leaderboard tracking, PR quality labels, and merge-time summaries.
- Admin dashboard: secure mentor moderation panel (publish/block/delete) with session auth and D1-backed state.

## What Is Implemented

### Issue Automation

- `/assign`: self-assign an issue for 8 hours.
- `/unassign`: release your issue assignment.
- `/leaderboard`: post the current monthly contributor ranking.
- New issue welcome comment is posted on `issues.opened`.
- Bug/security labels (`bug`, `vulnerability`, `security`) are reported to BLT API.
- Stale contributor assignments are auto-released by cron.

### Mentor Workflow

- `/mentor`: request a mentor.
- `/unmentor`: remove mentor assignment (issue author, assigned mentor, or maintainer).
- `/mentor-pause`: mentor requests temporary pause.
- `/handoff`: assigned mentor transfers mentorship.
- `/rematch`: contributor requests a different mentor.
- `needs-mentor` label can trigger automatic mentor assignment.
- Security-sensitive issues (`security`, `vulnerability`, `security-sensitive`, `private-security`) bypass mentor auto-assignment.
- Stale mentor assignments are released after 14 days of no human activity.

Mentor matching behavior:

- Prefers active mentors with available capacity.
- Uses issue label specialties when possible.
- Uses load-aware selection with deterministic tie-breaking.
- Tracks active mentor assignments in D1 for homepage display.

### Pull Request Automation

- Excess PR protection: new PR is auto-closed when the author already has 50 or more open PRs in the same repository.
- If a PR closes a mentored issue, the assigned mentor is requested as reviewer.
- PRs are labeled for peer review status:
	- `has-peer-review`
	- `needs-peer-review`
- PRs are labeled for unresolved review threads:
	- `unresolved-conversations: N` (green/red depending on count)
- PRs are labeled for queued checks:
	- `N checks pending`
- On merged PRs, one combined comment is posted with:
	- merge congratulations
	- contributor leaderboard snippet
	- reviewer leaderboard snippet

Round-robin mentor reviewer mode:

- The code includes round-robin reviewer assignment helper logic.
- The runtime path checks `MENTOR_AUTO_PR_REVIEWER_ENABLED`, and the helper is additionally gated by the module constant `MENTOR_AUTO_PR_REVIEWER_ENABLED = False` in `src/worker.py`.

### Leaderboard Model (D1)

Leaderboard scoring is event-driven and computed from D1 counters:

- Open PR: +1
- Merged PR: +10
- Closed unmerged PR: -2
- Review credit: +5 (first two unique reviewers per PR/month)
- Comment credit: +2 (bots and CodeRabbit pings excluded)

Data is maintained via webhook events plus incremental backfill tables.

### Web Experience

Homepage (`GET /`) includes:

- live mentors list from D1 `mentors`
- per-mentor activity stats
- active mentor assignments (from D1 `mentor_assignments`)
- referral leaderboard based on `referred_by`
- mentor command quick guide
- mentor signup form that posts to `POST /api/mentors`

Mentor signup API behavior (`POST /api/mentors`):

- Validates display name, GitHub username, specialties, timezone, and optional `referred_by`.
- Attempts to verify GitHub usernames via GitHub API (submitter and optional referrer), with fail-open behavior on transient API/network errors.
- Stores the mentor in D1 `mentors`.
- Auto-sets initial `active` state based on whether the mentor has at least one merged PR in `GITHUB_ORG`.
	- If no merged PR is found, record is stored inactive and can later be published from the admin dashboard.

GitHub App landing (`GET /github-app`) shows install URL and env status dashboard.

### Admin Dashboard

Admin UI and auth are implemented in `src/services/admin/service.py` and wired through `AdminService(env).handle(request)` in `src/worker.py`.

Implemented behavior:

- First-run bootstrap via `GET/POST /admin/signup`:
	- Allows creating the first admin user only once.
	- Enforces username format and password rules.
- Login/logout flow via `GET/POST /admin/login` and `GET /admin/logout`.
- Session auth:
	- Cookie name: `blt_admin_session`
	- Session TTL: 7 days
	- Session records are stored in D1 and expired sessions are cleaned automatically.
- Dashboard view at `GET /admin`:
	- Mentor totals (total/published/blocked/assignments)
	- Mentor table with specialties, capacity, timezone, referral source, assignment count.
- Mentor moderation actions via `POST /admin/mentors/action`:
	- `publish` (set active)
	- `block` (set inactive)
	- `delete` (remove mentor and clear mentor assignment records)

Important distinction:

- `POST /admin/reset-leaderboard-month` is intentionally **not** session-authenticated in `AdminService`.
- It is protected separately in `src/worker.py` using bearer `ADMIN_SECRET`.

## HTTP Endpoints

### Public Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | BLT-Pool homepage with mentor directory and active assignments |
| `GET` | `/github-app` | GitHub App landing page |
| `GET` | `/health` | Health check (`{"status":"ok","service":"BLT-Pool"}`) |
| `GET` | `/callback` | Post-install callback page |
| `POST` | `/api/mentors` | Add mentor to D1-backed pool (validated) |
| `POST` | `/api/github/webhooks` | GitHub webhook receiver |

### Admin Endpoints

Admin routes are handled by `src/services/admin/service.py` and session-backed in D1.

| Method | Path | Description |
|---|---|---|
| `GET`, `POST` | `/admin/signup` | Create first admin account |
| `GET`, `POST` | `/admin/login` | Admin login |
| `GET` | `/admin/logout` | End admin session |
| `GET` | `/admin` | Admin dashboard (requires valid admin session cookie) |
| `POST` | `/admin/mentors/action` | Publish, block, or delete mentor records |
| `POST` | `/admin/reset-leaderboard-month` | Reset month data (requires bearer `ADMIN_SECRET`) |

## Environment Variables

### Required Secrets

| Variable | Purpose |
|---|---|
| `APP_ID` | GitHub App numeric ID |
| `PRIVATE_KEY` | GitHub App private key (PEM/PKCS#1/PKCS#8) |
| `WEBHOOK_SECRET` | GitHub webhook signature verification secret |

### Config Vars (`wrangler.toml [vars]`)

| Variable | Default | Purpose |
|---|---|---|
| `BLT_API_URL` | `https://github-app.owaspblt.org` | BLT API base URL (webhook fallback in code: `https://blt-api.owasp-blt.workers.dev`) |
| `GITHUB_ORG` | `OWASP-BLT` | Org used for mentor stats and checks |
| `GITHUB_APP_SLUG` | `blt-pool` | Install URL slug on `/github-app` |

### Optional Secrets

| Variable | Purpose |
|---|---|
| `GITHUB_TOKEN` | Higher GitHub API rate-limit for user lookups and merged-PR checks |
| `ADMIN_SECRET` | Bearer token for `/admin/reset-leaderboard-month` |
| `GITHUB_CLIENT_ID` | Optional status-display-only variable on `/github-app` |
| `GITHUB_CLIENT_SECRET` | Optional status-display-only variable on `/github-app` |
| `MENTOR_AUTO_PR_REVIEWER_ENABLED` | Runtime flag checked by PR open path for round-robin reviewer attempt |

### Admin Auth Notes

- Admin login itself does not require `ADMIN_SECRET`.
- Admin auth requires a working D1 binding (`LEADERBOARD_DB`) because users/sessions are persisted there.
- Without D1 configured, `/admin*` routes return an admin-unavailable response.

## D1 Data Model Overview

The codebase stores more than leaderboard counters in D1. Current usage includes:

- Leaderboard/event tables (monthly stats, open PRs, PR state, review credits, backfill state)
- Mentor pool tables:
	- `mentors`
	- `mentor_assignments`
- Admin/auth tables:
	- `admin_users`
	- `admin_sessions`

These tables are created on-demand by schema/bootstrap helpers in `src/worker.py` and `src/services/admin/service.py`.

## GitHub App Permissions and Events

### Permissions (`app.yml`)

| Permission | Access |
|---|---|
| Issues | Read & Write |
| Pull Requests | Read & Write |
| Metadata | Read |
| Checks | Read |
| Actions | Read |

### Default Subscribed Events (`app.yml`)

- `issue_comment`
- `issues`
- `pull_request`
- `pull_request_review`
- `check_run`
- `workflow_run`

Note:

- The webhook router in `src/worker.py` also contains handlers for `pull_request_review_comment` and `pull_request_review_thread`.
- These two events are not in `app.yml` default events in the current codebase.

## Setup

### Prerequisites

- Cloudflare Workers account
- GitHub App installation
- Node.js (for Wrangler CLI)
- Python (for tests)

### Local Setup

```bash
cp .dev.vars.example .dev.vars
npx wrangler dev
```

### D1 Setup

```bash
npx wrangler d1 create blt-leaderboard
```

Copy the returned `database_id` into `wrangler.toml` in `[[d1_databases]]`.

### Deploy

```bash
npx wrangler secret put APP_ID
npx wrangler secret put PRIVATE_KEY
npx wrangler secret put WEBHOOK_SECRET
npx wrangler deploy
```

Optional bulk secret helper:

```bash
chmod +x scripts/upload-production-vars.sh
./scripts/upload-production-vars.sh
```

## Testing

```bash
pip install pytest
pytest test_worker.py -v
```

## Project Structure

```text
.
├── README.md
├── app.yml
├── wrangler.toml
├── .dev.vars.example
├── public/
├── scripts/
├── src/
│   ├── worker.py
│   ├── index_template.py
│   └── services/
│       ├── admin/
│       │   ├── __init__.py
│       │   └── service.py
│       └── mentor_seed.py
├── templates/
│   ├── index.html
│   └── callback.html
└── test_worker.py
```

## Related Projects

- [OWASP BLT](https://github.com/OWASP-BLT/BLT)
- [BLT-API](https://github.com/OWASP-BLT/BLT-API)
- [BLT-Action](https://github.com/OWASP-BLT/BLT-Action)

## License

[AGPL-3.0](LICENSE)
