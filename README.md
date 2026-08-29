# Ingestion Service

A Go cron job that pulls posts from whitelisted Instagram Business/Creator accounts through the Business Discovery API and lands them in Postgres as raw data.

**Write-side only.** There is no public HTTP API. The two endpoints it does expose are `/healthz` for container health checks and an admin-only manual ingest trigger.

## Responsibility

1. Read the IG account whitelist from `ig_sources`
2. Call Business Discovery once per account
3. Upsert the results into `ig_raw_posts`, idempotently
4. Record the outcome in `ingestion_runs`

Raw data is captured in full before anything is interpreted. Deciding which post is an event, parsing Thai captions, geocoding — all of that happens in a later phase, reading from `ig_raw_posts` without re-hitting the API.

## Schema ownership

This service **does not migrate the database**. The three tables it uses are owned by [Core-Service](../Core-Service), which runs the Prisma migrations. Start Core-Service's `prisma migrate deploy` before booting this service.

The database user configured here needs write access only — no DDL rights.

## Layout

The layering mirrors `blaze-backend`: **Controller → Service → Repository**, with providers for external APIs and manual dependency wiring in one readable file.

```
cmd/ingestion/main.go                  entrypoint: config, db, startup checks, signals
internal/config/                       typed Configurations + env loader with validation
internal/constants/                    Environment enum
internal/logging/logger.go             TLogger (slog) with Layer enum and run_id support
internal/errors/errors.go              TError hierarchy
internal/libs/                         singletons: postgres pool, slog handler
internal/providers/instagram/          Business Discovery client + error classification
internal/providers/googlechat/         fatal-error alerting webhook
internal/repositories/igsource/        ig_sources queries
internal/repositories/igrawpost/       ig_raw_posts upsert
internal/repositories/ingestionrun/    ingestion_runs audit log
internal/services/ingestion/           the orchestrator: worker pool, retries, counters
internal/services/system/              health check
internal/controllers/middleware.go     admin API key authentication
internal/controllers/system/           GET /healthz
internal/controllers/ingestion/        POST /api/v1/admin/ingest/instagram
internal/cron/cron.go                  scheduler (Asia/Bangkok)
internal/routes/routes.go              manual dependency wiring, mirrors routes.ts
```

### Where this differs from the spec's suggested layout

The spec sketched `internal/instagram/`, `internal/store/`, `internal/ingest/`. Those became `internal/providers/instagram/`, `internal/repositories/*/`, and `internal/services/ingestion/` so the naming matches `blaze-backend`. The components and their responsibilities are unchanged.

Two other deliberate deviations from the spec, both noted where they occur in the code:

- **`migrations/` is gone.** Schema ownership moved to Core-Service.
- **The access token travels in an `Authorization: Bearer` header**, not the `access_token` query parameter the spec shows. The Graph API accepts both; the header keeps the token out of access logs and proxy caches.

## Configuration

Everything comes from the environment and is validated at startup — a missing variable is a boot failure with a readable message, never a silent 3am cron failure. See [.env.example](.env.example).

| Env | Example | Notes |
|---|---|---|
| `DATABASE_URL` | `postgres://...` | write-capable user |
| `IG_ACCESS_TOKEN` | `EAAG...` | long-lived token (60 days) |
| `IG_USER_ID` | `1784...` | our own IG Business account |
| `IG_API_VERSION` | `v25.0` | pinned, never the default |
| `IG_MEDIA_LIMIT` | `25` | recent posts per account |
| `CRON_SCHEDULE` | `0 */6 * * *` | every 6 hours |
| `WORKER_CONCURRENCY` | `3` | sources fetched in parallel |
| `RUN_ON_STARTUP` | `false` | for dev / manual triggering |
| `GOOGLE_CHAT_WEBHOOK_URL` | | optional; empty disables alerting |
| `ADMIN_API_KEY` | | optional; empty disables the manual trigger. Min 32 chars |
| `PORT` | `8082` | health endpoint |
| `ENV` | `local` | `local` logs as text, `dev`/`prod` as JSON |

## HTTP endpoints

### `GET /healthz`

200 when the database is reachable, 503 otherwise. For container health checks only.

### `POST /api/v1/admin/ingest/instagram`

Runs an ingestion pass on demand, for when waiting up to six hours for the next cron tick is not an option.

Guarded by the `x-api-key` header, compared against `ADMIN_API_KEY` in constant time — the same pattern `blaze-backend` uses for its admin routes. **If `ADMIN_API_KEY` is unset the route is not registered at all**, and a warning says so at startup. Registering a mutating endpoint without authentication would be worse than not having it.

Asynchronous by default: an ingestion pass takes far longer than a request should, so the trigger acknowledges and runs in the background.

```bash
curl -X POST http://localhost:8082/api/v1/admin/ingest/instagram -H "x-api-key: $ADMIN_API_KEY"
```

```json
{ "status": "accepted", "message": "Ingestion run started. Watch the ingestion_runs table or the logs for the outcome." }
```

Add `?wait=true` to block until the run finishes and get the counters back. This is what makes the idempotency check easy to verify — run it twice, and `posts_new` must be `0` the second time:

```bash
curl -X POST "http://localhost:8082/api/v1/admin/ingest/instagram?wait=true" -H "x-api-key: $ADMIN_API_KEY"
```

```json
{
  "run_id": 42,
  "status": "partial",
  "sources_total": 87,
  "sources_ok": 85,
  "sources_failed": 2,
  "posts_new": 31,
  "posts_updated": 104
}
```

| Status | Meaning |
|---|---|
| `202` | run started in the background |
| `200` | run finished (`wait=true`); the body's `status` field says how it went |
| `401` | missing or wrong `x-api-key` |
| `409` | a run is already in progress — the cron and the trigger share one run slot |
| `500` | the run could not start at all, e.g. the database was unreachable |

A finished run whose `status` is `failed` still comes back as `200`: the request succeeded, and the counters are worth reading. Only a run that never started is a `500`.

Synchronous runs are capped at 15 minutes. Both modes are cancelled on shutdown, and shutdown waits for an in-flight manual run so its `ingestion_runs` row does not stay stuck on `running`.

## Error handling

Failures are classified into three kinds because each one demands a different response:

| Kind | Meta error codes | Response |
|---|---|---|
| **Permanent** | `24`, `110` | deactivate the source, record `last_error`, never retry |
| **Transient** | `1`, `2`, HTTP 5xx, timeouts | retry with exponential backoff, up to 3 retries |
| **Fatal** | `4`, `17`, `32` (rate limit), `190` (expired token), `10` and `200`-`299` (permission) | abort the whole run, mark it `failed`, raise an alert |

Rate limiting aborts the entire run rather than skipping one source: continuing would only collect 429s across the board and lengthen the cooldown.

Permission errors are fatal rather than permanent, which is the non-obvious one. Code `10` and the `200`-`299` block describe **our app or token**, not the account being looked up, so they fail identically for every source. Treating one as permanent would walk the whitelist deactivating healthy accounts one at a time. Note the startup token check does not catch these: the token is valid, it just lacks a scope.

Unrecognised error codes are treated as **transient**. Retrying and letting `consecutive_failures` build up is far safer than deactivating a healthy source on the first unfamiliar error.

A source that fails **5 runs in a row** is auto-deactivated, so a dead account stops burning rate-limit quota. Sources cut short by a fatal abort are explicitly exempt from that counter — otherwise a couple of rate-limited runs would deactivate an entire healthy whitelist.

## Observability

Every log line inside a run carries `run_id`:

```
INFO  ingestion run started    run_id=42 sources=87
WARN  source failed            run_id=42 username=xxx kind=transient attempt=2
ERROR source deactivated       run_id=42 username=yyy reason="not a business account"
INFO  ingestion run finished   run_id=42 status=partial ok=85 failed=2 new=31 updated=104
```

## Token management

The long-lived token lasts 60 days and can be renewed **before** it expires. Let it lapse and the whole OAuth flow has to be redone.

Auto-refresh is a later phase. For now:

- **Startup check** — `GET /me?fields=id` runs at boot. A broken token exits immediately with a clear message.
- **Fatal alert** — error code `190` sends a Google Chat notification saying a new OAuth flow is required.

## Known trap: `media_url` expires

The `media_url` Instagram returns is a **signed CDN URL that expires within days**. Storing it is fine; serving it directly to a browser is not — the images will break later. Before the frontend ships, either mirror the images to our own object storage or re-fetch the URL at render time.

## Rate limits

Business Discovery falls under Platform Rate Limiting (not the tighter Business Use Case limit). One source costs one API call: 100 sources every 6 hours is ~400 calls/day, far below the ceiling.

Past roughly 150 sources — or a schedule tighter than 6 hours — switch to a tiered schedule instead: frequent posters every 6 hours, everything else once a day, driven by a new `sync_interval_hours` column on `ig_sources`.

## Development

```bash
go build ./...
go test ./...
go test -race ./...
go run ./cmd/ingestion
```

Set `RUN_ON_STARTUP=true` to trigger a run immediately instead of waiting for the schedule.
