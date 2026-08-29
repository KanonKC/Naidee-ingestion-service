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

---

# Processing Service

`cmd/processing` (package tree under `internal/processing/`) — merged into this repo, still built and deployed as its own binary/container (`Dockerfile.processing`), with its own `go run ./cmd/processing`.

A Go cron job that reads unprocessed rows from `ig_raw_posts`, puts them through the Claude Message Batches API to classify and extract structured event data, geocodes the venue, and lands the result in `events` and `venues`.

**Write-side only.** There is no public HTTP API. The endpoints it exposes are `/healthz` for container health checks and two admin-only routes for triggering and inspecting a run.

## Responsibility

1. Find `ig_raw_posts` where `processed_at IS NULL`
2. Submit them as one Claude batch (1 post = 1 request) to classify and extract
3. Resolve the extracted venue, geocoding it the first time it is seen
4. Upsert `events`, `venues`
5. Stamp `ig_raw_posts.processed_at`
6. Record the outcome in `processing_runs`

**This service does not know Instagram exists.** Its entire view of the world is "there are rows in `ig_raw_posts` with `processed_at IS NULL`". If a future Eventpop or Zipevent ingester lands rows in the same shape, this service picks them up with no code change at all.

## Schema ownership

This service **does not migrate the database**. Every table it touches is owned by [Core-Service](../Core-Service), which runs the Prisma migrations. Run Core-Service's `prisma migrate deploy` before booting this service.

The database user configured here needs write access only — no DDL rights.

## Layout

```
cmd/processing/main.go                             entrypoint: config, db, cron, signals
internal/processing/config/                        typed Configurations + env loader with validation
internal/processing/constants/                     Environment enum
internal/processing/logging/logger.go               TLogger (slog) with Layer enum and run_id support
internal/processing/errors/errors.go                 TError hierarchy
internal/processing/libs/                            singletons: postgres pool, slog handler
internal/processing/providers/claude/                Message Batches client, prompt, response decoding
internal/processing/providers/geocode/                Nominatim client with the mandatory rate limit
internal/processing/repositories/igrawpost/           pending-post queries + processed stamp
internal/processing/repositories/event/               events upsert
internal/processing/repositories/venue/               venues lookup and insert
internal/processing/repositories/processingrun/        processing_runs audit log
internal/processing/services/processing/               the orchestrator: batch, poll, apply, venues
internal/processing/services/system/                    health check
internal/processing/controllers/middleware.go            admin token authentication
internal/processing/controllers/processing/               manual trigger + run status
internal/processing/routes/routes.go                       dependency wiring and route table
```

## Getting started

```bash
cp .env.example .env
```

Fill in `DATABASE_URL`, `ANTHROPIC_API_KEY`, and a real contact address in `GEOCODE_USER_AGENT`. Then:

```bash
go run ./cmd/processing
```

The service validates its whole configuration at boot and refuses to start on a problem, rather than dying at the first cron tick.

## How a run works

```
1. SELECT pending posts (LIMIT LLM_POST_LIMIT)
2. 0 rows      → close the run as completed, submit nothing
3. Submit one batch, record batch_id on processing_runs immediately
4. Poll every BATCH_POLL_INTERVAL until the batch ends, or BATCH_POLL_TIMEOUT
5. Fetch results and write them straight away, in the same run
6. Per post: upsert the event → resolve the venue → stamp processed_at
7. Close out processing_runs with the counters
```

Two ordering rules carry most of the correctness:

- **Results are written the moment they are fetched.** Batch results expire after 29 days, and a `batch_id` parked in a table with nobody fetching it is a slow data leak. There is deliberately no "fetch a previous run's results later" path.
- **`processed_at` is stamped last.** A failure anywhere above it leaves the post pending, so the next run retries it. A post can be processed twice; it can never be silently dropped.

### Costs and why it is a batch

The Batch API costs half of what the same requests cost synchronously, and this is a cron job with nobody waiting on the answer — the tradeoff is free. The system prompt is byte-identical across every request in a batch and carries a cache breakpoint, so it is billed once per batch rather than once per post.

## Venue matching (and its known limitation)

This phase does **exact matching on a normalised name only**. `normalizeVenueName` lowercases, collapses whitespace and trims trailing punctuation — enough to unify `BACC`, `bacc` and `BACC  `, and deliberately not enough to unify `BACC` with `หอศิลป์กรุงเทพ`.

Those two are the same building and **will become two rows in `venues`**. That is accepted for now: fuzzy matching is a later phase, and merging the duplicates then is a backfill that does not disturb existing data. `internal/processing/services/processing/venue_test.go` pins this behaviour so the limitation stays a decision rather than a surprise.

## Geocoding

Nominatim was chosen because it is free and its results may be cached indefinitely, which Google's terms forbid. The price is a strict usage policy, and both halves of it are enforced in the client rather than trusted to callers:

- **One request per second** (`GEOCODE_MIN_INTERVAL`). Config validation refuses to boot with anything lower while pointed at the public instance.
- **An identifiable `User-Agent`** (`GEOCODE_USER_AGENT`). Nominatim blocks clients without one; an empty value is a boot failure.

Breaking either gets the IP blocked, which takes the whole service down — so neither is a warning.

A venue that cannot be geocoded is still created, with `lat`/`lng`/`geocoded_at` left NULL. The event exists and is fully usable; it just has no pin on the map until someone fixes the address.

## Review status

`events.review_status` starts as:

| Extraction | `review_status` |
|---|---|
| `is_event = true` and `confidence = high` | `auto_published` |
| anything else | `pending` |

Reprocessing never overwrites `review_status`, so a re-run cannot undo an approve or reject an admin already made by hand.

Note one deliberate deviation from the spec's SQL: the spec keys `auto_published` on confidence alone, which would auto-publish a post the model was *confidently sure is not an event*. Since `review_status` is what the read side filters on, the condition here also requires `is_event`.

## Reprocessing an edited caption

Captions get edited after posting — a corrected date, an added signup link. This repo's ingestion side stores a `caption_hash` alongside each post and clears `processed_at` when it changes, which queues the post for reprocessing here. `UNIQUE (raw_post_id)` on `events` makes that an UPDATE of the existing row rather than a duplicate.

Whether an edited caption *should* also reset an admin's approve/reject decision is an open question, deliberately left for a later phase.

**On first deploy:** every existing `ig_raw_posts` row has `caption_hash = NULL`, so the next ingestion run writes a hash and clears `processed_at`. Since nothing has been processed yet in Phase 1, this is a no-op in practice — but it is worth knowing if you deploy this against a database that already has processed rows.

## Overlap protection

The cron fires every thirty minutes; a batch can be polled for up to two hours. Without a guard, four runs would be submitting the same posts at once.

Both the cron and the manual trigger go through the same `atomic.Bool` compare-and-swap, so they are serialised against each other with no second lock to keep in sync. An overlapping cron tick is skipped; an overlapping manual trigger gets a `409` naming the run that is in the way.

This is an **in-process** flag and protects a single replica, which is what this service is designed to be. Running more than one replica needs `pg_try_advisory_lock` instead — the flag cannot see another process.

## Admin API

Not a public API and not for the frontend. It is internal tooling for debugging, backfilling, and demos, and it is authenticated by a single shared token compared in constant time — there is no user/role system because there is one operator.

Set `ADMIN_API_TOKEN` (min 32 chars) to enable it. Leave it empty and **the routes are not registered at all**; an unauthenticated trigger would be worse than no trigger.

`HTTP_BIND_ADDRESS` defaults to `127.0.0.1`. In a container set it to `0.0.0.0` and restrict access at the network layer — never expose this port publicly.

| Route | Purpose |
|---|---|
| `POST /admin/runs` | start a run; `202` with a `run_id`, or `409` with the run already in progress |
| `GET /admin/runs/{id}` | run status and counters, read straight from `processing_runs` |
| `GET /healthz` | container health check (database reachability) |

`POST /admin/runs` answers immediately and never waits for the batch — a run can take hours. Poll `GET /admin/runs/{id}` for the outcome. No extra rate limiting is needed: concurrent calls all collide on the same guard and get `409`.

```bash
curl -X POST http://127.0.0.1:8083/admin/runs -H "X-Admin-Token: $ADMIN_API_TOKEN"
```

### Bruno collection

`bruno/Naidee Processing Service` covers the whole surface. Open it, pick the **Local** environment, and set `Admin_Api_Token` to the same value as the service's `ADMIN_API_TOKEN` — it is the only thing you have to fill in.

| Folder | Request | What it is for |
|---|---|---|
| Admin | Trigger Run | starts a run; captures `run_id` into the environment |
| Admin | Get Run | polls the run Trigger Run just captured |
| Admin | Trigger Run - Missing Token | sends no token — a `401` here is the pass |
| Admin | Get Run - Not Found | asks for a run that does not exist — expects `404` |
| System | Healthz | database reachability |

Trigger Run writes `run_id` into `Run_Id` after every response, so **Trigger Run → Get Run** works back to back with nothing to copy by hand. It captures the id from a `409` too, which is the case you most want to look at: that response names the run already in the way.

`Base_URL` defaults to `http://127.0.0.1:8083`, matching the service's default bind address.

## Error handling

| Situation | What happens |
|---|---|
| Batch submit fails (network, auth) | run marked `failed`, no `processed_at` set — the next run retries the whole set |
| Poll exceeds `BATCH_POLL_TIMEOUT` | run marked `failed`, `batch_id` kept so the batch can be recovered by hand within 29 days |
| One item comes back as invalid JSON | logged with the raw response, counted in `posts_failed`, post left pending — the rest of the batch is unaffected |
| A submitted post gets no result line | counted in `posts_failed` and left pending, so it cannot be silently lost |
| Geocoding finds nothing, or fails | venue created with NULL coordinates; the event lands normally |
| Venue resolution fails outright | logged; the event row survives and the post still counts as processed |

## Testing

```bash
go test -race ./...
```

The orchestrator, the batch-response decoding and the admin routes are all covered without a database or an API key. The batch client is an interface precisely so the tests never spend money.
