# Ingestion Service

One Go service, one process, one container. It pulls posts from whitelisted Instagram Business/Creator accounts, and turns the raw posts it collects into structured event data — ingest and process are two jobs inside the same service, not two separate services. Neither is more important than the other; think of them as two widgets living in the same app, the way TRAILBLAZER-backend groups unrelated features under one roof.

**Write-side only.** There is no public read API. It exposes `/health` for container health checks and three admin-only routes for manually triggering or inspecting a run.

## Responsibility

**Ingestion** — capturing raw data before anything is interpreted:

1. Read the IG account whitelist from `ig_sources`
2. Call Business Discovery once per account
3. Upsert the results into `ig_raw_posts`, idempotently
4. Record the outcome in `ingestion_runs`

**Processing** — turning raw posts into ready-to-use records:

1. Find `ig_raw_posts` where `processed_at IS NULL`
2. Submit them as one Claude batch (1 post = 1 request) to classify and extract
3. Resolve the extracted venue, geocoding it the first time it is seen
4. Upsert `events`, `venues`
5. Stamp `ig_raw_posts.processed_at`
6. Record the outcome in `processing_runs`

**Processing does not know Instagram exists.** Its entire view of the world is "there are rows in `ig_raw_posts` with `processed_at IS NULL`". If a future Eventpop or Zipevent ingester lands rows in the same shape, processing picks them up with no code change at all — that's why the two stay decoupled through the `ig_raw_posts` table even while living in the same process.

## Schema ownership

This service **does not migrate the database**. Every table it touches is owned by [Core-Service](../Core-Service), which runs the Prisma migrations. Start Core-Service's `prisma migrate deploy` before booting this service.

The database user configured here needs write access only — no DDL rights.

## Layout

One binary, `cmd/ingestion-service`. The layering mirrors `blaze-backend`: **Controller → Service → Repository**, with providers for external APIs and manual dependency wiring in one readable file. Concerns that are genuinely shared (config, cron scheduler, admin auth, health check, logging, errors) are single files/packages; concerns specific to one job sit as sibling subpackages named for it (`controllers/ingestion` next to `controllers/processing`, `services/ingestion` next to `services/processing`, and so on) — never nested under a wrapping `ingestion/` or `processing/` directory.

```
cmd/ingestion-service/main.go          entrypoint: config, db, startup checks, both cron schedulers, one server, signals
internal/config/                       ONE Configurations struct + ONE Load()/validate() — everything required to boot
internal/constants/                    Environment enum
internal/logging/logger.go             TLogger (slog) with Layer enum and run_id support
internal/errors/errors.go              TError hierarchy
internal/libs/                         singletons: postgres pool, slog handler
internal/providers/instagram/          Business Discovery client + error classification
internal/providers/googlechat/         fatal-error alerting webhook
internal/providers/claude/             Message Batches client, prompt, response decoding
internal/providers/geocode/            Nominatim client with the mandatory rate limit
internal/repositories/igsource/        ig_sources queries
internal/repositories/igrawpost/       ig_raw_posts — Upsert (ingestion) + ListUnprocessed/MarkProcessed (processing), same table
internal/repositories/ingestionrun/    ingestion_runs audit log
internal/repositories/event/           events upsert
internal/repositories/venue/           venues lookup and insert
internal/repositories/processingrun/   processing_runs audit log
internal/services/ingestion/           the ingestion orchestrator: worker pool, retries, counters
internal/services/processing/          the processing orchestrator: batch, poll, apply, venues
internal/services/system/              health check
internal/controllers/middleware.go     AuthenticateAdmin — one x-api-key check for every admin route
internal/controllers/system/           GET /health
internal/controllers/ingestion/        POST /api/v1/admin/ingestion/trigger
internal/controllers/processing/       POST /api/v1/admin/processing/trigger, GET /api/v1/admin/processing/runs/{id}
internal/cron/cron.go                  IngestionCron + ProcessingCron — two independent schedules, one process (Asia/Bangkok)
internal/routes/routes.go              Build() — dependency wiring for the whole service, mirrors routes.ts
```

### Where this differs from the spec's suggested layout

The spec sketched `internal/instagram/`, `internal/store/`, `internal/ingest/`. Those became `internal/providers/instagram/`, `internal/repositories/*/`, and `internal/services/ingestion/` so the naming matches `blaze-backend`. The components and their responsibilities are unchanged.

Other deliberate deviations from the spec, each noted where it occurs in the code:

- **`migrations/` is gone.** Schema ownership moved to Core-Service.
- **The access token travels in an `Authorization: Bearer` header**, not the `access_token` query parameter the spec shows. The Graph API accepts both; the header keeps the token out of access logs and proxy caches.

## Configuration

Everything comes from one environment and is validated at startup — a missing variable is a boot failure with a readable message, never a silent 3am cron failure. See [.env.example](.env.example).

| Env | Example | Notes |
|---|---|---|
| `DATABASE_URL` | `postgres://...` | write-capable user |
| `PORT` | `8082` | the one HTTP server |
| `HTTP_BIND_ADDRESS` | `0.0.0.0` | listen address |
| `ENV` | `local` | `local` logs as text, `dev`/`prod` as JSON |
| `IG_ACCESS_TOKEN` | `EAAG...` | long-lived token (60 days) |
| `IG_USER_ID` | `1784...` | our own IG Business account |
| `IG_API_VERSION` | `v25.0` | pinned, never the default |
| `IG_MEDIA_LIMIT` | `25` | recent posts per account |
| `CRON_SCHEDULE` | `0 */6 * * *` | ingestion cron, every 6 hours |
| `WORKER_CONCURRENCY` | `3` | sources fetched in parallel |
| `RUN_ON_STARTUP` | `false` | trigger ingestion immediately on boot |
| `GOOGLE_CHAT_WEBHOOK_URL` | | optional; empty disables alerting |
| `ANTHROPIC_API_KEY` | `sk-ant-...` | Claude Message Batches API |
| `LLM_MODEL` | `claude-haiku-4-5` | |
| `LLM_MAX_TOKENS` | `500` | caps each extraction |
| `LLM_POST_LIMIT` | `5000` | posts submitted per processing run |
| `BATCH_POLL_INTERVAL` | `30s` | |
| `BATCH_POLL_TIMEOUT` | `2h` | |
| `GEOCODE_BASE_URL` | `https://nominatim.openstreetmap.org` | |
| `GEOCODE_USER_AGENT` | | required; Nominatim blocks unidentified clients |
| `GEOCODE_MIN_INTERVAL` | `1s` | |
| `PROCESSING_CRON_SCHEDULE` | `*/30 * * * *` | processing cron, every 30 minutes — independent of `CRON_SCHEDULE` |
| `PROCESSING_RUN_ON_STARTUP` | `false` | trigger processing immediately on boot |
| `ADMIN_API_KEY` | | optional; empty disables all three admin routes. Min 32 chars |

## HTTP endpoints

All under one server, one port, one API key.

### `GET /health`

200 when the database is reachable, 503 otherwise. For container health checks only.

### `POST /api/v1/admin/ingestion/trigger`

Runs an ingestion pass on demand, for when waiting up to six hours for the next cron tick is not an option.

Guarded by the `x-api-key` header, compared against `ADMIN_API_KEY` in constant time — the same pattern `blaze-backend` uses for its admin routes, and the same key that guards the processing routes below. **If `ADMIN_API_KEY` is unset none of the admin routes are registered at all**, and a warning says so at startup.

Asynchronous by default: an ingestion pass takes far longer than a request should, so the trigger acknowledges and runs in the background.

```bash
curl -X POST http://localhost:8082/api/v1/admin/ingestion/trigger -H "x-api-key: $ADMIN_API_KEY"
```

```json
{ "status": "accepted", "message": "Ingestion run started. Watch the ingestion_runs table or the logs for the outcome." }
```

Add `?wait=true` to block until the run finishes and get the counters back. This is what makes the idempotency check easy to verify — run it twice, and `posts_new` must be `0` the second time:

```bash
curl -X POST "http://localhost:8082/api/v1/admin/ingestion/trigger?wait=true" -H "x-api-key: $ADMIN_API_KEY"
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

### `POST /api/v1/admin/processing/trigger`

Starts a processing run on demand. `202` with a `run_id`, or `409` naming the run already in progress — the cron and the trigger go through the same `atomic.Bool` compare-and-swap, so at most one processing run is ever in flight.

```bash
curl -X POST http://localhost:8082/api/v1/admin/processing/trigger -H "x-api-key: $ADMIN_API_KEY"
```

Add `?limit=N` to cap how many pending posts this one run submits, overriding `LLM_POST_LIMIT` for just this run — useful for a small test run without waiting on the whole backlog:

```bash
curl -X POST "http://localhost:8082/api/v1/admin/processing/trigger?limit=10" -H "x-api-key: $ADMIN_API_KEY"
```

`limit` must be between 1 and the Batch API's cap (100,000) or the request is a `400`; omit it (or pass `0`) to use `LLM_POST_LIMIT`.

Never waits for the batch — a run can take hours. Poll the route below for the outcome.

### `GET /api/v1/admin/processing/runs/{id}`

Run status and counters, read straight from `processing_runs`.

```bash
curl http://localhost:8082/api/v1/admin/processing/runs/42 -H "x-api-key: $ADMIN_API_KEY"
```

The processing cron fires every thirty minutes; a batch can be polled for up to two hours. Without the compare-and-swap guard above, overlapping ticks would submit the same posts twice — an overlapping cron tick is silently skipped, an overlapping manual trigger gets a `409`. This is an **in-process** guard and protects a single replica; running more than one needs `pg_try_advisory_lock` instead.

## Error handling

### Ingestion

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

### Processing

| Situation | What happens |
|---|---|
| Batch submit fails (network, auth) | run marked `failed`, no `processed_at` set — the next run retries the whole set |
| Poll exceeds `BATCH_POLL_TIMEOUT` | run marked `failed`, `batch_id` kept so the batch can be recovered by hand within 29 days |
| One item comes back as invalid JSON | logged with the raw response, counted in `posts_failed`, post left pending — the rest of the batch is unaffected |
| A submitted post gets no result line | counted in `posts_failed` and left pending, so it cannot be silently lost |
| Geocoding finds nothing, or fails | venue created with NULL coordinates; the event lands normally |
| Venue resolution fails outright | logged; the event row survives and the post still counts as processed |

## Observability

Every log line inside an ingestion run carries `run_id`:

```
INFO  ingestion run started    run_id=42 sources=87
WARN  source failed            run_id=42 username=xxx kind=transient attempt=2
ERROR source deactivated       run_id=42 username=yyy reason="not a business account"
INFO  ingestion run finished   run_id=42 status=partial ok=85 failed=2 new=31 updated=104
```

## Token management

The long-lived IG token lasts 60 days and can be renewed **before** it expires. Let it lapse and the whole OAuth flow has to be redone.

Auto-refresh is a later phase. For now:

- **Startup check** — `GET /me?fields=id` runs at boot. A broken token exits immediately with a clear message.
- **Fatal alert** — error code `190` sends a Google Chat notification saying a new OAuth flow is required.

## Known trap: `media_url` expires

The `media_url` Instagram returns is a **signed CDN URL that expires within days**. Storing it is fine; serving it directly to a browser is not — the images will break later. Before the frontend ships, either mirror the images to our own object storage or re-fetch the URL at render time.

## Rate limits

Business Discovery falls under Platform Rate Limiting (not the tighter Business Use Case limit). One source costs one API call: 100 sources every 6 hours is ~400 calls/day, far below the ceiling.

Past roughly 150 sources — or a schedule tighter than 6 hours — switch to a tiered schedule instead: frequent posters every 6 hours, everything else once a day, driven by a new `sync_interval_hours` column on `ig_sources`.

## Batches and why processing uses one

The Batch API costs half of what the same requests cost synchronously, and this is a cron job with nobody waiting on the answer — the tradeoff is free. The system prompt is byte-identical across every request in a batch and carries a cache breakpoint, so it is billed once per batch rather than once per post.

A processing run:

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

## Venue matching (and its known limitation)

This phase does **exact matching on a normalised name only**. `normalizeVenueName` lowercases, collapses whitespace and trims trailing punctuation — enough to unify `BACC`, `bacc` and `BACC  `, and deliberately not enough to unify `BACC` with `หอศิลป์กรุงเทพ`.

Those two are the same building and **will become two rows in `venues`**. That is accepted for now: fuzzy matching is a later phase, and merging the duplicates then is a backfill that does not disturb existing data. `internal/services/processing/venue_test.go` pins this behaviour so the limitation stays a decision rather than a surprise.

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

Captions get edited after posting — a corrected date, an added signup link. Ingestion stores a `caption_hash` alongside each post and clears `processed_at` when it changes, which queues the post for reprocessing. `UNIQUE (raw_post_id)` on `events` makes that an UPDATE of the existing row rather than a duplicate.

Whether an edited caption *should* also reset an admin's approve/reject decision is an open question, deliberately left for a later phase.

**On first deploy:** every existing `ig_raw_posts` row has `caption_hash = NULL`, so the next ingestion run writes a hash and clears `processed_at`. Since nothing has been processed yet in Phase 1, this is a no-op in practice — but it is worth knowing if you deploy this against a database that already has processed rows.

## Bruno collection

`bruno/Naidee Ingestion Service` covers the whole surface. Open it, pick the **Local** environment, and set `Admin_Api_Key` — it is the only thing you have to fill in.

| Folder | Request | What it is for |
|---|---|---|
| System | Health | database reachability |
| Admin | Trigger Ingestion | triggers an ingestion run (`?wait=true`) |
| Admin | Trigger Processing | starts a processing run; captures `run_id` into the environment. Has a disabled `limit` query param to try |
| Admin | Get Processing Status | polls the run Trigger Processing just captured |

Trigger Processing writes `run_id` into `Run_Id` after every response, so **Trigger Processing → Get Processing Status** works back to back with nothing to copy by hand. It captures the id from a `409` too, which is the case you most want to look at: that response names the run already in the way.

Requests point at `http://localhost:8082`, hardcoded per-request rather than a `Base_URL` variable, matching this collection's existing convention.

## Development

```bash
cp .env.example .env
go build ./...
go test ./...
go test -race ./...
go run ./cmd/ingestion-service
```

The service validates its whole configuration at boot — both the ingestion and processing variables — and refuses to start on a problem, rather than dying at the first cron tick. Set `RUN_ON_STARTUP=true` / `PROCESSING_RUN_ON_STARTUP=true` to trigger a run immediately instead of waiting for its schedule.

## Testing

```bash
go test -race ./...
```

The orchestrators, the batch-response decoding, and the admin routes are all covered without a database or an API key. The batch client is an interface precisely so the tests never spend money.
