# FlowForge

[![CI](https://github.com/ShivangSingh28/flowforge/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/ShivangSingh28/flowforge/actions/workflows/ci.yml)
![Node.js 22](https://img.shields.io/badge/Node.js-22-339933?logo=node.js&logoColor=white)
![TypeScript](https://img.shields.io/badge/TypeScript-strict-3178C6?logo=typescript&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![RabbitMQ](https://img.shields.io/badge/RabbitMQ-4-FF6600?logo=rabbitmq&logoColor=white)

**FlowForge is a distributed background-job processing system** built with Node.js, strict TypeScript, Express, PostgreSQL, Prisma, and RabbitMQ.

It separates job submission, reliable message publication, and job execution into three independently runnable processes:

- **API** — validates submissions and atomically stores each job with an outbox event.
- **Outbox Relay** — claims unpublished events and publishes persistent RabbitMQ messages with publisher confirms.
- **Worker** — consumes messages, uses database-backed execution leases and fencing tokens, retries eligible failures, and acknowledges only after durable outcomes.

The system is designed around **at-least-once delivery**, explicit recovery behavior, and safe handling of duplicate messages—not an exactly-once execution claim.

**[Architecture & failure flows →](docs/architecture.md)** · **[CI workflow →](.github/workflows/ci.yml)**

## Engineering highlights

- Transactional Outbox: job + outbox event committed in one PostgreSQL transaction.
- Concurrent relay claims using `FOR UPDATE SKIP LOCKED`, leases, and per-event lock tokens.
- Persistent RabbitMQ publication with mandatory routing and publisher confirms.
- Worker manual ACK, configurable prefetch, execution leases, heartbeat renewal, and fencing tokens.
- Capped exponential backoff for publication and handler retries; durable dead-letter routing for malformed deliveries.
- Bounded graceful shutdown, broker reconnect with backoff/jitter, readiness checks, and operational metrics.
- Unit/HTTP tests plus isolated PostgreSQL/RabbitMQ integration tests for process and broker recovery.
- GitHub Actions CI covering formatting, lint, Prisma validation, TypeScript, tests, build, and real-service integration tests.

## Quick start

### Requirements

- Node.js 22
- npm
- Docker Engine / Docker Desktop with Docker Compose

### Run locally

```sh
npm install
```

Create your local environment file:

```powershell
Copy-Item .env.example .env
```

Start PostgreSQL and RabbitMQ:

```sh
docker compose up -d postgres rabbitmq
```

Apply database migrations and start the API:

```sh
npm run prisma:migrate:dev
npm run dev
```

Run the relay and worker in separate terminals:

```sh
npm run relay
npm run worker
```

The API listens on `http://localhost:3000` by default. `GET /health` is a liveness check; `GET /health/ready` checks PostgreSQL readiness.

## Documentation

- [Architecture, component responsibilities, delivery guarantees, recovery, and testing](docs/architecture.md)
- [CI workflow](.github/workflows/ci.yml)
- [Environment variables, retry/lease tuning, worker crash-recovery walkthrough, integration testing, scripts, and project structure](#configuration-and-operations)

---

## Configuration and operations

- Node.js 20 or newer
- npm

## Getting started

```sh
npm install
Copy-Item .env.example .env
docker compose up -d postgres rabbitmq
npm run prisma:migrate:dev
npm run dev
```

The API listens on `http://localhost:3000` by default. `GET /health` returns
`{ "status": "ok" }`.
`GET /health/ready` checks PostgreSQL connectivity and returns HTTP 503 when
the database cannot be reached.
RabbitMQ is configured through `RABBITMQ_URL`; Compose uses `RABBITMQ_USER` and
`RABBITMQ_PASSWORD` for broker credentials. The API requires PostgreSQL but
does not require RabbitMQ to be running or configured to start. The relay and
worker require the URL and initialize the durable
`flowforge.jobs` exchange and job queue. For non-default destinations, configure
`OUTBOX_ROUTING_KEY`, `WORKER_QUEUE`, and `WORKER_ROUTING_KEY` consistently.
`WORKER_LEASE_DURATION_MS` sets the execution lease duration (default 30000 ms)
and `WORKER_HEARTBEAT_INTERVAL_MS` sets the renewal and recovery polling
interval (default 10000 ms). The heartbeat interval must be less than half the
lease duration. `WORKER_CRASH_TEST_DURATION_MS` sets the development-only
`crash-test` handler runtime (default 60000 ms); this handler is unavailable in
production, waits without performing external side effects, and logs only safe
lifecycle events with the job ID.
`WORKER_QUEUE` selects the worker queue (default `flowforge.jobs.default`).
`WORKER_HEALTH_PORT` and `OUTBOX_HEALTH_PORT` default to `0` (an OS-assigned
port, logged at startup), allowing multiple local worker and relay processes.
Set explicit distinct ports when health probes need stable addresses.
RabbitMQ reconnect delays use `RABBITMQ_RECONNECT_BASE_DELAY_MS` and
`RABBITMQ_RECONNECT_MAX_DELAY_MS` with bounded exponential backoff and jitter.
`WORKER_SHUTDOWN_GRACE_MS` and `OUTBOX_SHUTDOWN_GRACE_MS` bound graceful
shutdown; relay grace must cover its publisher-confirm timeout.

Run the API, worker, and relay in separate terminals with `npm run dev`,
`npm run worker`, and `npm run relay`.
`OUTBOX_POLL_INTERVAL_MS`, `OUTBOX_BATCH_SIZE`, `OUTBOX_LEASE_DURATION_MS`,
`OUTBOX_PUBLISH_CONFIRM_TIMEOUT_MS`, `OUTBOX_RETRY_BASE_DELAY_MS`, and
`OUTBOX_RETRY_MAX_DELAY_MS` configure the independent relay. Retry delay grows
exponentially from the base to the maximum; events are never discarded after a
fixed attempt count.
Job handler retries use `JOB_RETRY_BASE_DELAY_MS` and
`JOB_RETRY_MAX_DELAY_MS`, separately from outbox publication retries.
`WORKER_PREFETCH` controls the number of unacknowledged deliveries per worker
and defaults to `1`. The worker currently handles the deterministic `demo` job
type. Execution leases are persisted with unique fencing tokens and renewed
while handlers run. A worker can reclaim a job after its lease expires; duplicate
deliveries wait for the current lease owner rather than being discarded.
Completion and terminal failure are persisted before the delivery is ACKed.
Handlers may run more than once after crashes or lease loss; fencing protects
database state but does not prevent duplicated external side effects.
Ordinary handler errors are retryable until `maxAttempts` is reached; retry
times use capped exponential backoff and the current RabbitMQ delivery remains
unacknowledged while its persisted `scheduledAt` is in the future. Handler
validation/business errors can throw `TerminalJobError` to fail immediately.
Missing handlers also fail terminally. Lease-expired claims count toward
`maxAttempts`; exhausted jobs become `FAILED`, with the last attempt retained.
External side effects in retryable handlers must be idempotent or protected by
their own idempotency key.

Malformed JSON and schema-invalid broker deliveries are copied to the durable
`flowforge.jobs.dead` queue through the durable `flowforge.jobs.dead` exchange.
The worker waits for the DLQ publisher confirm before ACKing the original
delivery. If routing or confirmation fails, it delays and requeues the original
delivery instead of losing it. Database/broker processing errors are delayed
and requeued; these delivery failures are separate from persisted job-handler
retries. No exactly-once handler guarantee is made.

Workers and relays detect channel/connection loss, stop their old consumer or
publisher generation, reconnect with bounded exponential backoff and jitter,
redeclare topology, and then resubscribe or resume due outbox claims. Worker
readiness is exposed at `/health/ready` and `/health/operations` on its
configured health port; the relay exposes the same paths. API health remains
independent of RabbitMQ and reports PostgreSQL readiness at `/health/ready`.
The API also exposes `/health/operations`. Operational responses include
pending outbox count, oldest unpublished event age, and failed-job count, but
never credentials or connection strings.

### Verifying worker crash recovery locally

Use a development/test environment with the Phase 5B migration already applied.
Do not use the crash-test handler in production.

1. Start the API with `npm run dev`, the relay with `npm run relay`, and a
   worker with `npm run worker` in separate terminals.
2. In PowerShell, submit a test job and save its ID:

   ```powershell
   $job = Invoke-RestMethod -Method Post -Uri http://localhost:3000/api/jobs -ContentType 'application/json' -Body '{"type":"crash-test","payload":{}}'
   $job.id
   ```

3. Wait for `worker.crash_test_started` with that ID in the worker output.
   Force-stop only that worker process (not the API or containers) before its
   60000 ms handler finishes. For a real crash test, do not use Ctrl+C, which
   initiates graceful shutdown. Identify the worker Node process by its command
   line and stop that specific PID:

   ```powershell
   Get-CimInstance Win32_Process -Filter "name = 'node.exe'" | Where-Object { $_.CommandLine -match 'tsx.*src/workers/worker.ts' } | Select-Object ProcessId,CommandLine
   $workerPid = [int](Read-Host 'Enter the worker ProcessId shown above')
   Stop-Process -Id $workerPid -Force
   ```

4. Restart the worker with `npm run worker`. After the lease expires, RabbitMQ
   should redeliver the unacknowledged message and the new worker should reclaim
   the job. The crash-test handler then waits for its configured duration and
   should log `worker.crash_test_completed`.
5. Verify the job is `COMPLETED`, its attempts count increased, and its attempt
   records show the expired first attempt and completed recovery attempt. Use a
   read-only query in your local database client; do not reset or delete data.

### Transactional Outbox

`POST /api/jobs` validates the request and inserts the `PENDING` job plus its
immutable `job.submitted` OutboxEvent in one PostgreSQL transaction. A successful
new submission returns `201` once that transaction commits; an identical
idempotent submission returns `200` and does not add another event. Reusing an
idempotency key with different data returns `409`. API acceptance does not
depend on RabbitMQ availability.

The independent relay claims due, unpublished events in short PostgreSQL
transactions using `FOR UPDATE SKIP LOCKED`, a per-event lock token, and a
database lease. It commits the claim before publishing, sends a persistent
message with the stable OutboxEvent ID as RabbitMQ `messageId`, uses mandatory
routing, and waits for publisher confirmation. It marks an event published only
after a positive confirmation and no unroutable return, with a conditional
update that verifies the lock token and lease.

An event transitions from due/unpublished to leased, then to published after
broker confirmation. A publication failure clears the lease, records a safe
error, and moves `availableAt` forward; an abandoned relay claim becomes
eligible again when `lockedUntil` expires.

If publication fails or is unroutable, the event remains in PostgreSQL with a
safe error and a future retry time calculated with capped exponential backoff.
There is no maximum attempt count. If the relay crashes while holding a claim,
the lease expires and another relay can claim the event. If RabbitMQ confirms
publication and the relay crashes before recording `publishedAt`, the event is
published again after recovery. This is intentionally **at-least-once
publication**, not exactly-once delivery or execution. Stable event identity
and worker lease/fencing behavior help make duplicate messages safe for the
database state, but handlers with external side effects still need their own
idempotency protections.
On SIGINT/SIGTERM the relay stops polling, waits for its in-flight publish
operations to confirm or time out, and then closes PostgreSQL and RabbitMQ.
The worker cancels its consumer first, allows in-flight database-backed work
through its grace period, then aborts remaining handlers. Unfinished deliveries
are not ACKed; channel closure returns them to RabbitMQ, and an active job lease
must expire before another worker can claim it. Handlers that ignore
AbortSignal may continue external side effects after shutdown; fencing still
prevents their stale database completion or failure.

The Outbox migration does not backfill existing jobs. Existing jobs may already
have a RabbitMQ message, may be pending without one, or may be in progress; the
database cannot reliably distinguish these cases. Any legacy pending-job
backfill is an explicit operational decision and may create duplicate
publications. If selected, backfill in bounded batches and account for duplicate
delivery; do not assume applying the migration alone republishes old jobs.

Published outbox events are retained; automatic archival/deletion is not
implemented. If retention is required, operators should delete only
`publishedAt` rows older than an approved retention window, in bounded
transactions, after checking job-history retention requirements. Never clean up
unpublished or actively leased events. Because the event-to-job foreign key is
restrictive, delete eligible published events before deleting their jobs.

### Running isolated integration tests

Integration tests use a separate Compose project, loopback-only ports, and
dedicated PostgreSQL and RabbitMQ credentials. They do not use `DATABASE_URL`
from `.env` and fail before migrations if the configured integration URL does
not point to the fixed `flowforge_integration` database on port `55432`.

1. Copy `.env.integration.example` to `.env.integration`. Keep the dedicated
   database name and ports unchanged; use only this environment for the
   integration-test services.
2. Run `npm run integration:up` to start the isolated PostgreSQL and RabbitMQ
   containers and wait for their health checks.
3. Run `npm run integration:test` to start/wait for the services if needed,
   apply existing migrations with `prisma migrate deploy` against the dedicated
   database, then run only `tests/integration/**/*.integration.test.ts`.
   Integration tests remove the database rows and RabbitMQ messages/queues
   they create, including on normal test cleanup.
4. Run `npm run integration:down` to stop and remove only the containers and
   network in the `flowforge-integration` Compose project. The dedicated
   `flowforge_integration_postgres_data` volume is intentionally retained;
   RabbitMQ test data is kept only in its container and is removed with it.
   This command does not use `down --volumes`; it does not stop or modify the
   development Compose project or its volumes.

The integration runner requires `.env.integration` and validates the database
name, loopback host, dedicated port, and URL credentials before invoking Docker,
Prisma, or Jest. Never point the integration variables at a development or
shared database/broker. To inspect services after a failed test, leave them
running and use `docker compose --env-file .env.integration --project-name
flowforge-integration -f docker-compose.integration.yml logs`; stop them with
`npm run integration:down` when finished.

### Continuous integration

GitHub Actions runs CI on pushes to `main`, pull requests targeting `main`, and
manual dispatch. The workflow uses Node.js 22 and checks formatting of
Prettier-supported files (including workflow YAML), lint, Prisma schema
validity, production and test TypeScript, unit tests, and the production build.
Prettier does not support SQL natively; the integration step applies the SQL
migrations to its isolated PostgreSQL service. CI invokes the existing
`npm run integration:test` runner, which starts its isolated PostgreSQL and
RabbitMQ Compose project and runs the real-service integration suite, including
worker/relay process recovery and RabbitMQ service-recreation tests. CI uses
dedicated throwaway integration credentials, always attempts to stop the
integration services without deleting the PostgreSQL volume, and uploads
integration/service logs when a job fails.

The database URL and PostgreSQL container credentials are configured in `.env`.
`POSTGRES_PORT` controls the host port (the example uses `5435` to avoid
colliding with a local PostgreSQL installation). Keep it consistent with the
port in `DATABASE_URL`. Use local development credentials only; do not commit
your `.env` file.

## Scripts

- `npm run dev` — run the server with automatic reload
- `npm run worker` — run the independent RabbitMQ worker
- `npm run relay` — run the independent Transactional Outbox publisher
- `npm run start:worker` — run the compiled worker from `dist/`
- `npm run build` — compile TypeScript into `dist/`
- `npm start` — run the compiled server
- `npm test` — run Jest tests
- `npm run integration:up` — start isolated integration-test services
- `npm run integration:test` — migrate and run real-service integration tests
- `npm run integration:down` — stop isolated integration-test services
- `npm run lint` — check TypeScript and JavaScript with ESLint
- `npm run format` — format project files with Prettier
- `npm run format:check` — check formatting without writing changes
- `npm run prisma:generate` — generate the Prisma Client
- `npm run prisma:validate` — validate the Prisma schema
- `npm run prisma:migrate:dev -- --name <migration>` — create and apply a migration

## Structure

- `src/config/` — validated runtime configuration
- `src/controllers/` and `src/routes/` — HTTP request handlers and route wiring
- `src/middleware/` — request logging, not-found, and error handling
- `src/services/`, `src/repositories/` — application and data-access layers, including the reusable Prisma client
- `src/producers/` — job message publisher
- `src/consumers/` and `src/workers/` — RabbitMQ consumer and independent worker entry point
- `src/queues/` — RabbitMQ connection manager and topology setup
- `src/utils/` and `src/types/` — shared helpers and types
- `prisma/` — Prisma schema and database migrations
- `docker-compose.yml` — local PostgreSQL service, health check, and persistent volume
- `tests/` — HTTP application tests
- `docker/` — reserved for future container-related files
