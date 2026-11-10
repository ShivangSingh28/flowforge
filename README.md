# FlowForge

FlowForge is a foundation for a distributed background job processing system.
This phase provides a strict TypeScript and Express HTTP service, validated
environment configuration, PostgreSQL persistence through Prisma, request
logging, centralized error handling, graceful shutdown, and RabbitMQ connection
infrastructure, default job topology, a confirmed job producer, and an
independently runnable worker for the `demo` job type. Redis, automatic retries,
dead-letter queues, and scheduled jobs are intentionally not included.

## Requirements

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
`RABBITMQ_PASSWORD` for broker credentials.
Startup creates the durable `flowforge.jobs` exchange and
`flowforge.jobs.default` queue before the HTTP server begins listening.
`JobProducer` publishes validated persistent job messages and resolves only
after publisher confirmation and any required channel backpressure drain.
`WORKER_LEASE_DURATION_MS` sets the execution lease duration (default 30000 ms)
and `WORKER_HEARTBEAT_INTERVAL_MS` sets the renewal and recovery polling
interval (default 10000 ms). The heartbeat interval must be less than half the
lease duration. `WORKER_CRASH_TEST_DURATION_MS` sets the development-only
`crash-test` handler runtime (default 60000 ms); this handler is unavailable in
production, waits without performing external side effects, and logs only safe
lifecycle events with the job ID.

Run the independent worker in a separate terminal with `npm run worker`.
`WORKER_PREFETCH` controls the number of unacknowledged deliveries per worker
and defaults to `1`. The worker currently handles the deterministic `demo` job
type. Execution leases are persisted with unique fencing tokens and renewed
while handlers run. A worker can reclaim a job after its lease expires; duplicate
deliveries wait for the current lease owner rather than being discarded.
Completion and terminal failure are persisted before the delivery is ACKed.
Handlers may run more than once after crashes or lease loss; fencing protects
database state but does not prevent duplicated external side effects. Automatic
retry policies and a dead-letter queue are not implemented.

### Verifying worker crash recovery locally

Use a development/test environment with the Phase 5B migration already applied.
Do not use the crash-test handler in production.

1. Start the API with `npm run dev`, then start a worker with `npm run worker`
   in separate terminals.
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

`POST /api/jobs` validates and persists a job before publishing it. A successful
new submission returns `201`; an identical idempotent submission returns `200`;
reusing an idempotency key with different submission data returns `409`. If
publishing fails after persistence, the job remains `PENDING` and the API
returns `503`. This is an interim database/broker consistency design. A
Transactional Outbox will address that consistency gap in a later phase.
Until then, retrying a submission with the same idempotency key returns its
existing job without republishing it; a job left pending by a broker failure
requires operational recovery.

The database URL and PostgreSQL container credentials are configured in `.env`.
`POSTGRES_PORT` controls the host port (the example uses `5434` to avoid
colliding with a local PostgreSQL installation). Keep it consistent with the
port in `DATABASE_URL`. Use local development credentials only; do not commit
your `.env` file.

## Scripts

- `npm run dev` — run the server with automatic reload
- `npm run worker` — run the independent RabbitMQ worker
- `npm run start:worker` — run the compiled worker from `dist/`
- `npm run build` — compile TypeScript into `dist/`
- `npm start` — run the compiled server
- `npm test` — run Jest tests
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
