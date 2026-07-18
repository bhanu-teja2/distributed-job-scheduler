# Code Guide

This guide is the shortest path from the repository layout to the runtime behavior. The system-design documents explain architecture and tradeoffs; this guide explains where those decisions live in code.

## Process Entry Points

| Process | Entry point | Responsibility |
| --- | --- | --- |
| API | `cmd/api/main.go` | Builds authenticated HTTP routes and dependency readiness checks. |
| Worker | `cmd/worker/main.go` | Claims due jobs, renews ownership, and invokes executors. |
| Event relay | `cmd/event-relay/main.go` | Publishes committed outbox events to Kafka. |
| Admin CLI | `cmd/scheduler-admin/main.go` | Creates and revokes tenant-scoped API clients. |

## Job Creation Path

1. `internal/server` authenticates `X-API-Key`, rate-limits the client, and routes the request.
2. `internal/job.Handler` decodes HTTP input but makes no lifecycle decisions.
3. `internal/job.Service` applies defaults, validates tenant-scoped input, and hashes the canonical request for idempotency.
4. `internal/job.PostgresRepository.Create` inserts the job and `job.created` outbox event in one transaction.
5. A repeated tenant/idempotency-key pair returns the original job only when its request hash matches.

## Worker Execution Path

1. `ClaimDueJobs` selects due work with `FOR UPDATE SKIP LOCKED`, deterministic ordering, and one database transaction per claim batch.
2. The same transaction changes each job to `RUNNING`, creates its attempt, and writes `job.started`.
3. `worker.Service.ProcessJob` acquires the secondary Redis owner lease before invoking the executor.
4. A renewal loop extends both PostgreSQL and Redis leases. Losing either lease cancels the handler context.
5. `CompleteExecution` or `FailExecution` atomically finalizes the attempt, job, DLQ record when needed, and lifecycle events.

PostgreSQL is authoritative. Redis reduces the chance of duplicate external work, but workers still rely on ownership-checked PostgreSQL updates before committing an outcome.

## Event Delivery Path

Lifecycle events are rows in `job_events`, so state and event creation cannot diverge. `internal/outbox.Relay` leases unpublished rows with `SKIP LOCKED`, publishes them using the job ID as Kafka key, and records publication only if it still owns the relay lease. A crash after Kafka accepts an event but before PostgreSQL records publication can produce a duplicate; consumers must deduplicate by `event_id`.

## Security Boundaries

- `internal/auth` hashes API keys with SHA-256 and stores only hashes plus display prefixes.
- Every API repository operation receives the authenticated `tenant_id`.
- Role order is `viewer < operator < admin`.
- `internal/worker.WebhookHandler` validates URL scheme, redirects, DNS results, and the actual dial target to mitigate SSRF and DNS-rebinding attacks.
- Scheduler-owned HTTP headers cannot be overridden by job payloads.

## Failure Semantics

`internal/worker.ClassifyError` converts executor errors into stable error codes and retryability. `internal/scheduler.DecideFailure` is the single retry/dead-letter rule and calculates exponential backoff from the next attempt number. Execution is at least once; webhook receivers should use the scheduler job ID as an idempotency key.

## Reading Order

For an implementation review, read these files in order:

1. `internal/scheduler/engine.go`
2. `internal/job/service.go`
3. `internal/job/repository.go`
4. `internal/worker/service.go`
5. `internal/worker/executor.go`
6. `internal/outbox/outbox.go`
7. `internal/auth/auth.go`
