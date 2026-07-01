# Distributed Job Scheduler - Detailed Project Plan

## Project Name

**Distributed Job Scheduler**

A production-style backend system for scheduling, executing, retrying, and tracking background jobs using **Go, PostgreSQL, Redis, Kafka, and Docker**.

This project is designed to demonstrate backend engineering, distributed systems, concurrency, database transactions, Redis-based coordination, Kafka-based eventing, and production-style service design.

---

## Resume-Friendly Description

```text
Distributed Job Scheduler | Go, PostgreSQL, Redis, Kafka, Docker

• Built a backend system for scheduling, executing, and tracking delayed jobs using distributed workers, retry policies, job priorities, dead-letter queues, and graceful shutdown handling.

• Implemented worker coordination using Redis-based locking, transactional job claiming in PostgreSQL, Kafka-based job events, and Go concurrency patterns for reliable horizontal job processing.
```

---

# 1. Project Goal

The goal is to build a backend job scheduler similar to a simplified version of:

- Kubernetes CronJobs
- Sidekiq
- Celery
- Temporal
- Quartz Scheduler
- BullMQ
- AWS EventBridge Scheduler

The system should allow users or other services to submit jobs that need to run:

- immediately
- after a delay
- at a scheduled time
- repeatedly using cron expressions, optional in later versions
- with retry policies
- with priority
- with timeout
- with failure tracking
- with dead-letter handling

The scheduler should be able to run multiple worker instances safely without processing the same job twice.

---

# 2. What This Project Demonstrates

## Backend Engineering

- REST API design
- Clean architecture
- Service layer and repository layer separation
- Input validation
- Error handling
- Pagination
- Filtering
- Structured logging
- Configuration management
- Health checks
- Graceful shutdown

## Go Development

- Goroutines
- Channels
- Worker pools
- Context cancellation
- Interfaces
- Dependency injection
- Graceful shutdown with OS signals
- Unit tests
- Integration tests
- Race condition avoidance

## PostgreSQL

- Job persistence
- Transactions
- Row-level locking
- `SELECT ... FOR UPDATE SKIP LOCKED`
- Index design
- Status-based querying
- Job attempt tracking
- Dead-letter storage
- Audit/history records

## Redis

- Distributed locks
- Worker heartbeats
- Worker registry
- Optional leader election
- Optional rate limiting

## Kafka

- Job lifecycle events
- Event-driven notifications
- Audit/event streaming
- Consumer groups
- Retry-related events
- Dead-letter events

## Docker

- Local development environment
- Multi-container orchestration
- PostgreSQL, Redis, Kafka setup
- One-command project startup

---

# 3. High-Level Architecture

```text
                    +----------------------+
                    |      API Client      |
                    |  curl / Postman / UI |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |   Scheduler API      |
                    |      Go REST API     |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |     PostgreSQL       |
                    | Jobs / Attempts / DLQ|
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |   Worker Service     |
                    |  Polls and Executes  |
                    +----+------------+----+
                         |            |
                         v            v
                    +---------+   +---------+
                    |  Redis  |   |  Kafka  |
                    | Locks   |   | Events  |
                    +---------+   +---------+
```

---

# 4. Main Components

## 4.1 Scheduler API Service

The Scheduler API exposes REST APIs to create, inspect, cancel, pause, resume, and retry jobs.

Responsibilities:

- Create jobs
- Validate job payload
- Store jobs in PostgreSQL
- Expose job status APIs
- Expose job attempt history APIs
- Cancel jobs
- Retry failed jobs manually
- Pause and resume jobs
- Publish Kafka events for important actions

---

## 4.2 Worker Service

The Worker Service executes jobs.

Responsibilities:

- Poll PostgreSQL for due jobs
- Claim jobs safely using PostgreSQL transactions
- Acquire Redis lock before execution
- Execute jobs using registered handlers
- Handle success
- Handle failure
- Schedule retries
- Move permanently failed jobs to dead-letter queue
- Publish Kafka job lifecycle events
- Maintain worker heartbeat

---

## 4.3 Scheduler Engine

The Scheduler Engine contains the main business rules.

Responsibilities:

- Decide when a job is due
- Validate status transitions
- Calculate next retry time
- Apply exponential backoff
- Decide when to dead-letter a job
- Handle job timeout
- Support priority-based execution

---

## 4.4 Job Executor

The Job Executor runs the actual job handler.

For this project, job execution can be simulated.

Example job types:

```text
SEND_EMAIL
GENERATE_REPORT
PROCESS_PAYMENT_RETRY
CALL_WEBHOOK
SYNC_CUSTOMER_DATA
CLEANUP_EXPIRED_SESSIONS
```

Suggested Go interface:

```go
type JobHandler interface {
    Execute(ctx context.Context, job Job) error
}
```

---

## 4.5 Kafka Event Publisher

The Kafka publisher emits lifecycle events whenever a job changes state.

Example events:

```text
job.created
job.claimed
job.started
job.completed
job.failed
job.retry_scheduled
job.dead_lettered
job.cancelled
worker.started
worker.heartbeat
worker.stopped
```

---

## 4.6 Redis Coordination Layer

Redis is used for distributed worker safety.

Uses:

- Job execution locks
- Worker heartbeat
- Worker registration
- Optional leader election
- Optional rate limiting

Example Redis keys:

```text
lock:job:{job_id}
worker:{worker_id}:heartbeat
workers:active
rate_limit:job_type:{job_type}
leader:scheduler
```

---

# 5. Repository Structure

Use a clean monorepo structure.

```text
distributed-job-scheduler/
│
├── README.md
├── docker-compose.yml
├── Makefile
├── .env.example
├── .gitignore
│
├── cmd/
│   ├── api/
│   │   └── main.go
│   └── worker/
│       └── main.go
│
├── internal/
│   ├── config/
│   ├── logger/
│   ├── server/
│   ├── middleware/
│   ├── response/
│   ├── errors/
│   │
│   ├── job/
│   │   ├── handler.go
│   │   ├── service.go
│   │   ├── repository.go
│   │   ├── model.go
│   │   ├── dto.go
│   │   └── validator.go
│   │
│   ├── worker/
│   │   ├── service.go
│   │   ├── pool.go
│   │   ├── heartbeat.go
│   │   └── executor.go
│   │
│   ├── scheduler/
│   │   ├── engine.go
│   │   ├── retry_policy.go
│   │   ├── priority.go
│   │   └── cron.go
│   │
│   ├── lock/
│   │   └── redis_lock.go
│   │
│   ├── kafka/
│   │   ├── producer.go
│   │   ├── consumer.go
│   │   └── events.go
│   │
│   ├── postgres/
│   │   └── db.go
│   │
│   └── redis/
│       └── client.go
│
├── migrations/
│   ├── 000001_create_jobs_table.up.sql
│   ├── 000001_create_jobs_table.down.sql
│   ├── 000002_create_job_attempts_table.up.sql
│   ├── 000002_create_job_attempts_table.down.sql
│   ├── 000003_create_dead_letter_jobs_table.up.sql
│   └── 000003_create_dead_letter_jobs_table.down.sql
│
├── pkg/
│   └── types/
│       └── job.go
│
├── docs/
│   ├── architecture.md
│   ├── api-contracts.md
│   ├── database-design.md
│   ├── kafka-events.md
│   ├── worker-design.md
│   ├── redis-locking.md
│   ├── local-development.md
│   └── future-improvements.md
│
├── scripts/
│   ├── migrate.sh
│   ├── seed_jobs.sh
│   └── run_load_test.sh
│
├── tests/
│   ├── integration/
│   └── load/
│
└── .github/
    └── workflows/
        └── ci.yml
```

---

# 6. Database Design

PostgreSQL is the source of truth.

---

## 6.1 Jobs Table

```sql
CREATE TABLE jobs (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    job_type TEXT NOT NULL,
    payload JSONB NOT NULL,

    status TEXT NOT NULL,
    priority INT NOT NULL DEFAULT 5,

    run_at TIMESTAMP WITH TIME ZONE NOT NULL,
    locked_until TIMESTAMP WITH TIME ZONE,
    locked_by TEXT,

    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    retry_backoff_seconds INT NOT NULL DEFAULT 30,

    timeout_seconds INT NOT NULL DEFAULT 300,

    last_error TEXT,
    created_by TEXT,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    completed_at TIMESTAMP WITH TIME ZONE,
    failed_at TIMESTAMP WITH TIME ZONE,
    cancelled_at TIMESTAMP WITH TIME ZONE
);
```

---

## 6.2 Job Attempts Table

```sql
CREATE TABLE job_attempts (
    id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id),
    worker_id TEXT NOT NULL,

    attempt_number INT NOT NULL,
    status TEXT NOT NULL,

    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE,
    failed_at TIMESTAMP WITH TIME ZONE,

    error_message TEXT,
    execution_duration_ms BIGINT,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);
```

---

## 6.3 Dead Letter Jobs Table

```sql
CREATE TABLE dead_letter_jobs (
    id UUID PRIMARY KEY,
    original_job_id UUID NOT NULL,
    name TEXT NOT NULL,
    job_type TEXT NOT NULL,
    payload JSONB NOT NULL,

    final_error TEXT,
    retry_count INT NOT NULL,
    failed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);
```

---

## 6.4 Job Events Table

Optional but useful for local audit history.

```sql
CREATE TABLE job_events (
    id UUID PRIMARY KEY,
    job_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    source TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);
```

---

## 6.5 Recommended Indexes

```sql
CREATE INDEX idx_jobs_status_run_at ON jobs(status, run_at);
CREATE INDEX idx_jobs_priority_run_at ON jobs(priority DESC, run_at ASC);
CREATE INDEX idx_jobs_locked_until ON jobs(locked_until);
CREATE INDEX idx_jobs_job_type ON jobs(job_type);
CREATE INDEX idx_job_attempts_job_id ON job_attempts(job_id);
CREATE INDEX idx_dead_letter_jobs_original_job_id ON dead_letter_jobs(original_job_id);
```

---

# 7. Job Status Model

Use the following job statuses:

```text
PENDING
SCHEDULED
RUNNING
SUCCEEDED
FAILED
RETRY_SCHEDULED
DEAD_LETTERED
CANCELLED
PAUSED
```

---

## 7.1 Valid Status Transitions

```text
PENDING -> RUNNING
SCHEDULED -> RUNNING
RUNNING -> SUCCEEDED
RUNNING -> FAILED
FAILED -> RETRY_SCHEDULED
RETRY_SCHEDULED -> RUNNING
FAILED -> DEAD_LETTERED
RETRY_SCHEDULED -> DEAD_LETTERED
PENDING -> CANCELLED
SCHEDULED -> CANCELLED
RETRY_SCHEDULED -> CANCELLED
PENDING -> PAUSED
SCHEDULED -> PAUSED
PAUSED -> SCHEDULED
```

---

# 8. Priority Model

Use integer-based priority.

```text
1  = lowest priority
5  = normal priority
10 = highest priority
```

When polling jobs, workers should fetch jobs ordered by:

```sql
ORDER BY priority DESC, run_at ASC, created_at ASC
```

This ensures that higher-priority jobs run first, but older jobs are not ignored.

---

# 9. Retry Policy

Implement retry policy using exponential backoff.

Example:

```text
Initial backoff: 30 seconds
Retry 1: 30 seconds
Retry 2: 60 seconds
Retry 3: 120 seconds
Retry 4: 240 seconds
```

Formula:

```text
next_retry_delay = retry_backoff_seconds * 2 ^ retry_count
```

Add maximum retry support.

If `retry_count >= max_retries`, move the job to the dead-letter queue.

---

# 10. Distributed Job Claiming

This is one of the most important parts of the project.

Workers should claim jobs using PostgreSQL transactions and row-level locking.

Example query:

```sql
WITH due_jobs AS (
    SELECT id
    FROM jobs
    WHERE status IN ('PENDING', 'SCHEDULED', 'RETRY_SCHEDULED')
      AND run_at <= now()
      AND (locked_until IS NULL OR locked_until < now())
    ORDER BY priority DESC, run_at ASC, created_at ASC
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
UPDATE jobs
SET status = 'RUNNING',
    locked_by = $2,
    locked_until = now() + interval '5 minutes',
    updated_at = now()
WHERE id IN (SELECT id FROM due_jobs)
RETURNING *;
```

Why this matters:

- Multiple workers can poll at the same time.
- `SKIP LOCKED` prevents duplicate job claiming.
- Jobs locked by one worker are skipped by other workers.
- Expired locks allow recovery from crashed workers.

---

# 11. Redis Locking

Use Redis as an additional safety mechanism.

When a worker claims a job from PostgreSQL, it should also acquire a Redis lock.

Redis lock key:

```text
lock:job:{job_id}
```

Value:

```text
worker_id
```

TTL:

```text
job timeout + safety buffer
```

Example:

```text
SET lock:job:{job_id} worker-123 NX EX 360
```

If lock acquisition fails, the worker should skip execution and release the PostgreSQL lock or mark the job back as pending.

---

# 12. Worker Pool Design

## 12.1 Worker Service Flow

```text
Start worker service
    |
    v
Load config
    |
    v
Connect PostgreSQL, Redis, Kafka
    |
    v
Start heartbeat goroutine
    |
    v
Start job polling loop
    |
    v
Claim due jobs
    |
    v
Send jobs into worker pool channel
    |
    v
Execute jobs concurrently
    |
    v
Update job status
    |
    v
Publish Kafka event
```

---

## 12.2 Worker Pool Pseudocode

```go
func StartWorkerPool(ctx context.Context, workerCount int, jobs <-chan Job) {
    var wg sync.WaitGroup

    for i := 0; i < workerCount; i++ {
        wg.Add(1)

        go func(workerNumber int) {
            defer wg.Done()

            for {
                select {
                case <-ctx.Done():
                    return

                case job := <-jobs:
                    executeJob(ctx, job)
                }
            }
        }(i)
    }

    wg.Wait()
}
```

---

## 12.3 Polling Loop Pseudocode

```go
func PollLoop(ctx context.Context) {
    ticker := time.NewTicker(config.PollInterval)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return

        case <-ticker.C:
            jobs, err := repository.ClaimDueJobs(ctx, workerID, batchSize)
            if err != nil {
                logger.Error("failed to claim jobs", err)
                continue
            }

            for _, job := range jobs {
                jobQueue <- job
            }
        }
    }
}
```

---

# 13. Job Execution Types

Implement fake job handlers so the project can run locally without external dependencies.

## 13.1 Send Email Job

Payload:

```json
{
  "to": "user@example.com",
  "subject": "Welcome",
  "body": "Hello from the scheduler"
}
```

Behavior:

- Sleep for 1 second.
- Randomly succeed or fail based on config.
- Log email sent.

---

## 13.2 Webhook Job

Payload:

```json
{
  "url": "https://example.com/webhook",
  "method": "POST",
  "body": {
    "event": "payment.succeeded"
  }
}
```

Behavior:

- Simulate HTTP call.
- Add timeout.
- Fail if invalid URL or configured failure simulation.

---

## 13.3 Report Generation Job

Payload:

```json
{
  "report_type": "daily_settlement",
  "merchant_id": "merchant_123"
}
```

Behavior:

- Simulate long-running job.
- Use context timeout.
- Generate fake report ID.

---

## 13.4 Payment Retry Job

Payload:

```json
{
  "payment_id": "pay_123",
  "merchant_id": "merchant_123",
  "amount": 10000,
  "currency": "EUR"
}
```

Behavior:

- Simulate retry of failed payment.
- Publish job completed or failed event.

---

# 14. REST API Design

Base path:

```text
/api/v1
```

---

## 14.1 Create Job

```http
POST /api/v1/jobs
```

Request:

```json
{
  "name": "Send welcome email",
  "job_type": "SEND_EMAIL",
  "payload": {
    "to": "user@example.com",
    "subject": "Welcome",
    "body": "Hello"
  },
  "run_at": "2026-07-01T12:00:00Z",
  "priority": 5,
  "max_retries": 3,
  "retry_backoff_seconds": 30,
  "timeout_seconds": 300
}
```

Response:

```json
{
  "success": true,
  "data": {
    "job_id": "uuid",
    "status": "SCHEDULED"
  },
  "error": null,
  "request_id": "uuid"
}
```

---

## 14.2 List Jobs

```http
GET /api/v1/jobs?status=SCHEDULED&job_type=SEND_EMAIL&page=1&page_size=20
```

Response:

```json
{
  "success": true,
  "data": {
    "items": [],
    "page": 1,
    "page_size": 20,
    "total": 100
  },
  "error": null,
  "request_id": "uuid"
}
```

---

## 14.3 Get Job by ID

```http
GET /api/v1/jobs/{job_id}
```

---

## 14.4 Cancel Job

```http
POST /api/v1/jobs/{job_id}/cancel
```

Only jobs in `PENDING`, `SCHEDULED`, or `RETRY_SCHEDULED` should be cancellable.

---

## 14.5 Retry Job Manually

```http
POST /api/v1/jobs/{job_id}/retry
```

This should create a new scheduled execution if the job is in `FAILED` or `DEAD_LETTERED`.

---

## 14.6 Pause Job

```http
POST /api/v1/jobs/{job_id}/pause
```

---

## 14.7 Resume Job

```http
POST /api/v1/jobs/{job_id}/resume
```

---

## 14.8 Get Job Attempts

```http
GET /api/v1/jobs/{job_id}/attempts
```

---

## 14.9 Get Dead Letter Jobs

```http
GET /api/v1/dead-letter-jobs
```

---

## 14.10 Requeue Dead Letter Job

```http
POST /api/v1/dead-letter-jobs/{id}/requeue
```

---

## 14.11 Worker Status

```http
GET /api/v1/workers
```

This can read from Redis worker heartbeat keys.

Response:

```json
{
  "success": true,
  "data": [
    {
      "worker_id": "worker-1",
      "status": "active",
      "last_heartbeat": "2026-07-01T12:00:00Z"
    }
  ],
  "error": null,
  "request_id": "uuid"
}
```

---

# 15. Kafka Events

Use a standard event envelope.

```json
{
  "event_id": "uuid",
  "event_type": "job.created",
  "source": "scheduler-api",
  "entity_type": "job",
  "entity_id": "job_id",
  "timestamp": "2026-07-01T12:00:00Z",
  "payload": {}
}
```

---

## 15.1 Topics

```text
job.created
job.claimed
job.started
job.completed
job.failed
job.retry_scheduled
job.dead_lettered
job.cancelled
worker.events
```

---

## 15.2 Events

### job.created

Published when a job is created.

Payload:

```json
{
  "job_id": "uuid",
  "job_type": "SEND_EMAIL",
  "status": "SCHEDULED",
  "run_at": "2026-07-01T12:00:00Z",
  "priority": 5
}
```

### job.started

Published when a worker starts executing a job.

Payload:

```json
{
  "job_id": "uuid",
  "worker_id": "worker-1",
  "attempt_number": 1
}
```

### job.completed

Published when the job succeeds.

Payload:

```json
{
  "job_id": "uuid",
  "worker_id": "worker-1",
  "execution_duration_ms": 1200
}
```

### job.failed

Published when a job attempt fails.

Payload:

```json
{
  "job_id": "uuid",
  "worker_id": "worker-1",
  "attempt_number": 1,
  "error": "simulated email failure"
}
```

### job.retry_scheduled

Published when a job is scheduled for retry.

Payload:

```json
{
  "job_id": "uuid",
  "retry_count": 1,
  "next_run_at": "2026-07-01T12:01:00Z"
}
```

### job.dead_lettered

Published when max retries are exhausted.

Payload:

```json
{
  "job_id": "uuid",
  "retry_count": 3,
  "final_error": "max retries exceeded"
}
```

---

# 16. Configuration

Use environment variables.

Example `.env.example`:

```env
APP_ENV=local
API_PORT=8080
WORKER_CONCURRENCY=5
WORKER_POLL_INTERVAL_SECONDS=5
WORKER_BATCH_SIZE=10

POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=scheduler
POSTGRES_PASSWORD=scheduler
POSTGRES_DB=scheduler_db
POSTGRES_SSL_MODE=disable

REDIS_ADDR=localhost:6379
REDIS_PASSWORD=
REDIS_DB=0

KAFKA_BROKERS=localhost:9092

JOB_LOCK_TTL_SECONDS=360
JOB_DEFAULT_TIMEOUT_SECONDS=300
JOB_DEFAULT_MAX_RETRIES=3
JOB_DEFAULT_BACKOFF_SECONDS=30

LOG_LEVEL=debug
```

---

# 17. Docker Compose

Docker Compose should include:

- API service
- Worker service
- PostgreSQL
- Redis
- Kafka
- Zookeeper or Redpanda
- Kafka UI
- Optional pgAdmin

Recommended services:

```yaml
services:
  postgres:
    image: postgres:16

  redis:
    image: redis:7

  zookeeper:
    image: confluentinc/cp-zookeeper:latest

  kafka:
    image: confluentinc/cp-kafka:latest

  kafka-ui:
    image: provectuslabs/kafka-ui:latest

  api:
    build:
      context: .
      dockerfile: Dockerfile.api

  worker:
    build:
      context: .
      dockerfile: Dockerfile.worker
```

Local URLs:

```text
API:        http://localhost:8080
Kafka UI:   http://localhost:8081
PostgreSQL: localhost:5432
Redis:      localhost:6379
```

---

# 18. Makefile

Create a useful Makefile.

```makefile
up:
	docker compose up -d

down:
	docker compose down

build:
	docker compose build

logs:
	docker compose logs -f

logs-api:
	docker compose logs -f api

logs-worker:
	docker compose logs -f worker

test:
	go test ./...

race:
	go test -race ./...

fmt:
	go fmt ./...

lint:
	golangci-lint run

migrate-up:
	migrate -path migrations -database "$$DATABASE_URL" up

migrate-down:
	migrate -path migrations -database "$$DATABASE_URL" down

seed:
	./scripts/seed_jobs.sh

load-test:
	./scripts/run_load_test.sh
```

---

# 19. Implementation Phases

## Phase 0: Repository Setup

Goal: create the project structure and base files.

Tasks:

1. Create GitHub repository.
2. Create folder structure.
3. Add README.
4. Add `.gitignore`.
5. Add `.env.example`.
6. Add Docker Compose with PostgreSQL, Redis, Kafka, Kafka UI.
7. Add Makefile.
8. Add basic docs.

Deliverable:

```bash
make up
```

starts all dependencies successfully.

---

## Phase 1: Basic Go API Service

Goal: create the API service skeleton.

Tasks:

1. Create `cmd/api/main.go`.
2. Add HTTP server.
3. Add config loader.
4. Add logger.
5. Add request ID middleware.
6. Add recovery middleware.
7. Add `/health` endpoint.
8. Add `/ready` endpoint.

Deliverable:

```bash
curl http://localhost:8080/health
```

returns:

```json
{
  "status": "ok"
}
```

---

## Phase 2: PostgreSQL Connection and Migrations

Goal: set up the database layer.

Tasks:

1. Add PostgreSQL connection package.
2. Add migration tool.
3. Create jobs table.
4. Create job_attempts table.
5. Create dead_letter_jobs table.
6. Add migration commands.
7. Test DB connection in `/ready`.

Deliverable: migrations run successfully and API can connect to PostgreSQL.

---

## Phase 3: Job Creation API

Goal: allow users to create jobs.

Tasks:

1. Create job model.
2. Create job DTOs.
3. Create job repository.
4. Create job service.
5. Create job handler.
6. Validate job type.
7. Validate payload.
8. Validate run_at.
9. Set default retries and timeout.
10. Store job in database.

Deliverable: users can create jobs using the REST API.

---

## Phase 4: Job Listing and Job Detail APIs

Goal: allow users to inspect jobs.

Tasks:

1. Add `GET /api/v1/jobs`.
2. Add pagination.
3. Add filter by status.
4. Add filter by job_type.
5. Add `GET /api/v1/jobs/{id}`.
6. Add `GET /api/v1/jobs/{id}/attempts`.

Deliverable: users can list and inspect jobs.

---

## Phase 5: Worker Service Skeleton

Goal: create a separate worker service.

Tasks:

1. Create `cmd/worker/main.go`.
2. Load config.
3. Connect PostgreSQL.
4. Connect Redis.
5. Connect Kafka.
6. Add graceful shutdown.
7. Add worker ID generation.
8. Add heartbeat logging.

Deliverable: worker starts and logs its worker ID.

---

## Phase 6: Job Claiming Logic

Goal: workers can safely claim due jobs.

Tasks:

1. Implement repository method `ClaimDueJobs`.
2. Use `FOR UPDATE SKIP LOCKED`.
3. Update job status to `RUNNING`.
4. Set `locked_by`.
5. Set `locked_until`.
6. Return claimed jobs.
7. Add tests for claiming logic.

Deliverable: multiple workers can run without claiming the same job.

---

## Phase 7: Worker Pool Execution

Goal: execute jobs concurrently.

Tasks:

1. Add worker pool.
2. Add job channel.
3. Add configurable worker concurrency.
4. Add job executor interface.
5. Add fake job handlers:
   - SEND_EMAIL
   - CALL_WEBHOOK
   - GENERATE_REPORT
   - PROCESS_PAYMENT_RETRY
6. Add context timeout per job.
7. Add graceful shutdown.

Deliverable: due jobs are picked up and executed by workers.

---

## Phase 8: Job Attempts

Goal: track every execution attempt.

Tasks:

1. Insert job_attempt row when execution starts.
2. Update attempt on success.
3. Update attempt on failure.
4. Track execution duration.
5. Store error message.

Deliverable: each job execution has visible attempt history.

---

## Phase 9: Success and Failure Handling

Goal: correctly update job state.

Tasks:

1. On success:
   - Mark job as `SUCCEEDED`.
   - Set `completed_at`.
   - Clear lock fields.
2. On failure:
   - Mark job as `FAILED`.
   - Store last_error.
   - Increment retry_count.
3. Ensure updates are transactional.

Deliverable: jobs move to correct final or retryable state.

---

## Phase 10: Retry Scheduling

Goal: automatically retry failed jobs.

Tasks:

1. Implement retry policy.
2. Calculate next retry time.
3. Update status to `RETRY_SCHEDULED`.
4. Set new `run_at`.
5. Stop retrying after max retries.
6. Move job to dead-letter table after max retries.

Deliverable: failed jobs are retried automatically and dead-lettered after max retries.

---

## Phase 11: Redis Locking

Goal: add Redis lock for extra distributed execution safety.

Tasks:

1. Implement Redis lock package.
2. Use `SET NX EX`.
3. Add lock release logic.
4. Ensure only lock owner can release lock.
5. Add lock TTL.
6. Add tests for lock behavior.

Deliverable: workers acquire Redis job locks before execution.

---

## Phase 12: Kafka Event Publishing

Goal: publish lifecycle events.

Tasks:

1. Create Kafka producer package.
2. Create event envelope struct.
3. Publish event on job creation.
4. Publish event on job start.
5. Publish event on job success.
6. Publish event on job failure.
7. Publish event on retry scheduled.
8. Publish event on dead-lettered.

Deliverable: Kafka UI shows job lifecycle events.

---

## Phase 13: Manual Job Operations

Goal: add operational APIs.

Tasks:

1. Cancel job.
2. Pause job.
3. Resume job.
4. Retry job manually.
5. Requeue dead-letter job.
6. Validate status transitions.

Deliverable: users can control jobs through API.

---

## Phase 14: Worker Heartbeats

Goal: track active workers.

Tasks:

1. Worker writes heartbeat to Redis.
2. API reads active workers from Redis.
3. Add `/api/v1/workers` endpoint.
4. Remove stale workers after TTL.

Deliverable: users can see currently active workers.

---

## Phase 15: Observability

Goal: make the project production-like.

Tasks:

1. Add structured logs.
2. Add request ID.
3. Add Prometheus metrics.
4. Add metrics endpoint.
5. Track:
   - jobs_created_total
   - jobs_completed_total
   - jobs_failed_total
   - jobs_dead_lettered_total
   - job_execution_duration_seconds
   - active_workers
   - worker_claimed_jobs_total
6. Add optional Grafana dashboard.

Deliverable: metrics are available at `/metrics`.

---

## Phase 16: Testing

Goal: make the project credible and maintainable.

Unit tests should cover:

- Retry policy
- Status transitions
- Job validation
- Payload validation
- Worker pool behavior
- Redis lock logic

Integration tests should cover:

- Job creation
- Job claiming
- Multiple workers claiming jobs
- Job success flow
- Job failure retry flow
- Dead-letter flow

Run race tests:

```bash
go test -race ./...
```

Deliverable: project has meaningful tests.

---

## Phase 17: Dockerize API and Worker

Goal: run the entire project with Docker Compose.

Tasks:

1. Create Dockerfile for API.
2. Create Dockerfile for Worker.
3. Add services to Docker Compose.
4. Add dependency health checks.
5. Ensure migrations can run.
6. Add startup documentation.

Deliverable:

```bash
docker compose up --build
```

runs the full project.

---

## Phase 18: Documentation

Goal: make the GitHub repository strong.

Create docs:

```text
docs/architecture.md
docs/api-contracts.md
docs/database-design.md
docs/kafka-events.md
docs/worker-design.md
docs/redis-locking.md
docs/local-development.md
docs/future-improvements.md
```

README should include:

- Project overview
- Architecture diagram
- Features
- Tech stack
- Local setup
- API examples
- Worker behavior
- Database design summary
- Kafka event list
- Redis usage
- Screenshots/logs if available
- Future improvements

---

## Phase 19: GitHub Actions

Goal: add CI pipeline.

Pipeline should:

1. Checkout code.
2. Set up Go.
3. Run `go fmt` check.
4. Run tests.
5. Run race tests if possible.
6. Build API binary.
7. Build Worker binary.
8. Build Docker images.

Example workflow name:

```text
Go CI
```

---

# 20. Optional Frontend

This project does not require a frontend, but a small dashboard can make it visually stronger.

Frontend stack:

- Next.js
- TypeScript
- Tailwind CSS
- shadcn/ui

Pages:

```text
/jobs
/jobs/[id]
/dead-letter
/workers
/metrics
```

Features:

- Create job
- View jobs table
- Filter by status
- View job attempts
- Retry job
- Cancel job
- View workers
- View dead-letter jobs

Only build frontend after backend MVP is complete.

---

# 21. MVP Definition

The first usable version should include:

- Create job API
- List jobs API
- Get job API
- Worker service
- Worker pool
- PostgreSQL job claiming
- Basic job execution
- Retry handling
- Dead-letter handling
- Docker Compose setup

Do not add Kafka, Redis, metrics, or frontend until this is working.

---

# 22. Version Milestones

## Version 1.0 - Basic Scheduler

Includes:

- REST API
- PostgreSQL
- Job creation
- Job listing
- Worker service
- Job execution
- Job attempts
- Retry handling
- Dead-letter handling

## Version 1.1 - Distributed Safety

Includes:

- Redis locks
- Worker heartbeats
- Multiple worker instances
- Stale lock recovery

## Version 1.2 - Event Driven

Includes:

- Kafka producer
- Job lifecycle events
- Kafka UI setup
- Event documentation

## Version 1.3 - Operations

Includes:

- Cancel job
- Pause job
- Resume job
- Manual retry
- Requeue dead-letter job

## Version 1.4 - Observability

Includes:

- Prometheus metrics
- Structured logs
- Optional Grafana dashboard

## Version 1.5 - Frontend Dashboard

Includes:

- Jobs dashboard
- Job detail page
- Dead-letter page
- Worker status page

---

# 23. Important Edge Cases

Handle these edge cases:

1. Worker crashes after claiming a job.
2. Worker crashes while executing a job.
3. Job lock expires.
4. Redis lock exists but PostgreSQL lock expired.
5. Job exceeds timeout.
6. Job fails repeatedly.
7. Job reaches max retries.
8. Job is cancelled while pending.
9. Job is cancelled while running.
10. Multiple workers try to claim the same job.
11. Kafka publishing fails after job succeeds.
12. Database transaction fails midway.
13. Invalid job payload.
14. Unsupported job type.
15. Worker receives shutdown signal while processing jobs.

---

# 24. Recommended Libraries

## HTTP Router

Choose one:

```text
github.com/gin-gonic/gin
github.com/labstack/echo/v4
github.com/go-chi/chi/v5
```

Recommended: `chi` or `gin`.

## PostgreSQL

Recommended:

```text
github.com/jackc/pgx/v5
```

## Redis

```text
github.com/redis/go-redis/v9
```

## Kafka

Recommended for simplicity:

```text
github.com/segmentio/kafka-go
```

## Logger

Recommended:

```text
go.uber.org/zap
```

## UUID

```text
github.com/google/uuid
```

## Migrations

```text
github.com/golang-migrate/migrate
```

## Validation

```text
github.com/go-playground/validator/v10
```

## Cron Parsing Optional

```text
github.com/robfig/cron/v3
```

---

# 25. Suggested API Response Format

Success:

```json
{
  "success": true,
  "data": {},
  "error": null,
  "request_id": "uuid"
}
```

Error:

```json
{
  "success": false,
  "data": null,
  "error": {
    "code": "INVALID_JOB_TYPE",
    "message": "unsupported job type"
  },
  "request_id": "uuid"
}
```

---

# 26. Suggested Coding Standards

Use these standards:

- Keep handlers thin.
- Put business logic in services.
- Put database queries in repositories.
- Use context in every DB, Redis, and Kafka call.
- Avoid global variables.
- Use dependency injection.
- Add interfaces where useful.
- Use structured logs.
- Validate inputs at the API boundary.
- Keep SQL readable.
- Write tests for business logic.
- Keep commits small and meaningful.

---

# 27. Suggested Git Commit Plan

Use clean commits:

```text
initial project structure
add docker compose dependencies
add api service skeleton
add postgres migrations
implement job creation api
implement job listing api
add worker service skeleton
implement job claiming with skip locked
add worker pool execution
add job attempts tracking
implement retry and dead-letter flow
add redis distributed locking
add kafka lifecycle events
add worker heartbeat tracking
add operational job APIs
add prometheus metrics
add dockerfiles for api and worker
add github actions ci
improve documentation
```

---

# 28. README Opening

Use this in README:

```markdown
# Distributed Job Scheduler

A production-style distributed job scheduler built with Go, PostgreSQL, Redis, Kafka, and Docker.

This project provides APIs for creating scheduled and delayed jobs, executes them using horizontally scalable workers, supports retries and dead-letter handling, and publishes job lifecycle events through Kafka.

The system demonstrates distributed job claiming, worker coordination, Redis-based locking, PostgreSQL transactional processing, Go concurrency patterns, and production-style observability.
```

---

# 29. Final Resume Bullets

After completing the project, use:

```text
Distributed Job Scheduler | Go, PostgreSQL, Redis, Kafka, Docker

• Built a distributed backend job scheduler supporting delayed jobs, priority-based execution, retries, dead-letter queues, worker heartbeats, and operational job controls.

• Implemented PostgreSQL-based transactional job claiming with SELECT FOR UPDATE SKIP LOCKED, Redis-based execution locks, Kafka lifecycle events, and Go worker pools for horizontally scalable job processing.

• Added Docker Compose setup, structured logging, health checks, graceful shutdown, and automated tests covering job creation, retries, distributed claiming, and dead-letter flows.
```

---

# 30. Codex Prompt 1 - Initial Setup

Paste this into Codex first:

```text
I want to build a production-style Distributed Job Scheduler using Go, PostgreSQL, Redis, Kafka, and Docker.

Create the initial monorepo structure:

distributed-job-scheduler/
- cmd/api/main.go
- cmd/worker/main.go
- internal/config
- internal/logger
- internal/server
- internal/middleware
- internal/response
- internal/errors
- internal/job
- internal/worker
- internal/scheduler
- internal/lock
- internal/kafka
- internal/postgres
- internal/redis
- migrations
- docs
- scripts
- tests
- README.md
- docker-compose.yml
- Makefile
- .env.example
- .gitignore

For now:
1. Implement a basic API service with /health and /ready endpoints.
2. Implement a basic worker service that starts, logs its worker ID, and shuts down gracefully.
3. Add Docker Compose for PostgreSQL 16, Redis 7, Kafka, Zookeeper, and Kafka UI.
4. Add a Makefile with up, down, logs, test, fmt, lint commands.
5. Add README with project overview and local setup.

Do not implement business logic yet. Focus on clean project structure and runnable local development setup.
```

---

# 31. Codex Prompt 2 - Database and Job APIs

```text
Now implement PostgreSQL migrations and the Job API.

Requirements:
1. Use pgx/v5 for PostgreSQL.
2. Add migrations for:
   - jobs
   - job_attempts
   - dead_letter_jobs
   - job_events
3. Implement job model, DTOs, repository, service, and handler.
4. Add APIs:
   - POST /api/v1/jobs
   - GET /api/v1/jobs
   - GET /api/v1/jobs/{id}
   - GET /api/v1/jobs/{id}/attempts
5. Add validation:
   - job_type is required
   - payload is required
   - run_at is required
   - priority defaults to 5
   - max_retries defaults to 3
   - timeout_seconds defaults to 300
6. Add pagination and filtering by status and job_type.
7. Use standard success/error response format.
8. Add unit tests for job service validation.

Keep handlers thin and put business logic in service layer.
```

---

# 32. Codex Prompt 3 - Worker Claiming and Execution

```text
Now implement the Worker Service job claiming and execution flow.

Requirements:
1. Implement ClaimDueJobs in the job repository using PostgreSQL SELECT FOR UPDATE SKIP LOCKED.
2. Workers should claim jobs where:
   - status is PENDING, SCHEDULED, or RETRY_SCHEDULED
   - run_at <= now()
   - locked_until is null or expired
3. When claimed:
   - set status to RUNNING
   - set locked_by to worker_id
   - set locked_until to now + configured timeout
4. Implement worker pool with configurable concurrency.
5. Add job handlers for:
   - SEND_EMAIL
   - CALL_WEBHOOK
   - GENERATE_REPORT
   - PROCESS_PAYMENT_RETRY
6. Add context timeout per job.
7. Track job attempts in job_attempts table.
8. On success, mark job SUCCEEDED.
9. On failure, mark job FAILED for now.

Add integration tests proving that multiple workers do not claim the same job.
```

---

# 33. Codex Prompt 4 - Retry and Dead Letter

```text
Now implement retry handling and dead-letter queue.

Requirements:
1. Add retry policy using exponential backoff:
   next_retry_delay = retry_backoff_seconds * 2 ^ retry_count
2. If a job fails and retry_count < max_retries:
   - increment retry_count
   - set status to RETRY_SCHEDULED
   - set run_at to next retry time
   - clear locked_by and locked_until
3. If retry_count >= max_retries:
   - set status to DEAD_LETTERED
   - insert row into dead_letter_jobs
   - clear locked fields
4. Add APIs:
   - GET /api/v1/dead-letter-jobs
   - POST /api/v1/dead-letter-jobs/{id}/requeue
5. Add tests for:
   - retry scheduling
   - exponential backoff
   - dead-letter insertion
   - requeue flow
```

---

# 34. Codex Prompt 5 - Redis Locks and Worker Heartbeats

```text
Now add Redis-based distributed locks and worker heartbeat tracking.

Requirements:
1. Use go-redis/v9.
2. Implement Redis lock package using SET NX EX.
3. Lock key format:
   lock:job:{job_id}
4. Lock value should be worker_id.
5. Only the worker that owns the lock should release it.
6. Worker must acquire Redis lock before executing a claimed job.
7. Add worker heartbeat:
   - key: worker:{worker_id}:heartbeat
   - TTL: 30 seconds
   - refresh every 10 seconds
8. Add API:
   - GET /api/v1/workers
9. Return active workers based on Redis heartbeat keys.
10. Add tests for lock acquire/release behavior.
```

---

# 35. Codex Prompt 6 - Kafka Events

```text
Now add Kafka lifecycle event publishing.

Requirements:
1. Use segmentio/kafka-go.
2. Create a common event envelope:
   - event_id
   - event_type
   - source
   - entity_type
   - entity_id
   - timestamp
   - payload
3. Publish events:
   - job.created
   - job.started
   - job.completed
   - job.failed
   - job.retry_scheduled
   - job.dead_lettered
   - job.cancelled
4. Add topics to Docker Compose initialization if needed.
5. Log publish success and failure.
6. Add Kafka UI to docker-compose for viewing events.
7. Add docs/kafka-events.md with event contracts and sample payloads.
```

---

# 36. Codex Prompt 7 - Operational APIs

```text
Now add operational job APIs.

Requirements:
1. Implement:
   - POST /api/v1/jobs/{id}/cancel
   - POST /api/v1/jobs/{id}/pause
   - POST /api/v1/jobs/{id}/resume
   - POST /api/v1/jobs/{id}/retry
2. Add status transition validation.
3. Only allow cancellation for PENDING, SCHEDULED, and RETRY_SCHEDULED jobs.
4. Only allow pause for PENDING and SCHEDULED jobs.
5. Only allow resume for PAUSED jobs.
6. Manual retry should be allowed for FAILED and DEAD_LETTERED jobs.
7. Publish Kafka events for each operation.
8. Add tests for valid and invalid status transitions.
```

---

# 37. Codex Prompt 8 - Observability and CI

```text
Now add observability and CI.

Requirements:
1. Add Prometheus metrics endpoint /metrics.
2. Track:
   - jobs_created_total
   - jobs_completed_total
   - jobs_failed_total
   - jobs_dead_lettered_total
   - job_execution_duration_seconds
   - worker_claimed_jobs_total
   - active_workers
3. Add structured logging with request_id.
4. Add graceful shutdown logs.
5. Add GitHub Actions workflow:
   - go fmt check
   - go test ./...
   - go test -race ./...
   - go build ./cmd/api
   - go build ./cmd/worker
6. Update README with:
   - architecture
   - local setup
   - API examples
   - worker design
   - Redis locking
   - Kafka events
   - testing instructions
```

---

# 38. Build Order Summary

Follow this exact order:

```text
1. Project structure
2. Docker Compose dependencies
3. API health endpoints
4. Worker skeleton
5. PostgreSQL migrations
6. Job creation API
7. Job listing/detail APIs
8. Worker job claiming
9. Worker pool execution
10. Job attempts tracking
11. Retry handling
12. Dead-letter handling
13. Redis locks
14. Worker heartbeats
15. Kafka lifecycle events
16. Operational APIs
17. Metrics and logs
18. Dockerfiles
19. GitHub Actions
20. Documentation polish
21. Optional frontend
```

---

# 39. Final Notes

Keep the first version small and working.

Do not start with Kafka, Redis, or frontend.

The core value of the project is:

1. Reliable job persistence in PostgreSQL.
2. Safe distributed claiming using `FOR UPDATE SKIP LOCKED`.
3. Worker pool execution in Go.
4. Retry and dead-letter logic.
5. Redis locking for distributed execution safety.
6. Kafka events for job lifecycle visibility.
7. Clean documentation and GitHub presentation.

Once the MVP is stable, add Redis, Kafka, metrics, and optional frontend.
