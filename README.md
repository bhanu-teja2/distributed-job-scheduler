# Distributed Job Scheduler

A production-style distributed job scheduler built with Go, PostgreSQL, Redis, Kafka, and Docker.

This project provides APIs for creating scheduled and delayed jobs, executes them using horizontally scalable workers, supports retries and dead-letter handling, and includes scaffolding for Redis locking and Kafka lifecycle events.

## Features

- REST API with `/health`, `/ready`, and `/api/v1/jobs`.
- PostgreSQL-backed job persistence and attempt history.
- Safe worker claiming with `SELECT ... FOR UPDATE SKIP LOCKED`.
- Concurrent worker execution with per-job timeout.
- Exponential retry scheduling and dead-letter queue.
- Docker Compose for PostgreSQL, Redis, Kafka, Kafka UI, API, and worker.
- Unit tests for validation, retry policy, and worker state handling.

## Local Setup

```bash
cp .env.example .env
make up
export DATABASE_URL="postgres://scheduler:scheduler@localhost:5432/scheduler_db?sslmode=disable"
make migrate-up
make seed
```

API: `http://localhost:8080`
Kafka UI: `http://localhost:8081`

## API Examples

```bash
curl http://localhost:8080/health
```

```bash
curl -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Send welcome email",
    "job_type": "SEND_EMAIL",
    "payload": {"to":"user@example.com","subject":"Welcome","body":"Hello"},
    "run_at": "2026-07-01T12:00:00Z",
    "priority": 5
  }'
```

```bash
curl "http://localhost:8080/api/v1/jobs?status=SCHEDULED&page=1&page_size=20"
```

## Worker Behavior

The worker polls due jobs, claims a batch transactionally in PostgreSQL, records an attempt, executes a simulated job handler, and then marks the job as succeeded, retry scheduled, or dead-lettered.

## Testing

```bash
make fmt
make test
make race
```

## Roadmap

- Wire Redis locks into the worker execution path.
- Publish lifecycle events from API and worker flows through Kafka.
- Add operational APIs for cancel, pause, resume, and manual retry.
- Add Prometheus metrics and an optional dashboard.
