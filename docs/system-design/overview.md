# System Overview

The API authenticates a tenant-scoped API key before any job query. Workers claim from all tenants but retain tenant identity on attempts, dead letters, and lifecycle events. Kafka is downstream of the durable event relay and is never part of the job-state transaction.

```mermaid
flowchart LR
    Dashboard["React operations dashboard"] --> Auth["API key auth and RBAC"]
    Client["Service client"] --> Auth
    Auth --> API["Scheduler API"]
    API --> DB[("PostgreSQL jobs, attempts, DLQ, outbox")]
    WorkerA["Worker replica A"] --> DB
    WorkerB["Worker replica B"] --> DB
    WorkerA --> Redis[("Redis locks and heartbeats")]
    WorkerB --> Redis
    WorkerA --> Target["Allowlisted webhook target"]
    WorkerB --> Target
    Relay["Event relay replicas"] --> DB
    Relay --> Kafka[("Kafka scheduler.events")]
```

The Distributed Job Scheduler accepts scheduled work through an API, persists jobs in PostgreSQL, executes due jobs through horizontally scalable workers, coordinates execution with Redis locks, and publishes lifecycle events to Kafka.

## Goals

- Reliable job persistence and inspection.
- Safe multi-worker job claiming.
- Retry, timeout, and dead-letter handling.
- Operational controls for cancel, pause, resume, retry, and requeue.
- Kubernetes-ready deployment.

## Non-Goals

- Real email, payment, or report integrations beyond the implemented webhook executor.
- Exactly-once execution guarantees across all external side effects.
- Workflow DAGs, cron recurrence, billing, and multi-region consensus.

## Container Diagram

```mermaid
flowchart LR
    Client["API client"] --> API["Scheduler API"]
    API --> PG[("PostgreSQL")]
    API --> Redis[("Redis rate limits")]
    Worker["Worker service"] --> PG
    Worker --> Redis
    Worker --> Handlers["HTTP webhook targets"]
    Relay["Event relay"] --> PG
    Relay --> Kafka[("Kafka lifecycle events")]
    Dashboard["React dashboard"] --> API
    Ops["Prometheus / Operators"] --> API
```
