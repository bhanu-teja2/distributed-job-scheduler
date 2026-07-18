# Event Design

Lifecycle events are no longer published inline. Each state transaction inserts a versioned `job_events` row, and relay replicas lease unpublished rows with `FOR UPDATE SKIP LOCKED`.

```mermaid
sequenceDiagram
    participant W as Worker
    participant P as PostgreSQL
    participant R as Event relay
    participant K as Kafka
    W->>P: Commit attempt + job state + event
    P-->>W: Transaction committed
    R->>P: Lease unpublished events
    R->>K: Publish keyed by job_id
    alt Kafka accepts event
        R->>P: Set published_at
    else Kafka unavailable
        R->>P: Store error and next_publish_at
    end
```

Delivery is durable and at least once. A relay crash between Kafka acknowledgement and `published_at` can publish the same `event_id` again, so consumers must deduplicate it. PostgreSQL remains the source of truth and retains unpublished events while Kafka is unavailable.

Envelope:

```json
{
  "event_id": "uuid",
  "schema_version": 1,
  "tenant_id": "uuid",
  "event_type": "job.completed",
  "source": "worker",
  "entity_type": "job",
  "entity_id": "job_id",
  "occurred_at": "2026-07-02T12:00:00Z",
  "correlation_id": "request-or-workflow-id",
  "causation_id": "uuid",
  "payload": {}
}
```

Events:

- `job.created`
- `job.started`
- `job.completed`
- `job.failed`
- `job.retry_scheduled`
- `job.dead_lettered`
- `job.cancelled`
- `job.requeued`
