# Kafka Events

The Kafka package defines an event envelope:

```json
{
  "event_id": "uuid",
  "event_type": "job.completed",
  "source": "worker",
  "entity_type": "job",
  "entity_id": "job_id",
  "timestamp": "2026-07-01T12:00:00Z",
  "payload": {}
}
```

Planned lifecycle events include `job.created`, `job.started`, `job.completed`, `job.failed`, `job.retry_scheduled`, `job.dead_lettered`, and `job.cancelled`.
