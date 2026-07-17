# Interview Guide

## Thirty-Second Explanation

This scheduler accepts delayed webhook jobs and runs them across horizontally scalable Go workers. PostgreSQL atomically claims work with `SKIP LOCKED`, Redis provides renewable owner-checked leases, and a transactional outbox ensures committed lifecycle changes are eventually published to Kafka even when the broker is unavailable.

## Strong Design Points

1. **Duplicate claims:** row locks and `SKIP LOCKED` let workers claim disjoint batches without a leader or database contention queue.
2. **Worker crashes:** a renewable lease marks ownership; expired running attempts are failed and retried or dead-lettered transactionally.
3. **Lost Kafka events:** lifecycle rows are inserted in the same transaction as job state and independently relayed with at-least-once delivery.
4. **Duplicate side effects:** exactly-once HTTP execution is not achievable across a database and remote service, so stable idempotency headers are part of the contract.
5. **Tenant isolation:** authentication establishes tenant identity; every API repository query includes that tenant key.
6. **Unsafe callbacks:** URL validation and connection-time IP validation block local/private targets unless an operator allowlists a host.

## Tradeoffs

- PostgreSQL polling is simpler and more auditable than making Kafka the job queue, but very high scheduling throughput would require partitioning or queue sharding.
- Redis is a secondary guard, not the source of truth; temporary Redis loss stops protected execution rather than silently weakening ownership.
- At-least-once delivery can duplicate events, so consumers deduplicate by `event_id`.
- API keys fit service-to-service scheduling; OIDC would be the next choice for human-facing enterprise access.

## Scaling Discussion

Scale workers independently, tune claim batch size relative to concurrency, and index tenant/status/run time. At larger scale, partition jobs by scheduled date or tenant, isolate outbox relay partitions by event key, archive terminal jobs, and use autoscaling based on due-job and outbox backlog rather than CPU alone.
