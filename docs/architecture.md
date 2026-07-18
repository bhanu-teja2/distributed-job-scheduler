# Architecture

The system is split into independently scalable API, worker, event-relay, and dashboard processes. The API validates and persists tenant-owned jobs in PostgreSQL. Workers poll PostgreSQL for due jobs and claim rows with `FOR UPDATE SKIP LOCKED`, allowing replicas to select disjoint work without a leader.

PostgreSQL is the source of truth for jobs, attempts, dead-letter records, and the lifecycle-event outbox. Redis provides secondary renewable execution leases, expiring worker heartbeats, and API rate limiting. The event relay publishes committed outbox rows to Kafka with at-least-once delivery. The dashboard uses the authenticated REST API and does not bypass tenant or role checks.

See [interview-guide.md](interview-guide.md) for the complete runtime walkthrough and design tradeoffs.
