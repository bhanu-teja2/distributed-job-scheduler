# ADR 0002: At-Least-Once Events Through a Transactional Outbox

**Status:** Accepted

Lifecycle events are persisted with state changes and relayed asynchronously to Kafka. A crash after Kafka accepts an event but before PostgreSQL records publication may produce a duplicate; consumers therefore deduplicate by `event_id`. This is preferred to best-effort publication, which can silently lose committed events.
