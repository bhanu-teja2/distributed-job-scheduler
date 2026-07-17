# ADR 0001: PostgreSQL Is the Scheduler Source of Truth

**Status:** Accepted

Jobs, attempts, dead letters, and lifecycle events share PostgreSQL transactions. Redis and Kafka may be temporarily unavailable without becoming authoritative for job state. This enables ownership checks and crash recovery from durable data, at the cost of polling and database write load.
