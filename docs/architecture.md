# Architecture

The system is split into an API process and a worker process. The API validates and persists jobs in PostgreSQL. The worker polls PostgreSQL for due jobs and claims rows with `FOR UPDATE SKIP LOCKED`, allowing multiple worker processes to run without processing the same row twice.

PostgreSQL is the source of truth for jobs, attempts, and dead-letter records. Redis and Kafka packages are included for the distributed-safety and event-driven milestones.
