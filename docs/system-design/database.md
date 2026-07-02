# Database Design

PostgreSQL is the source of truth. Redis and Kafka are coordination and eventing layers.

```mermaid
erDiagram
    jobs ||--o{ job_attempts : records
    jobs ||--o{ job_events : emits
    jobs ||--o| dead_letter_jobs : dead_letters
    jobs {
        uuid id PK
        text name
        text job_type
        jsonb payload
        text status
        int priority
        timestamptz run_at
        text locked_by
        timestamptz locked_until
        int retry_count
        int max_retries
    }
    job_attempts {
        uuid id PK
        uuid job_id FK
        text worker_id
        int attempt_number
        text status
        bigint execution_duration_ms
    }
    dead_letter_jobs {
        uuid id PK
        uuid original_job_id
        jsonb payload
        text final_error
    }
    job_events {
        uuid id PK
        uuid job_id
        text event_type
        jsonb payload
    }
```

Transactional boundaries:

- claim due jobs and set lock owner in one SQL update
- attempt finalization and job status change must match the owning worker
- dead-letter insertion and job status update happen in one transaction
