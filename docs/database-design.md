# Database Design

The database has four tables:

- `jobs`: source of truth for scheduled work and current status.
- `job_attempts`: one row per execution attempt.
- `dead_letter_jobs`: permanently failed jobs after retries are exhausted.
- `job_events`: local audit/event history for future Kafka-backed flows.

Workers claim due jobs by status, `run_at`, and expired lock state, ordered by priority and age.
