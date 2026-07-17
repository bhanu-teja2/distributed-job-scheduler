DROP INDEX IF EXISTS idx_job_events_tenant_job;
DROP INDEX IF EXISTS idx_job_events_outbox;
ALTER TABLE job_events
    DROP COLUMN IF EXISTS claimed_until,
    DROP COLUMN IF EXISTS claimed_by,
    DROP COLUMN IF EXISTS last_publish_error,
    DROP COLUMN IF EXISTS next_publish_at,
    DROP COLUMN IF EXISTS publish_attempts,
    DROP COLUMN IF EXISTS published_at,
    DROP COLUMN IF EXISTS causation_id,
    DROP COLUMN IF EXISTS correlation_id,
    DROP COLUMN IF EXISTS schema_version,
    DROP COLUMN IF EXISTS tenant_id;
ALTER TABLE dead_letter_jobs DROP COLUMN IF EXISTS requeued_job_id, DROP COLUMN IF EXISTS requeued_at, DROP COLUMN IF EXISTS tenant_id;
ALTER TABLE job_attempts
    DROP COLUMN IF EXISTS retryable,
    DROP COLUMN IF EXISTS error_code,
    DROP COLUMN IF EXISTS result,
    DROP COLUMN IF EXISTS tenant_id;
DROP INDEX IF EXISTS idx_jobs_tenant_status_run_at;
DROP INDEX IF EXISTS uq_jobs_tenant_idempotency;
ALTER TABLE jobs
    DROP COLUMN IF EXISTS source_dead_letter_id,
    DROP COLUMN IF EXISTS request_hash,
    DROP COLUMN IF EXISTS idempotency_key,
    DROP COLUMN IF EXISTS tenant_id;
DROP TABLE IF EXISTS api_clients;
DROP TABLE IF EXISTS tenants;
