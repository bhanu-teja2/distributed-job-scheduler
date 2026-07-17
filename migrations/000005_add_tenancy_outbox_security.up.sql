CREATE TABLE IF NOT EXISTS tenants (
    id UUID PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO tenants (id, slug, name)
VALUES ('00000000-0000-0000-0000-000000000001', 'default', 'Default Tenant')
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS api_clients (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    name TEXT NOT NULL,
    key_prefix TEXT NOT NULL,
    key_hash TEXT NOT NULL UNIQUE,
    role TEXT NOT NULL CHECK (role IN ('viewer', 'operator', 'admin')),
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_api_clients_tenant ON api_clients(tenant_id, enabled);

ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id),
    ADD COLUMN IF NOT EXISTS idempotency_key TEXT,
    ADD COLUMN IF NOT EXISTS request_hash TEXT,
    ADD COLUMN IF NOT EXISTS source_dead_letter_id UUID;

CREATE UNIQUE INDEX IF NOT EXISTS uq_jobs_tenant_idempotency
    ON jobs(tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_tenant_status_run_at
    ON jobs(tenant_id, status, run_at, priority DESC);

ALTER TABLE job_attempts
    ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id),
    ADD COLUMN IF NOT EXISTS result JSONB,
    ADD COLUMN IF NOT EXISTS error_code TEXT,
    ADD COLUMN IF NOT EXISTS retryable BOOLEAN;

ALTER TABLE dead_letter_jobs
    ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id),
    ADD COLUMN IF NOT EXISTS requeued_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS requeued_job_id UUID REFERENCES jobs(id);

ALTER TABLE job_events
    ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id),
    ADD COLUMN IF NOT EXISTS schema_version INT NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS correlation_id TEXT,
    ADD COLUMN IF NOT EXISTS causation_id UUID,
    ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS publish_attempts INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS next_publish_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS last_publish_error TEXT,
    ADD COLUMN IF NOT EXISTS claimed_by TEXT,
    ADD COLUMN IF NOT EXISTS claimed_until TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_job_events_outbox
    ON job_events(next_publish_at, created_at)
    WHERE published_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_job_events_tenant_job
    ON job_events(tenant_id, job_id, created_at, id);
