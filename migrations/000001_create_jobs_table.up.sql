CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    job_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL,
    priority INT NOT NULL DEFAULT 5,
    run_at TIMESTAMP WITH TIME ZONE NOT NULL,
    locked_until TIMESTAMP WITH TIME ZONE,
    locked_by TEXT,
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    retry_backoff_seconds INT NOT NULL DEFAULT 30,
    timeout_seconds INT NOT NULL DEFAULT 300,
    last_error TEXT,
    created_by TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    completed_at TIMESTAMP WITH TIME ZONE,
    failed_at TIMESTAMP WITH TIME ZONE,
    cancelled_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS idx_jobs_status_run_at ON jobs(status, run_at);
CREATE INDEX IF NOT EXISTS idx_jobs_priority_run_at ON jobs(priority DESC, run_at ASC);
CREATE INDEX IF NOT EXISTS idx_jobs_locked_until ON jobs(locked_until);
CREATE INDEX IF NOT EXISTS idx_jobs_job_type ON jobs(job_type);
