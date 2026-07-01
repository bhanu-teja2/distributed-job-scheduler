CREATE TABLE IF NOT EXISTS dead_letter_jobs (
    id UUID PRIMARY KEY,
    original_job_id UUID NOT NULL,
    name TEXT NOT NULL,
    job_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    final_error TEXT NOT NULL,
    retry_count INT NOT NULL,
    failed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dead_letter_jobs_original_job_id ON dead_letter_jobs(original_job_id);
