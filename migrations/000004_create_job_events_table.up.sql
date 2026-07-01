CREATE TABLE IF NOT EXISTS job_events (
    id UUID PRIMARY KEY,
    job_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    source TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);
