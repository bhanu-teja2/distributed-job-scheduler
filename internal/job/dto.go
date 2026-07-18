package job

import (
	"encoding/json"
	"time"
)

// CreateRequest is the validated input for a scheduled webhook job.
type CreateRequest struct {
	Name                string          `json:"name"`
	JobType             string          `json:"job_type"`
	Payload             json.RawMessage `json:"payload"`
	RunAt               time.Time       `json:"run_at"`
	Priority            int             `json:"priority"`
	MaxRetries          int             `json:"max_retries"`
	RetryBackoffSeconds int             `json:"retry_backoff_seconds"`
	TimeoutSeconds      int             `json:"timeout_seconds"`
	IdempotencyKey      string          `json:"-"`
}

// CreateResponse identifies a newly created or idempotently replayed job.
type CreateResponse struct {
	JobID    string `json:"job_id"`
	Status   Status `json:"status"`
	Replayed bool   `json:"-"`
}

// ListFilter contains validated tenant-scoped list and ordering criteria.
type ListFilter struct {
	Status        Status
	JobType       string
	Page          int
	PageSize      int
	Sort          string
	Order         string
	CreatedAfter  *time.Time
	CreatedBefore *time.Time
}

// Page is a bounded page of jobs and its total matching count.
type Page struct {
	Items    []Job `json:"items"`
	Page     int   `json:"page"`
	PageSize int   `json:"page_size"`
	Total    int64 `json:"total"`
}
