package job

import (
	"encoding/json"
	"time"
)

type CreateRequest struct {
	Name                string          `json:"name"`
	JobType             string          `json:"job_type"`
	Payload             json.RawMessage `json:"payload"`
	RunAt               time.Time       `json:"run_at"`
	Priority            int             `json:"priority"`
	MaxRetries          int             `json:"max_retries"`
	RetryBackoffSeconds int             `json:"retry_backoff_seconds"`
	TimeoutSeconds      int             `json:"timeout_seconds"`
	CreatedBy           string          `json:"created_by"`
}

type CreateResponse struct {
	JobID  string `json:"job_id"`
	Status Status `json:"status"`
}

type ListFilter struct {
	Status   Status
	JobType  string
	Page     int
	PageSize int
}

type Page struct {
	Items    []Job `json:"items"`
	Page     int   `json:"page"`
	PageSize int   `json:"page_size"`
	Total    int64 `json:"total"`
}
