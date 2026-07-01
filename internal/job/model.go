package job

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Status string

const (
	StatusPending        Status = "PENDING"
	StatusScheduled      Status = "SCHEDULED"
	StatusRunning        Status = "RUNNING"
	StatusSucceeded      Status = "SUCCEEDED"
	StatusFailed         Status = "FAILED"
	StatusRetryScheduled Status = "RETRY_SCHEDULED"
	StatusDeadLettered   Status = "DEAD_LETTERED"
	StatusCancelled      Status = "CANCELLED"
	StatusPaused         Status = "PAUSED"
)

type Job struct {
	ID                  uuid.UUID       `json:"id"`
	Name                string          `json:"name"`
	JobType             string          `json:"job_type"`
	Payload             json.RawMessage `json:"payload"`
	Status              Status          `json:"status"`
	Priority            int             `json:"priority"`
	RunAt               time.Time       `json:"run_at"`
	LockedUntil         *time.Time      `json:"locked_until,omitempty"`
	LockedBy            *string         `json:"locked_by,omitempty"`
	RetryCount          int             `json:"retry_count"`
	MaxRetries          int             `json:"max_retries"`
	RetryBackoffSeconds int             `json:"retry_backoff_seconds"`
	TimeoutSeconds      int             `json:"timeout_seconds"`
	LastError           *string         `json:"last_error,omitempty"`
	CreatedBy           *string         `json:"created_by,omitempty"`
	CreatedAt           time.Time       `json:"created_at"`
	UpdatedAt           time.Time       `json:"updated_at"`
	CompletedAt         *time.Time      `json:"completed_at,omitempty"`
	FailedAt            *time.Time      `json:"failed_at,omitempty"`
	CancelledAt         *time.Time      `json:"cancelled_at,omitempty"`
}

type Attempt struct {
	ID                  uuid.UUID  `json:"id"`
	JobID               uuid.UUID  `json:"job_id"`
	WorkerID            string     `json:"worker_id"`
	AttemptNumber       int        `json:"attempt_number"`
	Status              string     `json:"status"`
	StartedAt           time.Time  `json:"started_at"`
	CompletedAt         *time.Time `json:"completed_at,omitempty"`
	FailedAt            *time.Time `json:"failed_at,omitempty"`
	ErrorMessage        *string    `json:"error_message,omitempty"`
	ExecutionDurationMS *int64     `json:"execution_duration_ms,omitempty"`
	CreatedAt           time.Time  `json:"created_at"`
}

type DeadLetterJob struct {
	ID            uuid.UUID       `json:"id"`
	OriginalJobID uuid.UUID       `json:"original_job_id"`
	Name          string          `json:"name"`
	JobType       string          `json:"job_type"`
	Payload       json.RawMessage `json:"payload"`
	FinalError    string          `json:"final_error"`
	RetryCount    int             `json:"retry_count"`
	FailedAt      time.Time       `json:"failed_at"`
	CreatedAt     time.Time       `json:"created_at"`
}
