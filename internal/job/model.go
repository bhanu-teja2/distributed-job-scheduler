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
	TenantID            uuid.UUID       `json:"tenant_id"`
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
	IdempotencyKey      *string         `json:"-"`
	RequestHash         *string         `json:"-"`
	ActiveAttemptID     uuid.UUID       `json:"-"`
	AttemptNumber       int             `json:"-"`
}

type Attempt struct {
	ID                  uuid.UUID       `json:"id"`
	JobID               uuid.UUID       `json:"job_id"`
	WorkerID            string          `json:"worker_id"`
	AttemptNumber       int             `json:"attempt_number"`
	Status              string          `json:"status"`
	StartedAt           time.Time       `json:"started_at"`
	CompletedAt         *time.Time      `json:"completed_at,omitempty"`
	FailedAt            *time.Time      `json:"failed_at,omitempty"`
	ErrorMessage        *string         `json:"error_message,omitempty"`
	ExecutionDurationMS *int64          `json:"execution_duration_ms,omitempty"`
	CreatedAt           time.Time       `json:"created_at"`
	Result              json.RawMessage `json:"result,omitempty"`
	ErrorCode           *string         `json:"error_code,omitempty"`
	Retryable           *bool           `json:"retryable,omitempty"`
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
	TenantID      uuid.UUID       `json:"tenant_id"`
	RequeuedAt    *time.Time      `json:"requeued_at,omitempty"`
	RequeuedJobID *uuid.UUID      `json:"requeued_job_id,omitempty"`
}

type Event struct {
	ID               uuid.UUID       `json:"event_id"`
	JobID            uuid.UUID       `json:"job_id"`
	TenantID         uuid.UUID       `json:"tenant_id"`
	EventType        string          `json:"event_type"`
	Source           string          `json:"source"`
	SchemaVersion    int             `json:"schema_version"`
	CorrelationID    *string         `json:"correlation_id,omitempty"`
	CausationID      *uuid.UUID      `json:"causation_id,omitempty"`
	Payload          json.RawMessage `json:"payload"`
	CreatedAt        time.Time       `json:"occurred_at"`
	PublishedAt      *time.Time      `json:"published_at,omitempty"`
	PublishAttempts  int             `json:"publish_attempts"`
	LastPublishError *string         `json:"last_publish_error,omitempty"`
}

type DashboardSummary struct {
	StatusCounts       map[Status]int64 `json:"status_counts"`
	ActiveWorkers      int              `json:"active_workers"`
	OutboxBacklog      int64            `json:"outbox_backlog"`
	FailuresLast24Hour int64            `json:"failures_last_24_hours"`
}

type ExecutionResult struct {
	Metadata  json.RawMessage
	ErrorCode string
	Retryable bool
	Message   string
}
