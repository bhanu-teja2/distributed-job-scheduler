package events

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

const (
	// Job lifecycle event names form the stable Kafka contract.
	JobCreated        = "job.created"
	JobClaimed        = "job.claimed"
	JobStarted        = "job.started"
	JobCompleted      = "job.completed"
	JobFailed         = "job.failed"
	JobRetryScheduled = "job.retry_scheduled"
	JobDeadLettered   = "job.dead_lettered"
	JobCancelled      = "job.cancelled"
	WorkerHeartbeat   = "worker.heartbeat"
)

// Publisher sends a durable event to an external event transport.
type Publisher interface {
	Publish(ctx context.Context, event Event) error
}

// Event is the versioned lifecycle envelope published by the outbox relay.
type Event struct {
	EventID         uuid.UUID       `json:"event_id"`
	SchemaVersion   int             `json:"schema_version"`
	TenantID        uuid.UUID       `json:"tenant_id"`
	EventType       string          `json:"event_type"`
	Source          string          `json:"source"`
	EntityType      string          `json:"entity_type"`
	EntityID        string          `json:"entity_id"`
	OccurredAt      time.Time       `json:"occurred_at"`
	CorrelationID   string          `json:"correlation_id,omitempty"`
	CausationID     *uuid.UUID      `json:"causation_id,omitempty"`
	Payload         json.RawMessage `json:"payload"`
	PublishAttempts int             `json:"-"`
}

// NoopPublisher accepts events without external delivery, primarily for tests.
type NoopPublisher struct{}

// Publish satisfies Publisher without performing I/O.
func (NoopPublisher) Publish(ctx context.Context, event Event) error {
	return nil
}

// New creates a version-one event envelope with a generated event ID.
func New(eventType, source, entityType, entityID string, payload any) Event {
	body, _ := json.Marshal(payload)
	return Event{
		EventID:       uuid.New(),
		SchemaVersion: 1,
		EventType:     eventType,
		Source:        source,
		EntityType:    entityType,
		EntityID:      entityID,
		OccurredAt:    time.Now().UTC(),
		Payload:       body,
	}
}
