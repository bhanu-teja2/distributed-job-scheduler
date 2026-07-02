package events

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

const (
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

type Publisher interface {
	Publish(ctx context.Context, event Event) error
}

type Event struct {
	EventID    uuid.UUID       `json:"event_id"`
	EventType  string          `json:"event_type"`
	Source     string          `json:"source"`
	EntityType string          `json:"entity_type"`
	EntityID   string          `json:"entity_id"`
	Timestamp  time.Time       `json:"timestamp"`
	Payload    json.RawMessage `json:"payload"`
}

type NoopPublisher struct{}

func (NoopPublisher) Publish(ctx context.Context, event Event) error {
	return nil
}

func New(eventType, source, entityType, entityID string, payload any) Event {
	body, _ := json.Marshal(payload)
	return Event{
		EventID:    uuid.New(),
		EventType:  eventType,
		Source:     source,
		EntityType: entityType,
		EntityID:   entityID,
		Timestamp:  time.Now().UTC(),
		Payload:    body,
	}
}
