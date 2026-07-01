package kafka

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Event struct {
	EventID    uuid.UUID       `json:"event_id"`
	EventType  string          `json:"event_type"`
	Source     string          `json:"source"`
	EntityType string          `json:"entity_type"`
	EntityID   string          `json:"entity_id"`
	Timestamp  time.Time       `json:"timestamp"`
	Payload    json.RawMessage `json:"payload"`
}

func NewEvent(eventType, source, entityType, entityID string, payload json.RawMessage) Event {
	return Event{EventID: uuid.New(), EventType: eventType, Source: source, EntityType: entityType, EntityID: entityID, Timestamp: time.Now().UTC(), Payload: payload}
}
