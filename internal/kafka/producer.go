package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/bhanuteja/distributed-job-scheduler/internal/events"
	"github.com/segmentio/kafka-go"
)

// Producer publishes versioned lifecycle events to one Kafka topic.
type Producer struct {
	writer  *kafka.Writer
	brokers []string
}

// NewProducer creates a synchronous, all-replicas-acknowledged Kafka producer.
func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer:  &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, Balancer: &kafka.LeastBytes{}, BatchTimeout: 10 * time.Millisecond, AllowAutoTopicCreation: true, RequiredAcks: kafka.RequireAll},
		brokers: append([]string(nil), brokers...),
	}
}

// Publish writes an event keyed by entity ID to preserve per-job partition order.
func (p *Producer) Publish(ctx context.Context, event events.Event) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, kafka.Message{Key: []byte(event.EntityID), Value: body})
}

// Close flushes and closes the underlying Kafka writer.
func (p *Producer) Close() error {
	return p.writer.Close()
}

// Ping verifies that at least the first configured broker is reachable.
func (p *Producer) Ping(ctx context.Context) error {
	if len(p.brokers) == 0 {
		return errors.New("no Kafka brokers configured")
	}
	connection, err := kafka.DialContext(ctx, "tcp", p.brokers[0])
	if err != nil {
		return err
	}
	return connection.Close()
}
