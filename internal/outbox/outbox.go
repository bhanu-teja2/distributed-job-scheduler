package outbox

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/bhanuteja/distributed-job-scheduler/internal/events"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

var publishedTotal = promauto.NewCounter(prometheus.CounterOpts{Name: "outbox_events_published_total", Help: "Durable events published to Kafka."})
var publishFailures = promauto.NewCounter(prometheus.CounterOpts{Name: "outbox_publish_failures_total", Help: "Kafka publication attempts that failed."})
var publishLatency = promauto.NewHistogram(prometheus.HistogramOpts{Name: "outbox_publish_latency_seconds", Help: "Time from event creation to Kafka publication."})

type Store struct{ pool *pgxpool.Pool }

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

func (s *Store) Claim(ctx context.Context, relayID string, batch int, lease time.Duration) ([]events.Event, error) {
	if batch < 1 {
		batch = 50
	}
	rows, err := s.pool.Query(ctx, `WITH pending AS (
  SELECT id FROM job_events
  WHERE published_at IS NULL AND next_publish_at <= now() AND (claimed_until IS NULL OR claimed_until < now())
  ORDER BY created_at ASC, id ASC LIMIT $1 FOR UPDATE SKIP LOCKED
)
UPDATE job_events e SET claimed_by=$2, claimed_until=now()+$3*interval '1 second'
FROM pending p WHERE e.id=p.id
RETURNING e.id,e.schema_version,e.tenant_id,e.job_id,e.event_type,e.source,e.created_at,coalesce(e.correlation_id,''),e.causation_id,e.payload,e.publish_attempts`, batch, relayID, int(lease.Seconds()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []events.Event
	for rows.Next() {
		var event events.Event
		var jobID uuid.UUID
		if err := rows.Scan(&event.EventID, &event.SchemaVersion, &event.TenantID, &jobID, &event.EventType, &event.Source, &event.OccurredAt, &event.CorrelationID, &event.CausationID, &event.Payload, &event.PublishAttempts); err != nil {
			return nil, err
		}
		event.EntityID = jobID.String()
		event.EntityType = "job"
		items = append(items, event)
	}
	return items, rows.Err()
}

func (s *Store) MarkPublished(ctx context.Context, id uuid.UUID, relayID string) error {
	tag, err := s.pool.Exec(ctx, `UPDATE job_events SET published_at=now(),publish_attempts=publish_attempts+1,last_publish_error=NULL,claimed_by=NULL,claimed_until=NULL WHERE id=$1 AND claimed_by=$2`, id, relayID)
	if err == nil && tag.RowsAffected() == 0 {
		return errors.New("outbox lease lost")
	}
	return err
}

func (s *Store) MarkFailed(ctx context.Context, id uuid.UUID, relayID string, attempts int, message string) error {
	delay := time.Duration(math.Min(300, math.Pow(2, float64(attempts)))) * time.Second
	_, err := s.pool.Exec(ctx, `UPDATE job_events SET publish_attempts=publish_attempts+1,last_publish_error=$3,next_publish_at=now()+$4*interval '1 second',claimed_by=NULL,claimed_until=NULL WHERE id=$1 AND claimed_by=$2`, id, relayID, message, int(delay.Seconds()))
	return err
}

func (s *Store) Replay(ctx context.Context, tenantID, id uuid.UUID) error {
	tag, err := s.pool.Exec(ctx, `UPDATE job_events SET published_at=NULL,next_publish_at=now(),last_publish_error=NULL,claimed_by=NULL,claimed_until=NULL WHERE tenant_id=$1 AND id=$2`, tenantID, id)
	if err == nil && tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return err
}

func (s *Store) Backlog(ctx context.Context, tenantID *uuid.UUID) (int64, error) {
	var count int64
	if tenantID == nil {
		return count, s.pool.QueryRow(ctx, `SELECT count(*) FROM job_events WHERE published_at IS NULL`).Scan(&count)
	}
	return count, s.pool.QueryRow(ctx, `SELECT count(*) FROM job_events WHERE tenant_id=$1 AND published_at IS NULL`, *tenantID).Scan(&count)
}

type Relay struct {
	store     *Store
	publisher events.Publisher
	log       *zap.Logger
	id        string
	batch     int
	interval  time.Duration
	lease     time.Duration
}

func NewRelay(store *Store, publisher events.Publisher, log *zap.Logger, batch int, interval time.Duration) *Relay {
	return &Relay{store: store, publisher: publisher, log: log, id: "relay-" + uuid.NewString(), batch: batch, interval: interval, lease: 30 * time.Second}
}

func (r *Relay) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		if err := r.publishBatch(ctx); err != nil && !errors.Is(err, context.Canceled) {
			r.log.Error("outbox batch failed", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (r *Relay) publishBatch(ctx context.Context) error {
	items, err := r.store.Claim(ctx, r.id, r.batch, r.lease)
	if err != nil {
		return err
	}
	for _, event := range items {
		if err := r.publisher.Publish(ctx, event); err != nil {
			publishFailures.Inc()
			_ = r.store.MarkFailed(ctx, event.EventID, r.id, event.PublishAttempts, truncate(err.Error(), 1000))
			continue
		}
		if err := r.store.MarkPublished(ctx, event.EventID, r.id); err != nil {
			return fmt.Errorf("mark event published: %w", err)
		}
		publishedTotal.Inc()
		publishLatency.Observe(time.Since(event.OccurredAt).Seconds())
	}
	return nil
}

func truncate(value string, limit int) string {
	if len(value) > limit {
		return value[:limit]
	}
	return value
}
