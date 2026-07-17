//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bhanuteja/distributed-job-scheduler/internal/auth"
	appErrors "github.com/bhanuteja/distributed-job-scheduler/internal/errors"
	"github.com/bhanuteja/distributed-job-scheduler/internal/job"
	appKafka "github.com/bhanuteja/distributed-job-scheduler/internal/kafka"
	"github.com/bhanuteja/distributed-job-scheduler/internal/lock"
	"github.com/bhanuteja/distributed-job-scheduler/internal/outbox"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	redisclient "github.com/redis/go-redis/v9"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"go.uber.org/zap"
)

func TestPostgresClaimingAndTransactionalEvents(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := t.Context()
	container, err := tcpostgres.Run(ctx, "postgres:16.4-alpine", tcpostgres.WithDatabase("scheduler"), tcpostgres.WithUsername("scheduler"), tcpostgres.WithPassword("scheduler"), tcpostgres.BasicWaitStrategies())
	if err != nil {
		t.Fatal(err)
	}
	testcontainers.CleanupContainer(t, container)
	connection, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, connection)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	applyMigrations(t, ctx, pool)
	repo := job.NewPostgresRepository(pool)
	now := time.Now().UTC().Add(-time.Second)
	for i := 0; i < 10; i++ {
		hash := uuid.NewString()
		_, err = repo.Create(ctx, job.Job{ID: uuid.New(), TenantID: auth.DefaultTenantID, Name: "integration", JobType: "CALL_WEBHOOK", Payload: json.RawMessage(`{"url":"https://example.com"}`), Status: job.StatusPending, Priority: i % 3, RunAt: now, MaxRetries: 3, RetryBackoffSeconds: 1, TimeoutSeconds: 5, RequestHash: &hash})
		if err != nil {
			t.Fatal(err)
		}
	}
	first, err := repo.ClaimDueJobs(ctx, "worker-a", 5, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	second, err := repo.ClaimDueJobs(ctx, "worker-b", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 5 || len(second) != 5 {
		t.Fatalf("expected split claims 5/5, got %d/%d", len(first), len(second))
	}
	seen := map[uuid.UUID]bool{}
	for _, item := range append(first, second...) {
		if seen[item.ID] {
			t.Fatalf("job %s claimed twice", item.ID)
		}
		seen[item.ID] = true
		if item.ActiveAttemptID == uuid.Nil {
			t.Fatal("claim did not create attempt")
		}
	}
	if err = repo.CompleteExecution(ctx, first[0], "worker-a", 50*time.Millisecond, json.RawMessage(`{"status_code":204}`)); err != nil {
		t.Fatal(err)
	}
	events, err := repo.ListEvents(ctx, auth.DefaultTenantID, first[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	var types []string
	for _, event := range events {
		types = append(types, event.EventType)
	}
	if !contains(types, "job.created") || !contains(types, "job.started") || !contains(types, "job.completed") {
		t.Fatalf("missing transactional events: %v", types)
	}

	idempotencyKey := "integration-idempotency-key"
	requestHash := "same-request"
	original, err := repo.Create(ctx, job.Job{ID: uuid.New(), TenantID: auth.DefaultTenantID, Name: "idempotent", JobType: "CALL_WEBHOOK", Payload: json.RawMessage(`{"url":"https://example.com"}`), Status: job.StatusPending, Priority: 5, RunAt: now, MaxRetries: 1, RetryBackoffSeconds: 1, TimeoutSeconds: 5, IdempotencyKey: &idempotencyKey, RequestHash: &requestHash})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := repo.Create(ctx, job.Job{ID: uuid.New(), TenantID: auth.DefaultTenantID, Name: "idempotent", JobType: "CALL_WEBHOOK", Payload: json.RawMessage(`{"url":"https://example.com"}`), Status: job.StatusPending, Priority: 5, RunAt: now, MaxRetries: 1, RetryBackoffSeconds: 1, TimeoutSeconds: 5, IdempotencyKey: &idempotencyKey, RequestHash: &requestHash})
	if err != nil || replayed.ID != original.ID {
		t.Fatalf("idempotency replay: original=%s replayed=%s err=%v", original.ID, replayed.ID, err)
	}
	conflictingHash := "different-request"
	_, err = repo.Create(ctx, job.Job{ID: uuid.New(), TenantID: auth.DefaultTenantID, Name: "conflict", JobType: "CALL_WEBHOOK", Payload: json.RawMessage(`{"url":"https://example.com"}`), Status: job.StatusPending, Priority: 5, RunAt: now, MaxRetries: 1, RetryBackoffSeconds: 1, TimeoutSeconds: 5, IdempotencyKey: &idempotencyKey, RequestHash: &conflictingHash})
	if !errors.Is(err, appErrors.ErrIdempotency) {
		t.Fatalf("expected idempotency conflict, got %v", err)
	}

	otherTenant := uuid.New()
	if _, err = pool.Exec(ctx, `INSERT INTO tenants(id,slug,name) VALUES($1,$2,$3)`, otherTenant, "other", "Other Tenant"); err != nil {
		t.Fatal(err)
	}
	otherHash := uuid.NewString()
	if _, err = repo.Create(ctx, job.Job{ID: uuid.New(), TenantID: otherTenant, Name: "hidden", JobType: "CALL_WEBHOOK", Payload: json.RawMessage(`{"url":"https://example.com"}`), Status: job.StatusPending, Priority: 5, RunAt: now, MaxRetries: 1, RetryBackoffSeconds: 1, TimeoutSeconds: 5, RequestHash: &otherHash}); err != nil {
		t.Fatal(err)
	}
	page, err := repo.List(ctx, auth.DefaultTenantID, job.ListFilter{Page: 1, PageSize: 100, Sort: "created_at", Order: "ASC"})
	if err != nil {
		t.Fatal(err)
	}
	for _, listed := range page.Items {
		if listed.TenantID != auth.DefaultTenantID {
			t.Fatalf("cross-tenant job leaked into list: %s", listed.ID)
		}
	}
}

func TestRedisOwnerCheckedLease(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := t.Context()
	container, err := tcredis.Run(ctx, "redis:7.2.5-alpine")
	if err != nil {
		t.Fatal(err)
	}
	testcontainers.CleanupContainer(t, container)
	connection, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	options, err := redisclient.ParseURL(connection)
	if err != nil {
		t.Fatal(err)
	}
	client := redisclient.NewClient(options)
	defer client.Close()
	manager := lock.NewRedisLock(client)
	ok, err := manager.Acquire(ctx, "lock:job:test", "worker-a", time.Minute)
	if err != nil || !ok {
		t.Fatalf("acquire: ok=%v err=%v", ok, err)
	}
	if released, err := manager.Release(ctx, "lock:job:test", "worker-b"); err != nil || released {
		t.Fatalf("non-owner release: released=%v err=%v", released, err)
	}
	if extended, err := manager.Extend(ctx, "lock:job:test", "worker-a", 2*time.Minute); err != nil || !extended {
		t.Fatalf("owner extend: extended=%v err=%v", extended, err)
	}
	if released, err := manager.Release(ctx, "lock:job:test", "worker-a"); err != nil || !released {
		t.Fatalf("owner release: released=%v err=%v", released, err)
	}
	limiter := auth.NewRateLimiter(client, 1)
	if allowed, _, err := limiter.Allow(ctx, "client-a"); err != nil || !allowed {
		t.Fatalf("first rate-limited request: allowed=%v err=%v", allowed, err)
	}
	if allowed, _, err := limiter.Allow(ctx, "client-a"); err != nil || allowed {
		t.Fatalf("second rate-limited request: allowed=%v err=%v", allowed, err)
	}
}

func TestMigrationUpDownUp(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := t.Context()
	container, err := tcpostgres.Run(ctx, "postgres:16.4-alpine", tcpostgres.WithDatabase("scheduler"), tcpostgres.WithUsername("scheduler"), tcpostgres.WithPassword("scheduler"), tcpostgres.BasicWaitStrategies())
	if err != nil {
		t.Fatal(err)
	}
	testcontainers.CleanupContainer(t, container)
	connection, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, connection)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	applyMigrations(t, ctx, pool)
	applyDownMigrations(t, ctx, pool)
	applyMigrations(t, ctx, pool)
	var count int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM tenants WHERE slug='default'`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("migration round trip failed: count=%d err=%v", count, err)
	}
}

func TestOutboxPublishesDurableEventToKafka(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := t.Context()
	database, err := tcpostgres.Run(ctx, "postgres:16.4-alpine", tcpostgres.WithDatabase("scheduler"), tcpostgres.WithUsername("scheduler"), tcpostgres.WithPassword("scheduler"), tcpostgres.BasicWaitStrategies())
	if err != nil {
		t.Fatal(err)
	}
	testcontainers.CleanupContainer(t, database)
	connection, err := database.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, connection)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	applyMigrations(t, ctx, pool)
	brokerContainer, err := redpanda.Run(ctx, "docker.redpanda.com/redpandadata/redpanda:v25.2.4")
	if err != nil {
		t.Fatal(err)
	}
	testcontainers.CleanupContainer(t, brokerContainer)
	broker, err := brokerContainer.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatal(err)
	}
	createTopic(t, ctx, broker, "scheduler.events")
	hash := uuid.NewString()
	created, err := job.NewPostgresRepository(pool).Create(ctx, job.Job{ID: uuid.New(), TenantID: auth.DefaultTenantID, Name: "outbox", JobType: "CALL_WEBHOOK", Payload: json.RawMessage(`{"url":"https://example.com"}`), Status: job.StatusPending, Priority: 5, RunAt: time.Now().UTC(), MaxRetries: 1, RetryBackoffSeconds: 1, TimeoutSeconds: 5, RequestHash: &hash})
	if err != nil {
		t.Fatal(err)
	}
	publisher := appKafka.NewProducer([]string{broker}, "scheduler.events")
	defer publisher.Close()
	relay := outbox.NewRelay(outbox.NewStore(pool), publisher, zap.NewNop(), 10, 50*time.Millisecond)
	relayCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = relay.Run(relayCtx) }()
	deadline := time.Now().Add(15 * time.Second)
	for {
		var published *time.Time
		var publishError *string
		err = pool.QueryRow(ctx, `SELECT published_at,last_publish_error FROM job_events WHERE job_id=$1 AND event_type='job.created'`, created.ID).Scan(&published, &publishError)
		if err != nil {
			t.Fatal(err)
		}
		if published != nil {
			break
		}
		if time.Now().After(deadline) {
			lastError := ""
			if publishError != nil {
				lastError = *publishError
			}
			t.Fatalf("event was not published; last error=%s", lastError)
		}
		time.Sleep(100 * time.Millisecond)
	}
	reader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: []string{broker}, Topic: "scheduler.events", Partition: 0, StartOffset: kafkago.FirstOffset, MinBytes: 1, MaxBytes: 1 << 20, MaxWait: 100 * time.Millisecond})
	defer reader.Close()
	readCtx, readCancel := context.WithTimeout(ctx, 20*time.Second)
	defer readCancel()
	message, err := reader.ReadMessage(readCtx)
	if err != nil {
		t.Fatal(err)
	}
	if string(message.Key) != created.ID.String() {
		t.Fatalf("expected Kafka key %s, got %s", created.ID, message.Key)
	}
	var envelope map[string]any
	if err = json.Unmarshal(message.Value, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope["event_type"] != "job.created" {
		t.Fatalf("unexpected event: %s", message.Value)
	}
}

func applyMigrations(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
	entries, err := os.ReadDir(filepath.Join(root, "migrations"))
	if err != nil {
		t.Fatal(err)
	}
	var files []string
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".up.sql") {
			files = append(files, entry.Name())
		}
	}
	sort.Strings(files)
	for _, name := range files {
		body, err := os.ReadFile(filepath.Join(root, "migrations", name))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = pool.Exec(ctx, string(body)); err != nil {
			t.Fatalf("apply %s: %v", name, err)
		}
	}
}

func applyDownMigrations(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
	entries, err := os.ReadDir(filepath.Join(root, "migrations"))
	if err != nil {
		t.Fatal(err)
	}
	var files []string
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".down.sql") {
			files = append(files, entry.Name())
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(files)))
	for _, name := range files {
		body, err := os.ReadFile(filepath.Join(root, "migrations", name))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = pool.Exec(ctx, string(body)); err != nil {
			t.Fatalf("apply %s: %v", name, err)
		}
	}
}
func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func createTopic(t *testing.T, ctx context.Context, broker, topic string) {
	t.Helper()
	seed, err := kafkago.DialContext(ctx, "tcp", broker)
	if err != nil {
		t.Fatal(err)
	}
	controller, err := seed.Controller()
	_ = seed.Close()
	if err != nil {
		t.Fatal(err)
	}
	connection, err := kafkago.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if err = connection.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
}
