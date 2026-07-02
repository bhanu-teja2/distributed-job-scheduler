package worker

import (
	"context"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type Info struct {
	WorkerID      string    `json:"worker_id"`
	Status        string    `json:"status"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
}

type Registry interface {
	Heartbeat(ctx context.Context, workerID string, ttl time.Duration) error
	ActiveWorkers(ctx context.Context) ([]Info, error)
}

type RedisRegistry struct {
	client *redis.Client
}

func NewRedisRegistry(client *redis.Client) *RedisRegistry {
	return &RedisRegistry{client: client}
}

func (r *RedisRegistry) Heartbeat(ctx context.Context, workerID string, ttl time.Duration) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	pipe := r.client.TxPipeline()
	pipe.Set(ctx, heartbeatKey(workerID), now, ttl)
	pipe.SAdd(ctx, "workers:active", workerID)
	_, err := pipe.Exec(ctx)
	return err
}

func (r *RedisRegistry) ActiveWorkers(ctx context.Context) ([]Info, error) {
	ids, err := r.client.SMembers(ctx, "workers:active").Result()
	if err != nil {
		return nil, err
	}
	workers := make([]Info, 0, len(ids))
	for _, id := range ids {
		value, err := r.client.Get(ctx, heartbeatKey(id)).Result()
		if err == redis.Nil {
			_ = r.client.SRem(ctx, "workers:active", id).Err()
			continue
		}
		if err != nil {
			return nil, err
		}
		heartbeat, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			heartbeat = time.Time{}
		}
		workers = append(workers, Info{WorkerID: id, Status: "active", LastHeartbeat: heartbeat})
	}
	return workers, nil
}

func heartbeatKey(workerID string) string {
	return "worker:" + strings.TrimSpace(workerID) + ":heartbeat"
}

type NoopRegistry struct{}

func (NoopRegistry) Heartbeat(ctx context.Context, workerID string, ttl time.Duration) error {
	return nil
}

func (NoopRegistry) ActiveWorkers(ctx context.Context) ([]Info, error) {
	return nil, nil
}
