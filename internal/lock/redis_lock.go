package lock

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisLock struct {
	client *redis.Client
}

func NewRedisLock(client *redis.Client) *RedisLock {
	return &RedisLock{client: client}
}

func (l *RedisLock) Acquire(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	return l.client.SetNX(ctx, key, owner, ttl).Result()
}

func (l *RedisLock) Release(ctx context.Context, key, owner string) (bool, error) {
	const script = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0`
	result, err := l.client.Eval(ctx, script, []string{key}, owner).Int()
	return result == 1, err
}
