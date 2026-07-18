package lock

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisLock stores an opaque owner value under a TTL-bound Redis key.
type RedisLock struct {
	client *redis.Client
}

// NewRedisLock creates an owner-checked Redis lease manager.
func NewRedisLock(client *redis.Client) *RedisLock {
	return &RedisLock{client: client}
}

// Acquire creates the lease only when no current owner exists.
func (l *RedisLock) Acquire(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	return l.client.SetNX(ctx, key, owner, ttl).Result()
}

// Release atomically deletes a lease only when owner still matches.
func (l *RedisLock) Release(ctx context.Context, key, owner string) (bool, error) {
	// A Lua compare-and-delete avoids deleting a lease reacquired by another
	// worker after this owner's TTL expired.
	const script = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0`
	result, err := l.client.Eval(ctx, script, []string{key}, owner).Int()
	return result == 1, err
}

// Extend atomically renews a lease only for its current owner.
func (l *RedisLock) Extend(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	const script = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0`
	result, err := l.client.Eval(ctx, script, []string{key}, owner, ttl.Milliseconds()).Int()
	return result == 1, err
}
