package auth

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/bhanuteja/distributed-job-scheduler/internal/response"
	"github.com/redis/go-redis/v9"
)

type RateLimiter struct {
	client   *redis.Client
	capacity int
}

func NewRateLimiter(client *redis.Client, perMinute int) *RateLimiter {
	if perMinute < 1 {
		perMinute = 120
	}
	return &RateLimiter{client: client, capacity: perMinute}
}
func (l *RateLimiter) Allow(ctx context.Context, clientID string) (bool, time.Duration, error) {
	const script = `
local capacity=tonumber(ARGV[1]); local rate=capacity/60; local now=tonumber(ARGV[2]);
local values=redis.call('HMGET',KEYS[1],'tokens','updated'); local tokens=tonumber(values[1]) or capacity; local updated=tonumber(values[2]) or now;
tokens=math.min(capacity,tokens+(now-updated)*rate); local allowed=0; if tokens>=1 then tokens=tokens-1; allowed=1 end;
redis.call('HMSET',KEYS[1],'tokens',tokens,'updated',now); redis.call('EXPIRE',KEYS[1],120); return {allowed,tokens}`
	result, err := l.client.Eval(ctx, script, []string{"rate:client:" + clientID}, l.capacity, float64(time.Now().UnixNano())/1e9).Slice()
	if err != nil {
		return false, 0, err
	}
	allowed := result[0].(int64) == 1
	if allowed {
		return true, 0, nil
	}
	return false, time.Second, error(nil)
}
func (l *RateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := FromContext(r.Context())
		if !ok {
			response.Error(w, r, http.StatusUnauthorized, "UNAUTHORIZED", "authentication required")
			return
		}
		allowed, retry, err := l.Allow(r.Context(), principal.ClientID.String())
		if err != nil {
			response.Error(w, r, http.StatusServiceUnavailable, "DEPENDENCY_UNAVAILABLE", "rate limiter unavailable")
			return
		}
		if !allowed {
			w.Header().Set("Retry-After", strconv.Itoa(int(retry.Seconds())))
			response.Error(w, r, http.StatusTooManyRequests, "RATE_LIMITED", "request rate exceeded")
			return
		}
		next.ServeHTTP(w, r)
	})
}
