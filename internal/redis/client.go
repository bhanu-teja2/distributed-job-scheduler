package redis

import (
	"context"

	"github.com/redis/go-redis/v9"
)

func Connect(ctx context.Context, addr, password string, db int) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{Addr: addr, Password: password, DB: db})
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}
	return client, nil
}
