package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Connect opens and verifies the PostgreSQL connection pool.
func Connect(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	return pgxpool.New(ctx, databaseURL)
}
