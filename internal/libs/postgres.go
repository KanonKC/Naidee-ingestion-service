package libs

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// pool is the process-wide connection pool singleton, mirroring the prisma
// singleton in blaze-backend. Repositories receive it through their constructor
// so they stay unit-testable.
var pool *pgxpool.Pool

// InitPostgres opens the pool and verifies the connection. Note this service
// never migrates: the schema is owned by Core-Service.
func InitPostgres(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	cfg.MaxConnLifetime = time.Hour
	cfg.MaxConns = 10

	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := p.Ping(pingCtx); err != nil {
		p.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	pool = p
	return pool, nil
}

// Postgres returns the initialised pool.
func Postgres() *pgxpool.Pool {
	return pool
}

// ClosePostgres releases the pool on shutdown.
func ClosePostgres() {
	if pool != nil {
		pool.Close()
	}
}
