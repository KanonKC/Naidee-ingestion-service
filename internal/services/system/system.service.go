package system

import (
	"context"
	"time"

	"event/ingestion-service/internal/logging"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Health is the shape returned by the health endpoint.
type Health struct {
	Database bool `json:"database"`
}

type Service struct {
	db     *pgxpool.Pool
	logger *logging.TLogger
}

func NewService(db *pgxpool.Pool) *Service {
	return &Service{db: db, logger: logging.New(logging.LayerService)}
}

// GetHealth reports whether the database is reachable. That is the only
// dependency a container health check needs to know about.
func (s *Service) GetHealth(ctx context.Context) Health {
	logger := s.logger.SetContext("service.system.getHealth", logging.SetContextOptions{Silent: true})

	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if err := s.db.Ping(pingCtx); err != nil {
		logger.Error(logging.Meta{Message: "Database check failed", Error: err})
		return Health{Database: false}
	}
	return Health{Database: true}
}
