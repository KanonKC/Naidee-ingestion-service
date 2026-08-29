package ingestionrun

import (
	"context"
	"fmt"

	"event/ingestion-service/internal/logging"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	db     *pgxpool.Pool
	logger *logging.TLogger
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{db: db, logger: logging.New(logging.LayerRepository)}
}

// Create opens a run row up front so a crashed process still leaves a
// `running` record behind to investigate.
func (r *Repository) Create(ctx context.Context, sourceKind string) (int64, error) {
	logger := r.logger.SetContext("repository.ingestionRun.create", logging.SetContextOptions{Silent: true})

	const query = `
		INSERT INTO ingestion_runs (source_kind, status)
		VALUES ($1, 'running')
		RETURNING id`

	var id int64
	if err := r.db.QueryRow(ctx, query, sourceKind).Scan(&id); err != nil {
		logger.Error(logging.Meta{Message: "Failed to create ingestion run", Data: map[string]any{"source_kind": sourceKind}, Error: err})
		return 0, fmt.Errorf("create ingestion_run: %w", err)
	}
	return id, nil
}

// Finish stamps the terminal status and the counters for the run.
func (r *Repository) Finish(ctx context.Context, request FinishIngestionRun) error {
	logger := r.logger.SetContext("repository.ingestionRun.finish", logging.SetContextOptions{Silent: true})

	const query = `
		UPDATE ingestion_runs
		SET status = $2,
		    sources_total = $3,
		    sources_ok = $4,
		    sources_failed = $5,
		    posts_new = $6,
		    posts_updated = $7,
		    error = $8,
		    finished_at = now()
		WHERE id = $1`

	_, err := r.db.Exec(ctx, query,
		request.ID,
		request.Status,
		request.SourcesTotal,
		request.SourcesOK,
		request.SourcesFailed,
		request.PostsNew,
		request.PostsUpdated,
		request.Error,
	)
	if err != nil {
		logger.Error(logging.Meta{Message: "Failed to finish ingestion run", Data: map[string]any{"run_id": request.ID}, Error: err})
		return fmt.Errorf("finish ingestion_run: %w", err)
	}
	return nil
}
