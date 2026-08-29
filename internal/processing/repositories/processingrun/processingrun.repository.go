package processingrun

import (
	"context"
	stderrors "errors"
	"fmt"

	"event/ingestion-service/internal/processing/logging"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	db     *pgxpool.Pool
	logger *logging.TLogger
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{db: db, logger: logging.New(logging.LayerRepository)}
}

// Create opens a run row up front so a crashed process still leaves a record
// behind to investigate, and so the manual trigger has a real run_id to hand
// back before the batch has even been submitted.
func (r *Repository) Create(ctx context.Context, status string) (int64, error) {
	logger := r.logger.SetContext("repository.processingRun.create", logging.SetContextOptions{Silent: true})

	const query = `INSERT INTO processing_runs (status) VALUES ($1) RETURNING id`

	var id int64
	if err := r.db.QueryRow(ctx, query, status).Scan(&id); err != nil {
		logger.Error(logging.Meta{Message: "Failed to create processing run", Error: err})
		return 0, fmt.Errorf("create processing_run: %w", err)
	}
	return id, nil
}

// SetBatch records the Anthropic batch id and how many posts went into it.
// Written the moment the batch is accepted: if the poll later times out, this
// is the only thing that makes the batch findable by hand within its 29 days.
func (r *Repository) SetBatch(ctx context.Context, id int64, batchID string, postsSubmitted int) error {
	logger := r.logger.SetContext("repository.processingRun.setBatch", logging.SetContextOptions{Silent: true})

	const query = `
		UPDATE processing_runs
		SET batch_id = $2, posts_submitted = $3, status = $4
		WHERE id = $1`

	if _, err := r.db.Exec(ctx, query, id, batchID, postsSubmitted, StatusPolling); err != nil {
		logger.Error(logging.Meta{Message: "Failed to record batch id", Data: map[string]any{"run_id": id, "batch_id": batchID}, Error: err})
		return fmt.Errorf("set processing_run batch: %w", err)
	}
	return nil
}

// Finish stamps the terminal status and the counters for the run.
func (r *Repository) Finish(ctx context.Context, request FinishProcessingRun) error {
	logger := r.logger.SetContext("repository.processingRun.finish", logging.SetContextOptions{Silent: true})

	const query = `
		UPDATE processing_runs
		SET status = $2,
		    posts_submitted = $3,
		    posts_succeeded = $4,
		    posts_failed = $5,
		    events_geocoded = $6,
		    error = $7,
		    finished_at = now()
		WHERE id = $1`

	_, err := r.db.Exec(ctx, query,
		request.ID,
		request.Status,
		request.PostsSubmitted,
		request.PostsSucceeded,
		request.PostsFailed,
		request.EventsGeocoded,
		request.Error,
	)
	if err != nil {
		logger.Error(logging.Meta{Message: "Failed to finish processing run", Data: map[string]any{"run_id": request.ID}, Error: err})
		return fmt.Errorf("finish processing_run: %w", err)
	}
	return nil
}

// Get returns one run, or nil when it does not exist. This is what backs
// GET /admin/runs/{id}: the caller polls our table, never Anthropic directly.
func (r *Repository) Get(ctx context.Context, id int64) (*ProcessingRun, error) {
	logger := r.logger.SetContext("repository.processingRun.get", logging.SetContextOptions{Silent: true})

	const query = `
		SELECT id, batch_id, status, posts_submitted, posts_succeeded,
		       posts_failed, events_geocoded, started_at, finished_at, error
		FROM processing_runs
		WHERE id = $1`

	var run ProcessingRun
	err := r.db.QueryRow(ctx, query, id).Scan(
		&run.ID, &run.BatchID, &run.Status, &run.PostsSubmitted, &run.PostsSucceeded,
		&run.PostsFailed, &run.EventsGeocoded, &run.StartedAt, &run.FinishedAt, &run.Error,
	)
	if stderrors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		logger.Error(logging.Meta{Message: "Failed to get processing run", Data: map[string]any{"run_id": id}, Error: err})
		return nil, fmt.Errorf("get processing_run: %w", err)
	}
	return &run, nil
}
