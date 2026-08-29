package igsource

import (
	"context"
	stderrors "errors"
	"fmt"

	"event/ingestion-service/internal/logging"

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

// ListActive returns every source due for a sync. NULLS FIRST puts sources that
// have never been synced at the front of the queue.
func (r *Repository) ListActive(ctx context.Context) ([]IgSource, error) {
	logger := r.logger.SetContext("repository.igSource.listActive", logging.SetContextOptions{Silent: true})

	const query = `
		SELECT id, username, display_name, is_active, last_synced_at, last_error,
		       consecutive_failures, created_at, updated_at
		FROM ig_sources
		WHERE is_active = true
		ORDER BY last_synced_at NULLS FIRST`

	rows, err := r.db.Query(ctx, query)
	if err != nil {
		logger.Error(logging.Meta{Message: "Failed to list active sources", Error: err})
		return nil, fmt.Errorf("list active ig_sources: %w", err)
	}
	defer rows.Close()

	sources := make([]IgSource, 0)
	for rows.Next() {
		var source IgSource
		if err := rows.Scan(
			&source.ID, &source.Username, &source.DisplayName, &source.IsActive,
			&source.LastSyncedAt, &source.LastError, &source.ConsecutiveFailures,
			&source.CreatedAt, &source.UpdatedAt,
		); err != nil {
			logger.Error(logging.Meta{Message: "Failed to scan source row", Error: err})
			return nil, fmt.Errorf("scan ig_sources row: %w", err)
		}
		sources = append(sources, source)
	}
	if err := rows.Err(); err != nil {
		logger.Error(logging.Meta{Message: "Failed while iterating source rows", Error: err})
		return nil, fmt.Errorf("iterate ig_sources: %w", err)
	}

	return sources, nil
}

// GetByUsername looks up a single source. Returns nil when it does not exist.
func (r *Repository) GetByUsername(ctx context.Context, username string) (*IgSource, error) {
	logger := r.logger.SetContext("repository.igSource.getByUsername", logging.SetContextOptions{Silent: true})

	const query = `
		SELECT id, username, display_name, is_active, last_synced_at, last_error,
		       consecutive_failures, created_at, updated_at
		FROM ig_sources
		WHERE username = $1`

	var source IgSource
	err := r.db.QueryRow(ctx, query, username).Scan(
		&source.ID, &source.Username, &source.DisplayName, &source.IsActive,
		&source.LastSyncedAt, &source.LastError, &source.ConsecutiveFailures,
		&source.CreatedAt, &source.UpdatedAt,
	)
	if stderrors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		logger.Error(logging.Meta{Message: "Failed to get source by username", Data: map[string]any{"username": username}, Error: err})
		return nil, fmt.Errorf("get ig_source by username: %w", err)
	}
	return &source, nil
}

// MarkSynced records a successful sync and clears the failure streak.
func (r *Repository) MarkSynced(ctx context.Context, id int64) error {
	logger := r.logger.SetContext("repository.igSource.markSynced", logging.SetContextOptions{Silent: true})

	const query = `
		UPDATE ig_sources
		SET last_synced_at = now(),
		    last_error = NULL,
		    consecutive_failures = 0,
		    updated_at = now()
		WHERE id = $1`

	if _, err := r.db.Exec(ctx, query, id); err != nil {
		logger.Error(logging.Meta{Message: "Failed to mark source synced", Data: map[string]any{"source_id": id}, Error: err})
		return fmt.Errorf("mark ig_source synced: %w", err)
	}
	return nil
}

// MarkFailed bumps consecutive_failures and returns the new value so the caller
// can decide whether the auto-deactivate threshold has been crossed.
func (r *Repository) MarkFailed(ctx context.Context, request MarkFailedIgSource) (int, error) {
	logger := r.logger.SetContext("repository.igSource.markFailed", logging.SetContextOptions{Silent: true})

	const query = `
		UPDATE ig_sources
		SET consecutive_failures = consecutive_failures + 1,
		    last_error = $2,
		    updated_at = now()
		WHERE id = $1
		RETURNING consecutive_failures`

	var consecutiveFailures int
	if err := r.db.QueryRow(ctx, query, request.ID, request.LastError).Scan(&consecutiveFailures); err != nil {
		logger.Error(logging.Meta{Message: "Failed to mark source failed", Data: map[string]any{"source_id": request.ID}, Error: err})
		return 0, fmt.Errorf("mark ig_source failed: %w", err)
	}
	return consecutiveFailures, nil
}

// Deactivate takes a source out of rotation so it stops burning rate-limit quota.
func (r *Repository) Deactivate(ctx context.Context, request DeactivateIgSource) error {
	logger := r.logger.SetContext("repository.igSource.deactivate", logging.SetContextOptions{Silent: true})

	const query = `
		UPDATE ig_sources
		SET is_active = false,
		    last_error = $2,
		    updated_at = now()
		WHERE id = $1`

	if _, err := r.db.Exec(ctx, query, request.ID, request.Reason); err != nil {
		logger.Error(logging.Meta{Message: "Failed to deactivate source", Data: map[string]any{"source_id": request.ID}, Error: err})
		return fmt.Errorf("deactivate ig_source: %w", err)
	}
	return nil
}
