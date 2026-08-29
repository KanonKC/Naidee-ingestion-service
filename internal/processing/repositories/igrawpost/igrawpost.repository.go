package igrawpost

import (
	"context"
	"fmt"

	"event/ingestion-service/internal/processing/logging"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	db     *pgxpool.Pool
	logger *logging.TLogger
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{db: db, logger: logging.New(logging.LayerRepository)}
}

// ListUnprocessed returns posts waiting for extraction, oldest first.
//
// Posts with no caption are skipped rather than sent: there is nothing for the
// model to read, so a request would burn tokens to be told is_event=false.
// They keep processed_at = NULL, which is honest — they were never processed.
func (r *Repository) ListUnprocessed(ctx context.Context, limit int) ([]UnprocessedPost, error) {
	logger := r.logger.SetContext("repository.igRawPost.listUnprocessed", logging.SetContextOptions{Silent: true})

	const query = `
		SELECT id, posted_at, caption
		FROM ig_raw_posts
		WHERE processed_at IS NULL
		  AND caption IS NOT NULL
		  AND caption <> ''
		ORDER BY posted_at
		LIMIT $1`

	rows, err := r.db.Query(ctx, query, limit)
	if err != nil {
		logger.Error(logging.Meta{Message: "Failed to list unprocessed posts", Error: err})
		return nil, fmt.Errorf("list unprocessed ig_raw_posts: %w", err)
	}
	defer rows.Close()

	posts := make([]UnprocessedPost, 0)
	for rows.Next() {
		var post UnprocessedPost
		if err := rows.Scan(&post.ID, &post.PostedAt, &post.Caption); err != nil {
			logger.Error(logging.Meta{Message: "Failed to scan unprocessed post row", Error: err})
			return nil, fmt.Errorf("scan ig_raw_posts row: %w", err)
		}
		posts = append(posts, post)
	}
	if err := rows.Err(); err != nil {
		logger.Error(logging.Meta{Message: "Failed while iterating unprocessed post rows", Error: err})
		return nil, fmt.Errorf("iterate ig_raw_posts: %w", err)
	}

	return posts, nil
}

// MarkProcessed stamps processed_at so the next run skips this post. It is only
// ever called after the event row has landed — a post marked processed whose
// event was never written would be silently lost.
func (r *Repository) MarkProcessed(ctx context.Context, id int64) error {
	logger := r.logger.SetContext("repository.igRawPost.markProcessed", logging.SetContextOptions{Silent: true})

	const query = `UPDATE ig_raw_posts SET processed_at = now() WHERE id = $1`

	if _, err := r.db.Exec(ctx, query, id); err != nil {
		logger.Error(logging.Meta{Message: "Failed to mark post processed", Data: map[string]any{"raw_post_id": id}, Error: err})
		return fmt.Errorf("mark ig_raw_post processed: %w", err)
	}
	return nil
}
