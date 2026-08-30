package igrawpost

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

// Upsert writes one post idempotently and reports whether the row was newly
// inserted.
//
// Captions, media urls and the raw payload are refreshed on every run because
// people edit captions after posting — fixing an event date, adding a signup
// link. Keeping the first version would leave stale data behind.
//
// When the caption actually changed, processed_at is cleared so processing
// reprocesses the post on its next run and updates the existing events row. The
// comparison is on caption_hash rather than the caption itself, and it must be
// a real change: an unchanged caption re-seen on every six-hourly run would
// otherwise queue an endless stream of pointless extractions.
//
// `xmax = 0` is the Postgres trick that distinguishes an INSERT from an UPDATE
// in an upsert, so posts_new / posts_updated need no extra query.
func (r *Repository) Upsert(ctx context.Context, request UpsertIgRawPost) (bool, error) {
	logger := r.logger.SetContext("repository.igRawPost.upsert", logging.SetContextOptions{Silent: true})

	const query = `
		INSERT INTO ig_raw_posts (
		    source_id, ig_media_id, permalink, caption, media_type,
		    media_url, thumbnail_url, posted_at, raw_payload, caption_hash
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (ig_media_id) DO UPDATE SET
		    caption       = EXCLUDED.caption,
		    media_url     = EXCLUDED.media_url,
		    thumbnail_url = EXCLUDED.thumbnail_url,
		    raw_payload   = EXCLUDED.raw_payload,
		    caption_hash  = EXCLUDED.caption_hash,
		    processed_at  = CASE
		        WHEN ig_raw_posts.caption_hash IS DISTINCT FROM EXCLUDED.caption_hash THEN NULL
		        ELSE ig_raw_posts.processed_at
		    END,
		    last_seen_at  = now()
		RETURNING (xmax = 0) AS inserted`

	var inserted bool
	err := r.db.QueryRow(ctx, query,
		request.SourceID,
		request.IgMediaID,
		request.Permalink,
		request.Caption,
		request.MediaType,
		request.MediaURL,
		request.ThumbnailURL,
		request.PostedAt,
		[]byte(request.RawPayload),
		request.CaptionHash(),
	).Scan(&inserted)
	if err != nil {
		logger.Error(logging.Meta{
			Message: "Failed to upsert raw post",
			Data:    map[string]any{"source_id": request.SourceID, "ig_media_id": request.IgMediaID},
			Error:   err,
		})
		return false, fmt.Errorf("upsert ig_raw_post: %w", err)
	}
	return inserted, nil
}

// ListUnprocessed returns posts waiting for extraction, oldest first. Called by
// processing.
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
