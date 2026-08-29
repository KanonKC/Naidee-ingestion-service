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
// When the caption actually changed, processed_at is cleared so
// Processing-Service reprocesses the post on its next run and updates the
// existing events row. The comparison is on caption_hash rather than the
// caption itself, and it must be a real change: an unchanged caption re-seen on
// every six-hourly run would otherwise queue an endless stream of pointless
// extractions.
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
