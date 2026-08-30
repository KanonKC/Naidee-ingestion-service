package event

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

// Upsert writes one extracted event, keyed on raw_post_id.
//
// UNIQUE (raw_post_id) is what makes reprocessing an edited caption an UPDATE
// rather than a duplicate row.
//
// Two things are deliberately left alone on conflict:
//
//   - review_status, so a re-run cannot undo an approve/reject an admin already
//     made by hand. (Whether an edited caption *should* reset that decision is
//     an open question — see §13 of the spec.)
//   - venue_id, because venue resolution is a separate step that runs after
//     this one and would otherwise be wiped on every reprocess.
func (r *Repository) Upsert(ctx context.Context, request UpsertEvent) error {
	logger := r.logger.SetContext("repository.event.upsert", logging.SetContextOptions{Silent: true})

	// A non-event is never auto-published however confident the model was:
	// review_status is what the read side filters on, and "confidently not an
	// event" must not read as "publish it".
	const query = `
		INSERT INTO events (
		    raw_post_id, title, start_at, end_at, price_text,
		    category, registration_url, is_event, confidence, review_status
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,
		    CASE WHEN $8 AND $9 = 'high' THEN 'auto_published' ELSE 'pending' END
		)
		ON CONFLICT (raw_post_id) DO UPDATE SET
		    title            = EXCLUDED.title,
		    start_at         = EXCLUDED.start_at,
		    end_at           = EXCLUDED.end_at,
		    price_text       = EXCLUDED.price_text,
		    category         = EXCLUDED.category,
		    registration_url = EXCLUDED.registration_url,
		    is_event         = EXCLUDED.is_event,
		    confidence       = EXCLUDED.confidence,
		    extracted_at     = now()`

	_, err := r.db.Exec(ctx, query,
		request.RawPostID,
		request.Title,
		request.StartAt,
		request.EndAt,
		request.PriceText,
		request.Category,
		request.RegistrationURL,
		request.IsEvent,
		request.Confidence,
	)
	if err != nil {
		logger.Error(logging.Meta{Message: "Failed to upsert event", Data: map[string]any{"raw_post_id": request.RawPostID}, Error: err})
		return fmt.Errorf("upsert event: %w", err)
	}
	return nil
}

// SetVenue attaches a resolved venue to an already-written event.
func (r *Repository) SetVenue(ctx context.Context, rawPostID int64, venueID int64) error {
	logger := r.logger.SetContext("repository.event.setVenue", logging.SetContextOptions{Silent: true})

	const query = `UPDATE events SET venue_id = $2 WHERE raw_post_id = $1`

	if _, err := r.db.Exec(ctx, query, rawPostID, venueID); err != nil {
		logger.Error(logging.Meta{
			Message: "Failed to attach venue to event",
			Data:    map[string]any{"raw_post_id": rawPostID, "venue_id": venueID},
			Error:   err,
		})
		return fmt.Errorf("set event venue: %w", err)
	}
	return nil
}
