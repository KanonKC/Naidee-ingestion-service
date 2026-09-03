package venue

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

// FindByNormalizedName looks up a venue by its normalized name. It returns nil
// when there is no match. This is the cache that keeps a venue seen in fifty
// posts from being geocoded fifty times.
func (r *Repository) FindByNormalizedName(ctx context.Context, nameNormalized string) (*Venue, error) {
	logger := r.logger.SetContext("repository.venue.findByNormalizedName", logging.SetContextOptions{Silent: true})

	const query = `
		SELECT id, name, name_normalized, name_th, address_text, lat, lng, geocoded_at
		FROM venues
		WHERE name_normalized = $1`

	var found Venue
	err := r.db.QueryRow(ctx, query, nameNormalized).Scan(
		&found.ID, &found.Name, &found.NameNormalized, &found.NameTH,
		&found.AddressText, &found.Lat, &found.Lng, &found.GeocodedAt,
	)
	if stderrors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		logger.Error(logging.Meta{Message: "Failed to find venue", Data: map[string]any{"name_normalized": nameNormalized}, Error: err})
		return nil, fmt.Errorf("find venue by normalized name: %w", err)
	}
	return &found, nil
}

// Create inserts a venue and returns its id.
//
// The ON CONFLICT clause is a no-op update that exists purely so the statement
// still RETURNs an id when the row was created in between our lookup and this
// insert. Without it the race would surface as a constraint violation and cost
// the post its venue for no good reason.
func (r *Repository) Create(ctx context.Context, request CreateVenue) (int64, error) {
	logger := r.logger.SetContext("repository.venue.create", logging.SetContextOptions{Silent: true})

	const query = `
		INSERT INTO venues (name, name_normalized, name_th, address_text, lat, lng, geocoded_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (name_normalized) DO UPDATE SET name = venues.name
		RETURNING id`

	var id int64
	err := r.db.QueryRow(ctx, query,
		request.Name,
		request.NameNormalized,
		request.NameTH,
		request.AddressText,
		request.Lat,
		request.Lng,
		request.GeocodedAt,
	).Scan(&id)
	if err != nil {
		logger.Error(logging.Meta{Message: "Failed to create venue", Data: map[string]any{"name": request.Name}, Error: err})
		return 0, fmt.Errorf("create venue: %w", err)
	}
	return id, nil
}
