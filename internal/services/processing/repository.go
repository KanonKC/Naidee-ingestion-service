package processing

import (
	"context"

	"event/ingestion-service/internal/repositories/event"
	"event/ingestion-service/internal/repositories/igrawpost"
	"event/ingestion-service/internal/repositories/processingrun"
	"event/ingestion-service/internal/repositories/venue"
)

// The repository layer is made of concrete structs, as in blaze-backend. These
// interfaces are declared here, on the consumer side, because Go convention is
// to accept interfaces — it keeps the orchestration logic testable without a
// database. The real repositories satisfy them implicitly.

type IgRawPostRepository interface {
	ListUnprocessed(ctx context.Context, limit int) ([]igrawpost.UnprocessedPost, error)
	MarkProcessed(ctx context.Context, id int64) error
}

type EventRepository interface {
	Upsert(ctx context.Context, request event.UpsertEvent) error
	SetVenue(ctx context.Context, rawPostID int64, venueID int64) error
}

type VenueRepository interface {
	FindByNormalizedName(ctx context.Context, nameNormalized string) (*venue.Venue, error)
	Create(ctx context.Context, request venue.CreateVenue) (int64, error)
}

type ProcessingRunRepository interface {
	Create(ctx context.Context, status string) (int64, error)
	SetBatch(ctx context.Context, id int64, batchID string, postsSubmitted int) error
	Finish(ctx context.Context, request processingrun.FinishProcessingRun) error
	Get(ctx context.Context, id int64) (*processingrun.ProcessingRun, error)
}
