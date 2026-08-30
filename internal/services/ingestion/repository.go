package ingestion

import (
	"context"

	"event/ingestion-service/internal/repositories/igrawpost"
	"event/ingestion-service/internal/repositories/igsource"
	"event/ingestion-service/internal/repositories/ingestionrun"
)

// The repository layer is made of concrete structs, as in blaze-backend. These
// interfaces are declared here, on the consumer side, because Go convention is
// to accept interfaces — it keeps the orchestration logic testable without a
// database. The real repositories satisfy them implicitly.

type IgSourceRepository interface {
	ListActive(ctx context.Context) ([]igsource.IgSource, error)
	MarkSynced(ctx context.Context, id int64) error
	MarkFailed(ctx context.Context, request igsource.MarkFailedIgSource) (int, error)
	Deactivate(ctx context.Context, request igsource.DeactivateIgSource) error
}

type IgRawPostRepository interface {
	Upsert(ctx context.Context, request igrawpost.UpsertIgRawPost) (bool, error)
}

type IngestionRunRepository interface {
	Create(ctx context.Context, sourceKind string) (int64, error)
	Finish(ctx context.Context, request ingestionrun.FinishIngestionRun) error
}
