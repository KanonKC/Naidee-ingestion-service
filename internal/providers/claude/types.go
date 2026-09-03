package claude

import (
	"context"
	"time"
)

// ExtractionRequest is one raw post handed to the model.
type ExtractionRequest struct {
	RawPostID int64
	PostedAt  time.Time
	Caption   string
}

// ExtractionResult is one post's outcome. Error is set when this specific
// request failed — the API errored on it, or the model returned something that
// is not the agreed schema. A failed item never blocks the rest of the batch.
type ExtractionResult struct {
	RawPostID       int64
	IsEvent         bool
	Confidence      string
	Title           *string
	VenueName       *string
	AddressDetail   *string
	StartDate       *time.Time
	EndDate         *time.Time
	PriceText       *string
	Category        *string
	RegistrationURL *string
	Error           error
}

// BatchStatus is the poll answer, trimmed to what the orchestrator acts on.
type BatchStatus struct {
	// Ended is true once every request has succeeded, errored, been canceled or
	// expired. Results are only fetchable after that.
	Ended     bool
	Succeeded int64
	Errored   int64
	Canceled  int64
	Expired   int64
}

// BatchClient is the extraction contract. It is an interface so the
// orchestrator can be tested without spending money on real batches.
type BatchClient interface {
	Submit(ctx context.Context, reqs []ExtractionRequest) (batchID string, err error)
	Poll(ctx context.Context, batchID string) (status BatchStatus, err error)
	FetchResults(ctx context.Context, batchID string) ([]ExtractionResult, error)
}

// extractionPayload is the JSON contract stated in the system prompt. Dates
// arrive as strings because the model cannot be trusted to emit a timezone.
type extractionPayload struct {
	IsEvent         bool    `json:"is_event"`
	Confidence      string  `json:"confidence"`
	Title           *string `json:"title"`
	VenueName       *string `json:"venue_name"`
	AddressDetail   *string `json:"address_detail"`
	StartDate       *string `json:"start_date"`
	EndDate         *string `json:"end_date"`
	PriceText       *string `json:"price_text"`
	Category        *string `json:"category"`
	RegistrationURL *string `json:"registration_url"`
}
