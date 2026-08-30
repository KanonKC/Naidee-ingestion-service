package processingrun

import "time"

// FinishProcessingRun closes out a run with its final status and counters.
type FinishProcessingRun struct {
	ID             int64
	Status         string
	PostsSubmitted int
	PostsSucceeded int
	PostsFailed    int
	EventsGeocoded int
	Error          *string
}

// ProcessingRun is a row of processing_runs, as served by GET /api/v1/admin/processing/runs/{id}.
type ProcessingRun struct {
	ID             int64      `json:"id"`
	BatchID        *string    `json:"batch_id"`
	Status         string     `json:"status"`
	PostsSubmitted int        `json:"posts_submitted"`
	PostsSucceeded int        `json:"posts_succeeded"`
	PostsFailed    int        `json:"posts_failed"`
	EventsGeocoded int        `json:"events_geocoded"`
	StartedAt      time.Time  `json:"started_at"`
	FinishedAt     *time.Time `json:"finished_at"`
	Error          *string    `json:"error"`
}
