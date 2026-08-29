package processing

// runCounters aggregates per-post results across one run.
//
// Unlike the ingestion service's equivalent this needs no mutex: a processing
// run walks its results on a single goroutine. Geocoding is the slow part and
// it is rate-limited to one call per second anyway, so there is nothing to be
// gained from parallelising the walk.
type runCounters struct {
	postsSubmitted int
	postsSucceeded int
	postsFailed    int
	eventsGeocoded int
}

// RunSummary is what a completed run reports back to its caller.
type RunSummary struct {
	RunID          int64   `json:"run_id"`
	BatchID        *string `json:"batch_id"`
	Status         string  `json:"status"`
	PostsSubmitted int     `json:"posts_submitted"`
	PostsSucceeded int     `json:"posts_succeeded"`
	PostsFailed    int     `json:"posts_failed"`
	EventsGeocoded int     `json:"events_geocoded"`
}
