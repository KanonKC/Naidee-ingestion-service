package ingestion

import "time"

const (
	// SourceKindInstagram is written to ingestion_runs.source_kind.
	SourceKindInstagram = "instagram"

	// Run statuses.
	StatusRunning = "running"
	StatusSuccess = "success"
	StatusPartial = "partial"
	StatusFailed  = "failed"

	// maxRetries is how many extra attempts a transient failure earns.
	maxRetries = 3

	// autoDeactivateThreshold takes a source out of rotation once it has failed
	// this many runs in a row, so a dead account stops eating rate-limit quota.
	autoDeactivateThreshold = 5

	// bookkeepingTimeout bounds the status writes that must still happen after
	// the run context has been cancelled by a fatal error.
	bookkeepingTimeout = 10 * time.Second
)

// retryBaseDelay is the first backoff step; it doubles per retry (1s, 2s, 4s).
// A var rather than a const so tests can shorten it.
var retryBaseDelay = time.Second
