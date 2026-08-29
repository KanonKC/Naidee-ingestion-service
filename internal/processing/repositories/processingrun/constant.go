package processingrun

// Run statuses written to processing_runs.status.
const (
	// StatusSubmitted is the opening state: the row exists, the batch does not yet.
	StatusSubmitted = "submitted"
	// StatusPolling means the batch was accepted and is being waited on.
	StatusPolling   = "polling"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
)
