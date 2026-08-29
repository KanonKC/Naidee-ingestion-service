package igsource

// DeactivateIgSource turns a source off for good, recording why.
type DeactivateIgSource struct {
	ID     int64
	Reason string
}

// MarkFailedIgSource records a failed attempt and bumps consecutive_failures.
type MarkFailedIgSource struct {
	ID        int64
	LastError string
}
