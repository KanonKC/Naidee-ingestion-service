package ingestionrun

// FinishIngestionRun closes out a run with its final status and counters.
type FinishIngestionRun struct {
	ID            int64
	Status        string
	SourcesTotal  int
	SourcesOK     int
	SourcesFailed int
	PostsNew      int
	PostsUpdated  int
	Error         *string
}
