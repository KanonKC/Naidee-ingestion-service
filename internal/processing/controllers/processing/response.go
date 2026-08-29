package processing

// startedResponse is the 202 body of POST /admin/runs.
type startedResponse struct {
	RunID  int64  `json:"run_id"`
	Status string `json:"status"`
}

// conflictResponse is the 409 body: which run is in the way.
type conflictResponse struct {
	Error string `json:"error"`
	RunID int64  `json:"run_id"`
}

type errorResponse struct {
	Error string `json:"error"`
}
