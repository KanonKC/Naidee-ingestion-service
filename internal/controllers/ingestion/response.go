package ingestion

type acceptedResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

type errorResponse struct {
	Message string `json:"message"`
}
