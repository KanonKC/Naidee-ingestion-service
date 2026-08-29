package system

import (
	"encoding/json"
	"net/http"

	"event/ingestion-service/internal/logging"
	systemservice "event/ingestion-service/internal/services/system"
)

type Controller struct {
	systemService *systemservice.Service
	logger        *logging.TLogger
}

func NewController(systemService *systemservice.Service) *Controller {
	return &Controller{systemService: systemService, logger: logging.New(logging.LayerController)}
}

// Health backs GET /healthz. It is for container health checks only — this
// service exposes no public API.
func (c *Controller) Health(w http.ResponseWriter, r *http.Request) {
	logger := c.logger.SetContext("controller.system.health", logging.SetContextOptions{Silent: true})

	health := c.systemService.GetHealth(r.Context())

	status := http.StatusOK
	if !health.Database {
		logger.Error(logging.Meta{Message: "Health check failed", Data: health})
		status = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(health); err != nil {
		logger.Error(logging.Meta{Message: "Failed to write health response", Error: err})
	}
}
