package ingestion

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"event/ingestion-service/internal/config"
	"event/ingestion-service/internal/controllers"
	"event/ingestion-service/internal/logging"
	ingestionservice "event/ingestion-service/internal/services/ingestion"
)

// manualRunTimeout bounds a manually triggered run so a wedged one cannot hold
// the service's single run slot forever.
const manualRunTimeout = 15 * time.Minute

type Controller struct {
	cfg              *config.Configurations
	ingestionService *ingestionservice.Service
	logger           *logging.TLogger

	// baseCtx is the application context. A triggered run has to outlive the
	// request that started it, so it cannot hang off r.Context() — but it must
	// still be cancelled on shutdown, which r.Context() would not give us either.
	baseCtx context.Context
}

func NewController(baseCtx context.Context, cfg *config.Configurations, ingestionService *ingestionservice.Service) *Controller {
	return &Controller{
		cfg:              cfg,
		ingestionService: ingestionService,
		logger:           logging.New(logging.LayerController),
		baseCtx:          baseCtx,
	}
}

// Trigger backs POST /api/v1/admin/ingestion/trigger — a manual run, for when
// waiting up to six hours for the next cron tick is not an option.
//
// It acknowledges and runs in the background by default. Pass ?wait=true to
// block until the run finishes and get the counters back, which is what makes
// the idempotency check ("run twice, posts_new must be 0") easy to verify.
func (c *Controller) Trigger(w http.ResponseWriter, r *http.Request) {
	logger := c.logger.SetContext("controller.ingestion.trigger")

	if !controllers.AuthenticateAdmin(r, c.cfg) {
		logger.Warn(logging.Meta{Message: "Invalid admin key"})
		respond(w, logger, http.StatusUnauthorized, errorResponse{Message: "Invalid admin key"})
		return
	}

	if r.URL.Query().Get("wait") == "true" {
		c.triggerAndWait(w, logger)
		return
	}

	logger.Info(logging.Meta{Message: "Manual ingestion trigger received", Data: map[string]any{"mode": "async"}})

	if !c.ingestionService.TriggerAsync(c.baseCtx) {
		respond(w, logger, http.StatusConflict, errorResponse{Message: "An ingestion run is already in progress"})
		return
	}

	respond(w, logger, http.StatusAccepted, acceptedResponse{
		Status:  "accepted",
		Message: "Ingestion run started. Watch the ingestion_runs table or the logs for the outcome.",
	})
}

func (c *Controller) triggerAndWait(w http.ResponseWriter, logger *logging.TLogger) {
	logger.Info(logging.Meta{Message: "Manual ingestion trigger received", Data: map[string]any{"mode": "wait"}})

	// The server's write timeout is sized for the health endpoint, so extend the
	// deadline for this one response rather than loosening it service-wide.
	if err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(manualRunTimeout + time.Minute)); err != nil {
		logger.Warn(logging.Meta{Message: "Could not extend the write deadline for a synchronous run", Error: err})
	}

	runCtx, cancel := context.WithTimeout(c.baseCtx, manualRunTimeout)
	defer cancel()

	summary, err := c.ingestionService.Run(runCtx)

	switch {
	case summary == nil && err == nil:
		respond(w, logger, http.StatusConflict, errorResponse{Message: "An ingestion run is already in progress"})

	case summary == nil:
		// The run never got off the ground — the database was unreachable.
		logger.Error(logging.Meta{Message: "Manual ingestion run could not start", Error: err})
		respond(w, logger, http.StatusInternalServerError, errorResponse{Message: "Ingestion run could not start"})

	default:
		// The run completed. A fatal error is not an HTTP failure: it is a
		// finished run whose status says `failed`, and the counters still matter.
		respond(w, logger, http.StatusOK, summary)
	}
}

func respond(w http.ResponseWriter, logger *logging.TLogger, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		logger.Error(logging.Meta{Message: "Failed to write response", Error: err})
	}
}
