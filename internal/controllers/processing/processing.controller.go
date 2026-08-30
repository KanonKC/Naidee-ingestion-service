package processing

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"event/ingestion-service/internal/config"
	"event/ingestion-service/internal/controllers"
	"event/ingestion-service/internal/logging"
	processingservice "event/ingestion-service/internal/services/processing"
)

type Controller struct {
	cfg               *config.Configurations
	processingService *processingservice.Service
	logger            *logging.TLogger

	// baseCtx is the application context. A triggered run has to outlive the
	// request that started it, so it cannot hang off r.Context() — but it must
	// still be cancelled on shutdown, which r.Context() would not give us either.
	baseCtx context.Context
}

func NewController(baseCtx context.Context, cfg *config.Configurations, processingService *processingservice.Service) *Controller {
	return &Controller{
		cfg:               cfg,
		processingService: processingService,
		logger:            logging.New(logging.LayerController),
		baseCtx:           baseCtx,
	}
}

// TriggerRun backs POST /admin/runs — a manual run, for debugging, backfilling,
// or demoing without waiting for the next cron tick.
//
// It answers immediately and never waits for the batch: a run can take hours,
// which is far longer than any reasonable request timeout. The caller gets a
// run id and polls GET /admin/runs/{id}.
func (c *Controller) TriggerRun(w http.ResponseWriter, r *http.Request) {
	logger := c.logger.SetContext("controller.processing.triggerRun")

	if !controllers.AuthenticateProcessingAdmin(r, c.cfg) {
		logger.Warn(logging.Meta{Message: "Invalid admin token"})
		respond(w, logger, http.StatusUnauthorized, errorResponse{Error: "invalid admin token"})
		return
	}

	runID, started, err := c.processingService.TriggerAsync(c.baseCtx)
	if err != nil {
		logger.Error(logging.Meta{Message: "Manual processing run could not start", Error: err})
		respond(w, logger, http.StatusInternalServerError, errorResponse{Error: "processing run could not start"})
		return
	}

	if !started {
		// No rate limiting is needed on this route: every concurrent call lands
		// here, so hammering it just produces 409s.
		respond(w, logger, http.StatusConflict, conflictResponse{
			Error: "a processing run is already in progress",
			RunID: runID,
		})
		return
	}

	logger.Info(logging.Meta{Message: "Manual processing run started", Data: map[string]any{"run_id": runID}})
	respond(w, logger, http.StatusAccepted, startedResponse{RunID: runID, Status: "started"})
}

// GetRun backs GET /admin/runs/{id}. It reads processing_runs directly — the
// caller polls our table, never Anthropic.
func (c *Controller) GetRun(w http.ResponseWriter, r *http.Request) {
	logger := c.logger.SetContext("controller.processing.getRun", logging.SetContextOptions{Silent: true})

	if !controllers.AuthenticateProcessingAdmin(r, c.cfg) {
		logger.Warn(logging.Meta{Message: "Invalid admin token"})
		respond(w, logger, http.StatusUnauthorized, errorResponse{Error: "invalid admin token"})
		return
	}

	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		respond(w, logger, http.StatusBadRequest, errorResponse{Error: "run id must be an integer"})
		return
	}

	run, err := c.processingService.GetRun(r.Context(), id)
	if err != nil {
		logger.Error(logging.Meta{Message: "Failed to read processing run", Data: map[string]any{"run_id": id}, Error: err})
		respond(w, logger, http.StatusInternalServerError, errorResponse{Error: "could not read the processing run"})
		return
	}
	if run == nil {
		respond(w, logger, http.StatusNotFound, errorResponse{Error: "processing run not found"})
		return
	}

	respond(w, logger, http.StatusOK, run)
}

func respond(w http.ResponseWriter, logger *logging.TLogger, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		logger.Error(logging.Meta{Message: "Failed to write response", Error: err})
	}
}
