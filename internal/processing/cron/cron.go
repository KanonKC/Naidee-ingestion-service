package cron

import (
	"context"
	"fmt"
	"time"

	"event/ingestion-service/internal/processing/config"
	"event/ingestion-service/internal/processing/logging"
	"event/ingestion-service/internal/processing/services/processing"

	robfigcron "github.com/robfig/cron/v3"
)

// ProcessingCron owns the schedule. It mirrors the TbCron class in blaze-backend.
type ProcessingCron struct {
	cfg               *config.Configurations
	processingService *processing.Service
	logger            *logging.TLogger
	scheduler         *robfigcron.Cron
}

func New(cfg *config.Configurations, processingService *processing.Service) *ProcessingCron {
	return &ProcessingCron{
		cfg:               cfg,
		processingService: processingService,
		logger:            logging.New(logging.LayerCron),
	}
}

// Run registers the processing job and starts the scheduler.
func (t *ProcessingCron) Run(ctx context.Context) error {
	logger := t.logger.SetContext("cron.processing.run")

	location, err := time.LoadLocation("Asia/Bangkok")
	if err != nil {
		return fmt.Errorf("load Asia/Bangkok timezone: %w", err)
	}

	t.scheduler = robfigcron.New(robfigcron.WithLocation(location))

	// The tick fires every thirty minutes but a run can poll a batch for up to
	// two hours. Service.Run skips itself when a run is already in flight, so
	// the ticks that land during a long batch are no-ops rather than duplicates.
	if _, err := t.scheduler.AddFunc(t.cfg.Cron.Schedule, func() {
		if _, err := t.processingService.Run(ctx); err != nil {
			logger.Error(logging.Meta{Message: "Scheduled processing run failed", Error: err})
		}
	}); err != nil {
		return fmt.Errorf("register processing job with schedule %q: %w", t.cfg.Cron.Schedule, err)
	}

	t.scheduler.Start()
	logger.Info(logging.Meta{
		Message: "Scheduler started",
		Data:    map[string]any{"schedule": t.cfg.Cron.Schedule, "timezone": "Asia/Bangkok"},
	})

	if t.cfg.Cron.RunOnStartup {
		logger.Info(logging.Meta{Message: "RUN_ON_STARTUP is set, triggering an immediate run"})
		go func() {
			if _, err := t.processingService.Run(ctx); err != nil {
				logger.Error(logging.Meta{Message: "Startup processing run failed", Error: err})
			}
		}()
	}

	return nil
}

// Stop halts the scheduler and waits for a job already in flight to finish.
func (t *ProcessingCron) Stop(ctx context.Context) {
	if t.scheduler == nil {
		return
	}
	stopCtx := t.scheduler.Stop()
	select {
	case <-stopCtx.Done():
	case <-ctx.Done():
	}
}
