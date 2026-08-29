package cron

import (
	"context"
	"fmt"
	"time"

	"event/ingestion-service/internal/config"
	"event/ingestion-service/internal/logging"
	"event/ingestion-service/internal/services/ingestion"

	robfigcron "github.com/robfig/cron/v3"
)

// IngestionCron owns the schedule. It mirrors the TbCron class in blaze-backend.
type IngestionCron struct {
	cfg              *config.Configurations
	ingestionService *ingestion.Service
	logger           *logging.TLogger
	scheduler        *robfigcron.Cron
}

func New(cfg *config.Configurations, ingestionService *ingestion.Service) *IngestionCron {
	return &IngestionCron{
		cfg:              cfg,
		ingestionService: ingestionService,
		logger:           logging.New(logging.LayerCron),
	}
}

// Run registers the ingestion job and starts the scheduler.
func (t *IngestionCron) Run(ctx context.Context) error {
	logger := t.logger.SetContext("cron.ingestion.run")

	location, err := time.LoadLocation("Asia/Bangkok")
	if err != nil {
		return fmt.Errorf("load Asia/Bangkok timezone: %w", err)
	}

	t.scheduler = robfigcron.New(robfigcron.WithLocation(location))

	// Instagram ingestion job
	if _, err := t.scheduler.AddFunc(t.cfg.Cron.Schedule, func() {
		if _, err := t.ingestionService.Run(ctx); err != nil {
			logger.Error(logging.Meta{Message: "Scheduled ingestion run failed", Error: err})
		}
	}); err != nil {
		return fmt.Errorf("register ingestion job with schedule %q: %w", t.cfg.Cron.Schedule, err)
	}

	t.scheduler.Start()
	logger.Info(logging.Meta{
		Message: "Scheduler started",
		Data:    map[string]any{"schedule": t.cfg.Cron.Schedule, "timezone": "Asia/Bangkok", "concurrency": t.cfg.Cron.WorkerConcurrency},
	})

	if t.cfg.Cron.RunOnStartup {
		logger.Info(logging.Meta{Message: "RUN_ON_STARTUP is set, triggering an immediate run"})
		go func() {
			if _, err := t.ingestionService.Run(ctx); err != nil {
				logger.Error(logging.Meta{Message: "Startup ingestion run failed", Error: err})
			}
		}()
	}

	return nil
}

// Stop halts the scheduler and waits for a job already in flight to finish.
func (t *IngestionCron) Stop(ctx context.Context) {
	if t.scheduler == nil {
		return
	}
	stopCtx := t.scheduler.Stop()
	select {
	case <-stopCtx.Done():
	case <-ctx.Done():
	}
}
