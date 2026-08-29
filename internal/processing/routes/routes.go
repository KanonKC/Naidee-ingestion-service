package routes

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"event/ingestion-service/internal/processing/config"
	processingcontroller "event/ingestion-service/internal/processing/controllers/processing"
	systemcontroller "event/ingestion-service/internal/processing/controllers/system"
	"event/ingestion-service/internal/processing/cron"
	"event/ingestion-service/internal/processing/logging"
	"event/ingestion-service/internal/processing/providers/claude"
	"event/ingestion-service/internal/processing/providers/geocode"
	"event/ingestion-service/internal/processing/repositories/event"
	"event/ingestion-service/internal/processing/repositories/igrawpost"
	"event/ingestion-service/internal/processing/repositories/processingrun"
	"event/ingestion-service/internal/processing/repositories/venue"
	"event/ingestion-service/internal/processing/services/processing"
	systemservice "event/ingestion-service/internal/processing/services/system"

	"github.com/jackc/pgx/v5/pgxpool"
)

// App holds everything main needs to start and stop the service.
type App struct {
	Server            *http.Server
	Cron              *cron.ProcessingCron
	ProcessingService *processing.Service
}

// Build wires every dependency by hand, mirroring routes.ts in blaze-backend:
// providers, then repositories, then services, then controllers, then routes.
// No DI container — the wiring is meant to be readable top to bottom.
func Build(ctx context.Context, cfg *config.Configurations, db *pgxpool.Pool) *App {
	logger := logging.New(logging.LayerOther).SetContext("routes.build", logging.SetContextOptions{Silent: true})

	// Providers
	claudeProvider := claude.New(cfg)
	geocodeProvider := geocode.New(cfg)

	// Repository Layer
	igRawPostRepository := igrawpost.NewRepository(db)
	eventRepository := event.NewRepository(db)
	venueRepository := venue.NewRepository(db)
	processingRunRepository := processingrun.NewRepository(db)

	// Service Layer
	systemSvc := systemservice.NewService(db)
	processingService := processing.NewService(
		cfg,
		igRawPostRepository,
		eventRepository,
		venueRepository,
		processingRunRepository,
		claudeProvider,
		geocodeProvider,
	)

	// Controller Layer
	systemCtrl := systemcontroller.NewController(systemSvc)
	processingCtrl := processingcontroller.NewController(ctx, cfg, processingService)

	// Routes — this service is write-side only. /healthz is for container health
	// checks; the admin routes are internal tooling and never public. The read
	// API that serves the website is a separate service entirely.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", systemCtrl.Health)

	if cfg.Admin.APIToken != "" {
		mux.HandleFunc("POST /admin/runs", processingCtrl.TriggerRun)
		mux.HandleFunc("GET /admin/runs/{id}", processingCtrl.GetRun)
		logger.Info(logging.Meta{Message: "Admin routes enabled", Data: map[string]any{"routes": []string{"POST /admin/runs", "GET /admin/runs/{id}"}}})
	} else {
		// Registering them unauthenticated would be worse than not having them.
		logger.Warn(logging.Meta{Message: "Admin routes disabled: ADMIN_API_TOKEN is not set"})
	}

	server := &http.Server{
		Addr:              fmt.Sprintf("%s:%d", cfg.BindAddress, cfg.Port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	return &App{
		Server:            server,
		Cron:              cron.New(cfg, processingService),
		ProcessingService: processingService,
	}
}
