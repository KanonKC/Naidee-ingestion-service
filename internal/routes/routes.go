package routes

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"event/ingestion-service/internal/config"
	ingestioncontroller "event/ingestion-service/internal/controllers/ingestion"
	processingcontroller "event/ingestion-service/internal/controllers/processing"
	systemcontroller "event/ingestion-service/internal/controllers/system"
	"event/ingestion-service/internal/cron"
	"event/ingestion-service/internal/logging"
	"event/ingestion-service/internal/providers/claude"
	"event/ingestion-service/internal/providers/geocode"
	"event/ingestion-service/internal/providers/googlechat"
	"event/ingestion-service/internal/providers/instagram"
	"event/ingestion-service/internal/repositories/event"
	"event/ingestion-service/internal/repositories/igrawpost"
	"event/ingestion-service/internal/repositories/igsource"
	"event/ingestion-service/internal/repositories/ingestionrun"
	"event/ingestion-service/internal/repositories/processingrun"
	"event/ingestion-service/internal/repositories/venue"
	"event/ingestion-service/internal/services/ingestion"
	"event/ingestion-service/internal/services/processing"
	systemservice "event/ingestion-service/internal/services/system"

	"github.com/jackc/pgx/v5/pgxpool"
)

// App holds everything main needs to start and stop the service.
type App struct {
	Server           *http.Server
	Cron             *cron.IngestionCron
	IngestionService *ingestion.Service
}

// Build wires every dependency by hand, mirroring routes.ts in blaze-backend:
// providers, then repositories, then services, then controllers, then routes.
// No DI container — the wiring is meant to be readable top to bottom.
func Build(ctx context.Context, cfg *config.Configurations, db *pgxpool.Pool) *App {
	logger := logging.New(logging.LayerOther).SetContext("routes.build", logging.SetContextOptions{Silent: true})

	// Providers
	instagramProvider := instagram.New(cfg)
	googleChatProvider := googlechat.New(cfg)

	// Repository Layer
	igSourceRepository := igsource.NewRepository(db)
	igRawPostRepository := igrawpost.NewRepository(db)
	ingestionRunRepository := ingestionrun.NewRepository(db)

	// Service Layer
	systemSvc := systemservice.NewService(db)
	ingestionService := ingestion.NewService(
		cfg,
		igSourceRepository,
		igRawPostRepository,
		ingestionRunRepository,
		instagramProvider,
		googleChatProvider,
	)

	// Controller Layer
	systemCtrl := systemcontroller.NewController(systemSvc)
	ingestionCtrl := ingestioncontroller.NewController(ctx, cfg, ingestionService)

	// Routes — this service is write-side only. /healthz is for container health
	// checks; the manual trigger is admin-only and never public.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", systemCtrl.Health)

	if cfg.Admin.APIKey != "" {
		mux.HandleFunc("POST /api/v1/admin/ingest/instagram", ingestionCtrl.Trigger)
		logger.Info(logging.Meta{Message: "Manual ingest trigger enabled", Data: map[string]any{"route": "POST /api/v1/admin/ingest/instagram"}})
	} else {
		// Registering it unauthenticated would be worse than not having it.
		logger.Warn(logging.Meta{Message: "Manual ingest trigger disabled: ADMIN_API_KEY is not set"})
	}

	server := &http.Server{
		Addr:              fmt.Sprintf("0.0.0.0:%d", cfg.Port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	return &App{
		Server:           server,
		Cron:             cron.New(cfg, ingestionService),
		IngestionService: ingestionService,
	}
}

// ProcessingApp holds everything cmd/processing's main needs to start and stop
// the service.
type ProcessingApp struct {
	Server            *http.Server
	Cron              *cron.ProcessingCron
	ProcessingService *processing.Service
}

// BuildProcessing wires cmd/processing's dependencies by hand, mirroring
// routes.ts in blaze-backend: providers, then repositories, then services,
// then controllers, then routes. No DI container — the wiring is meant to be
// readable top to bottom.
func BuildProcessing(ctx context.Context, cfg *config.Configurations, db *pgxpool.Pool) *ProcessingApp {
	logger := logging.New(logging.LayerOther).SetContext("routes.buildProcessing", logging.SetContextOptions{Silent: true})

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

	if cfg.Admin.APIKey != "" {
		mux.HandleFunc("POST /admin/runs", processingCtrl.TriggerRun)
		mux.HandleFunc("GET /admin/runs/{id}", processingCtrl.GetRun)
		logger.Info(logging.Meta{Message: "Admin routes enabled", Data: map[string]any{"routes": []string{"POST /admin/runs", "GET /admin/runs/{id}"}}})
	} else {
		// Registering them unauthenticated would be worse than not having them.
		logger.Warn(logging.Meta{Message: "Admin routes disabled: ADMIN_API_KEY is not set"})
	}

	server := &http.Server{
		Addr:              fmt.Sprintf("%s:%d", cfg.BindAddress, cfg.Port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	return &ProcessingApp{
		Server:            server,
		Cron:              cron.NewProcessing(cfg, processingService),
		ProcessingService: processingService,
	}
}
