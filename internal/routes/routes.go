package routes

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"event/ingestion-service/internal/config"
	ingestioncontroller "event/ingestion-service/internal/controllers/ingestion"
	systemcontroller "event/ingestion-service/internal/controllers/system"
	"event/ingestion-service/internal/cron"
	"event/ingestion-service/internal/logging"
	"event/ingestion-service/internal/providers/googlechat"
	"event/ingestion-service/internal/providers/instagram"
	"event/ingestion-service/internal/repositories/igrawpost"
	"event/ingestion-service/internal/repositories/igsource"
	"event/ingestion-service/internal/repositories/ingestionrun"
	"event/ingestion-service/internal/services/ingestion"
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
