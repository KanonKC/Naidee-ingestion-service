package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	// Embeds the tzdata database so Asia/Bangkok resolves on any base image,
	// including scratch containers and Windows dev machines.
	_ "time/tzdata"

	"event/ingestion-service/internal/config"
	"event/ingestion-service/internal/libs"
	"event/ingestion-service/internal/logging"
	"event/ingestion-service/internal/routes"
)

func main() {
	// Config is validated before anything else so a missing variable is a boot
	// failure with a readable message, not a mystery at the first cron tick.
	cfg, err := config.Load()
	if err != nil {
		// The logger is not configured yet, so this one goes straight to stderr.
		os.Stderr.WriteString("ingestion-service failed to start: " + err.Error() + "\n")
		os.Exit(1)
	}

	libs.InitLogger(cfg.Env)
	logger := logging.New(logging.LayerOther).SetContext("ingestion.bootstrap")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := libs.InitPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error(logging.Meta{Message: "Failed to connect to the database", Error: err})
		os.Exit(1)
	}
	defer libs.ClosePostgres()

	app := routes.Build(ctx, cfg, db)

	// Startup token check: a dead token must fail the boot, not the 3am run.
	tokenCtx, cancelToken := context.WithTimeout(ctx, 30*time.Second)
	err = app.IngestionService.VerifyToken(tokenCtx)
	cancelToken()
	if err != nil {
		logger.Error(logging.Meta{
			Message: "Instagram access token is not usable — refusing to start. Check IG_ACCESS_TOKEN, IG_USER_ID and IG_API_VERSION.",
			Error:   err,
		})
		os.Exit(1)
	}

	if err := app.Cron.Run(ctx); err != nil {
		logger.Error(logging.Meta{Message: "Failed to start the scheduler", Error: err})
		os.Exit(1)
	}

	go func() {
		logger.Info(logging.Meta{Message: "Health server listening", Data: map[string]any{"addr": app.Server.Addr}})
		if err := app.Server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error(logging.Meta{Message: "Health server stopped unexpectedly", Error: err})
			stop()
		}
	}()

	<-ctx.Done()
	logger.Info(logging.Meta{Message: "Shutdown signal received, draining"})

	shutdownCtx, cancelShutdown := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancelShutdown()

	app.Cron.Stop(shutdownCtx)
	// A manually triggered run is not owned by the cron scheduler, so wait for
	// it separately — otherwise its ingestion_runs row stays stuck on `running`.
	app.IngestionService.WaitIdle(shutdownCtx)
	if err := app.Server.Shutdown(shutdownCtx); err != nil {
		logger.Error(logging.Meta{Message: "Health server shutdown failed", Error: err})
	}

	logger.Info(logging.Meta{Message: "Shutdown complete"})
}
