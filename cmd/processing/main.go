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

	"event/ingestion-service/internal/processing/config"
	"event/ingestion-service/internal/processing/libs"
	"event/ingestion-service/internal/processing/logging"
	"event/ingestion-service/internal/processing/routes"
)

func main() {
	// Config is validated before anything else so a missing variable is a boot
	// failure with a readable message, not a mystery at the first cron tick.
	cfg, err := config.Load()
	if err != nil {
		// The logger is not configured yet, so this one goes straight to stderr.
		os.Stderr.WriteString("processing-service failed to start: " + err.Error() + "\n")
		os.Exit(1)
	}

	libs.InitLogger(cfg.Env)
	logger := logging.New(logging.LayerOther).SetContext("processing.bootstrap")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := libs.InitPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error(logging.Meta{Message: "Failed to connect to the database", Error: err})
		os.Exit(1)
	}
	defer libs.ClosePostgres()

	app := routes.Build(ctx, cfg, db)

	if err := app.Cron.Run(ctx); err != nil {
		logger.Error(logging.Meta{Message: "Failed to start the scheduler", Error: err})
		os.Exit(1)
	}

	go func() {
		logger.Info(logging.Meta{Message: "Admin server listening", Data: map[string]any{"addr": app.Server.Addr}})
		if err := app.Server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error(logging.Meta{Message: "Admin server stopped unexpectedly", Error: err})
			stop()
		}
	}()

	<-ctx.Done()
	logger.Info(logging.Meta{Message: "Shutdown signal received, draining"})

	shutdownCtx, cancelShutdown := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancelShutdown()

	app.Cron.Stop(shutdownCtx)
	// A manually triggered run is not owned by the cron scheduler, so wait for
	// it separately — otherwise its processing_runs row stays stuck on `polling`.
	app.ProcessingService.WaitIdle(shutdownCtx)
	if err := app.Server.Shutdown(shutdownCtx); err != nil {
		logger.Error(logging.Meta{Message: "Admin server shutdown failed", Error: err})
	}

	logger.Info(logging.Meta{Message: "Shutdown complete"})
}
