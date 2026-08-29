package libs

import (
	"log/slog"
	"os"

	"event/ingestion-service/internal/processing/constants"
)

// Logger is the process-wide structured logger singleton, mirroring the winston
// singleton in blaze-backend. TLogger wraps it; application code should not use
// it directly.
var Logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

// InitLogger swaps the handler once the environment is known. Local runs get the
// human-readable text handler; dev/prod emit JSON for log aggregation.
func InitLogger(env constants.Environment) {
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	if env == constants.EnvironmentLocal {
		opts.Level = slog.LevelDebug
		Logger = slog.New(slog.NewTextHandler(os.Stdout, opts))
		return
	}
	Logger = slog.New(slog.NewJSONHandler(os.Stdout, opts))
}
