package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"event/ingestion-service/internal/constants"

	"github.com/joho/godotenv"
)

// maxBatchRequests is the Message Batches API limit on requests per batch.
const maxBatchRequests = 100_000

// LoadIngestion reads and validates the configuration cmd/ingestion needs.
// Every caller is expected to fail fast on a non-nil error — a
// half-configured cron job that dies at 3am is worse than one that never boots.
func LoadIngestion() (*Configurations, error) {
	cfg, err := load()
	if err != nil {
		return nil, err
	}
	if err := cfg.validateIngestion(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// LoadProcessing reads and validates the configuration cmd/processing needs.
func LoadProcessing() (*Configurations, error) {
	cfg, err := load()
	if err != nil {
		return nil, err
	}
	if err := cfg.validateProcessing(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// load parses every field from the environment, regardless of which binary is
// running. Whichever fields the caller's binary does not care about are simply
// left at whatever the environment (or their fallback) produced, and never read.
func load() (*Configurations, error) {
	// Missing .env is fine: in dev/prod the environment is injected by the runtime.
	_ = godotenv.Load()

	cfg := &Configurations{
		Env:         constants.MakeEnvironment(os.Getenv("ENV")),
		Port:        envInt("PORT", 8082),
		DatabaseURL: os.Getenv("DATABASE_URL"),
		// Shared by both binaries. Defaults to 0.0.0.0 (matches cmd/ingestion's
		// prior hardcoded behavior). cmd/processing's admin trigger is an
		// internal tool that should not be public — its own .env sets
		// HTTP_BIND_ADDRESS=127.0.0.1 explicitly rather than relying on this
		// default.
		BindAddress: envString("HTTP_BIND_ADDRESS", "0.0.0.0"),

		Instagram: InstagramConfigurations{
			AccessToken: os.Getenv("IG_ACCESS_TOKEN"),
			UserID:      os.Getenv("IG_USER_ID"),
			APIVersion:  os.Getenv("IG_API_VERSION"),
			MediaLimit:  envInt("IG_MEDIA_LIMIT", 25),
		},
		LLM: LLMConfigurations{
			APIKey:       os.Getenv("ANTHROPIC_API_KEY"),
			Model:        envString("LLM_MODEL", "claude-haiku-4-5"),
			MaxTokens:    int64(envInt("LLM_MAX_TOKENS", 500)),
			PostLimit:    envInt("LLM_POST_LIMIT", 5000),
			PollInterval: envDuration("BATCH_POLL_INTERVAL", 30*time.Second),
			PollTimeout:  envDuration("BATCH_POLL_TIMEOUT", 2*time.Hour),
		},
		Geocode: GeocodeConfigurations{
			BaseURL:     envString("GEOCODE_BASE_URL", "https://nominatim.openstreetmap.org"),
			UserAgent:   os.Getenv("GEOCODE_USER_AGENT"),
			MinInterval: envDuration("GEOCODE_MIN_INTERVAL", time.Second),
		},

		IngestionCron: IngestionCronConfigurations{
			Schedule:          envString("CRON_SCHEDULE", "0 */6 * * *"),
			WorkerConcurrency: envInt("WORKER_CONCURRENCY", 3),
			RunOnStartup:      envBool("RUN_ON_STARTUP", false),
		},
		ProcessingCron: ProcessingCronConfigurations{
			Schedule:     envString("PROCESSING_CRON_SCHEDULE", "*/30 * * * *"),
			RunOnStartup: envBool("PROCESSING_RUN_ON_STARTUP", false),
		},

		Alert: AlertConfigurations{
			GoogleChatWebhookURL: os.Getenv("GOOGLE_CHAT_WEBHOOK_URL"),
		},
		Admin: AdminConfigurations{
			APIKey: os.Getenv("ADMIN_API_KEY"),
		},
	}

	return cfg, nil
}

// validateCommon checks the fields every binary in this module depends on,
// including ADMIN_API_KEY: both cmd/ingestion and cmd/processing guard their
// manual trigger with the same x-api-key mechanism.
func (c *Configurations) validateCommon() []string {
	var problems []string
	if c.DatabaseURL == "" {
		problems = append(problems, "DATABASE_URL is required")
	}
	if c.Port < 1 || c.Port > 65535 {
		problems = append(problems, fmt.Sprintf("PORT must be between 1 and 65535, got %d", c.Port))
	}
	if c.Admin.APIKey != "" && len(c.Admin.APIKey) < 32 {
		problems = append(problems, "ADMIN_API_KEY must be at least 32 characters (leave it empty to disable the manual trigger)")
	}
	return problems
}

// validateIngestion collects every problem at once so a misconfigured deploy
// is fixed in one pass instead of one restart per missing variable.
func (c *Configurations) validateIngestion() error {
	problems := c.validateCommon()

	if c.Instagram.AccessToken == "" {
		problems = append(problems, "IG_ACCESS_TOKEN is required")
	}
	if c.Instagram.UserID == "" {
		problems = append(problems, "IG_USER_ID is required")
	}
	if c.Instagram.APIVersion == "" {
		problems = append(problems, "IG_API_VERSION is required (pin it, e.g. v25.0)")
	} else if !strings.HasPrefix(c.Instagram.APIVersion, "v") {
		problems = append(problems, fmt.Sprintf("IG_API_VERSION must look like v25.0, got %q", c.Instagram.APIVersion))
	}
	if c.Instagram.MediaLimit < 1 {
		problems = append(problems, "IG_MEDIA_LIMIT must be >= 1")
	}
	if c.IngestionCron.Schedule == "" {
		problems = append(problems, "CRON_SCHEDULE is required")
	}
	if c.IngestionCron.WorkerConcurrency < 1 {
		problems = append(problems, "WORKER_CONCURRENCY must be >= 1")
	}

	return problemsToError(problems)
}

func (c *Configurations) validateProcessing() error {
	problems := c.validateCommon()

	if c.LLM.APIKey == "" {
		problems = append(problems, "ANTHROPIC_API_KEY is required")
	}
	if c.LLM.Model == "" {
		problems = append(problems, "LLM_MODEL is required")
	}
	if c.LLM.MaxTokens < 1 {
		problems = append(problems, "LLM_MAX_TOKENS must be >= 1")
	}
	if c.LLM.PostLimit < 1 {
		problems = append(problems, "LLM_POST_LIMIT must be >= 1")
	}
	// The Batch API caps a batch at 100,000 requests. Refuse a limit that would
	// build a batch the API is guaranteed to reject.
	if c.LLM.PostLimit > maxBatchRequests {
		problems = append(problems, fmt.Sprintf("LLM_POST_LIMIT must be <= %d (Batch API cap)", maxBatchRequests))
	}
	if c.LLM.PollInterval < time.Second {
		problems = append(problems, "BATCH_POLL_INTERVAL must be >= 1s")
	}
	if c.LLM.PollTimeout <= c.LLM.PollInterval {
		problems = append(problems, "BATCH_POLL_TIMEOUT must be greater than BATCH_POLL_INTERVAL")
	}
	if c.Geocode.BaseURL == "" {
		problems = append(problems, "GEOCODE_BASE_URL is required")
	}
	// Nominatim's usage policy requires an identifiable UA and blocks clients
	// without one, so an empty value is a boot failure rather than a warning.
	if c.Geocode.UserAgent == "" {
		problems = append(problems, "GEOCODE_USER_AGENT is required (Nominatim blocks unidentified clients)")
	}
	if c.Geocode.MinInterval < time.Second && strings.Contains(c.Geocode.BaseURL, "nominatim.openstreetmap.org") {
		problems = append(problems, "GEOCODE_MIN_INTERVAL must be >= 1s against the public Nominatim instance")
	}
	if c.ProcessingCron.Schedule == "" {
		problems = append(problems, "PROCESSING_CRON_SCHEDULE is required")
	}

	return problemsToError(problems)
}

func problemsToError(problems []string) error {
	if len(problems) > 0 {
		return fmt.Errorf("invalid configuration:\n  - %s", strings.Join(problems, "\n  - "))
	}
	return nil
}

func envString(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return parsed
}

func envBool(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return parsed
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return parsed
}
