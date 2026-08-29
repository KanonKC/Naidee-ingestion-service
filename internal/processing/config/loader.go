package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"event/ingestion-service/internal/processing/constants"

	"github.com/joho/godotenv"
)

// Load reads configuration from the environment (and a .env file when present)
// and validates it. Every caller is expected to fail fast on a non-nil error —
// a half-configured cron job that dies at 3am is worse than one that never boots.
func Load() (*Configurations, error) {
	// Missing .env is fine: in dev/prod the environment is injected by the runtime.
	_ = godotenv.Load()

	cfg := &Configurations{
		Env:  constants.MakeEnvironment(os.Getenv("ENV")),
		Port: envInt("PORT", 8083),
		// Loopback by default: the admin trigger is an internal tool, never public.
		// Containers that need it reachable set 0.0.0.0 explicitly.
		BindAddress: envString("HTTP_BIND_ADDRESS", "127.0.0.1"),
		DatabaseURL: os.Getenv("DATABASE_URL"),
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
		Cron: CronConfigurations{
			Schedule:     envString("CRON_SCHEDULE", "*/30 * * * *"),
			RunOnStartup: envBool("RUN_ON_STARTUP", false),
		},
		Admin: AdminConfigurations{
			APIToken: os.Getenv("ADMIN_API_TOKEN"),
		},
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Validate collects every problem at once so a misconfigured deploy is fixed in
// one pass instead of one restart per missing variable.
func (c *Configurations) Validate() error {
	var problems []string

	if c.DatabaseURL == "" {
		problems = append(problems, "DATABASE_URL is required")
	}
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
	if c.Cron.Schedule == "" {
		problems = append(problems, "CRON_SCHEDULE is required")
	}
	if c.Admin.APIToken != "" && len(c.Admin.APIToken) < 32 {
		problems = append(problems, "ADMIN_API_TOKEN must be at least 32 characters (leave it empty to disable the manual trigger)")
	}
	if c.Port < 1 || c.Port > 65535 {
		problems = append(problems, fmt.Sprintf("PORT must be between 1 and 65535, got %d", c.Port))
	}

	if len(problems) > 0 {
		return fmt.Errorf("invalid configuration:\n  - %s", strings.Join(problems, "\n  - "))
	}
	return nil
}

// maxBatchRequests is the Message Batches API limit on requests per batch.
const maxBatchRequests = 100_000

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
