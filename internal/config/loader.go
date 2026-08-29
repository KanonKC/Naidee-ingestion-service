package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"event/ingestion-service/internal/constants"

	"github.com/joho/godotenv"
)

// Load reads configuration from the environment (and a .env file when present)
// and validates it. Every caller is expected to fail fast on a non-nil error —
// a half-configured cron job that dies at 3am is worse than one that never boots.
func Load() (*Configurations, error) {
	// Missing .env is fine: in dev/prod the environment is injected by the runtime.
	_ = godotenv.Load()

	cfg := &Configurations{
		Env:         constants.MakeEnvironment(os.Getenv("ENV")),
		Port:        envInt("PORT", 8082),
		DatabaseURL: os.Getenv("DATABASE_URL"),
		Instagram: InstagramConfigurations{
			AccessToken: os.Getenv("IG_ACCESS_TOKEN"),
			UserID:      os.Getenv("IG_USER_ID"),
			APIVersion:  os.Getenv("IG_API_VERSION"),
			MediaLimit:  envInt("IG_MEDIA_LIMIT", 25),
		},
		Cron: CronConfigurations{
			Schedule:          envString("CRON_SCHEDULE", "0 */6 * * *"),
			WorkerConcurrency: envInt("WORKER_CONCURRENCY", 3),
			RunOnStartup:      envBool("RUN_ON_STARTUP", false),
		},
		Alert: AlertConfigurations{
			GoogleChatWebhookURL: os.Getenv("GOOGLE_CHAT_WEBHOOK_URL"),
		},
		Admin: AdminConfigurations{
			APIKey: os.Getenv("ADMIN_API_KEY"),
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
	if c.Cron.Schedule == "" {
		problems = append(problems, "CRON_SCHEDULE is required")
	}
	if c.Cron.WorkerConcurrency < 1 {
		problems = append(problems, "WORKER_CONCURRENCY must be >= 1")
	}
	if c.Admin.APIKey != "" && len(c.Admin.APIKey) < 32 {
		problems = append(problems, "ADMIN_API_KEY must be at least 32 characters (leave it empty to disable the manual trigger)")
	}
	if c.Port < 1 || c.Port > 65535 {
		problems = append(problems, fmt.Sprintf("PORT must be between 1 and 65535, got %d", c.Port))
	}

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
