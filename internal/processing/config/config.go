package config

import (
	"time"

	"event/ingestion-service/internal/processing/constants"
)

// Configurations is the typed shape of every setting this service reads from
// the environment. It mirrors the Configurations interface in blaze-backend.
type Configurations struct {
	Env         constants.Environment
	Port        int
	BindAddress string
	DatabaseURL string
	LLM         LLMConfigurations
	Geocode     GeocodeConfigurations
	Cron        CronConfigurations
	Admin       AdminConfigurations
}

type LLMConfigurations struct {
	APIKey string
	// Model is the Claude model every batch request runs against.
	Model string
	// MaxTokens caps each extraction so a runaway response cannot inflate the bill.
	MaxTokens int64
	// PostLimit bounds how many raw posts one run submits. Guards against a
	// first-day backlog too large for a single batch.
	PostLimit int
	// PollInterval is how often a submitted batch is checked for completion.
	PollInterval time.Duration
	// PollTimeout gives up on a batch that never ends. The batch keeps running on
	// Anthropic's side; the posts stay unprocessed and the next run retries them.
	PollTimeout time.Duration
}

type GeocodeConfigurations struct {
	BaseURL string
	// UserAgent is mandatory: Nominatim blocks unidentified clients.
	UserAgent string
	// MinInterval is the client-side rate limit. The public instance allows one
	// request per second and blocks IPs that go faster.
	MinInterval time.Duration
}

type CronConfigurations struct {
	Schedule     string
	RunOnStartup bool
}

type AdminConfigurations struct {
	// APIToken guards the manual trigger. When empty those routes are not
	// registered at all — an unauthenticated trigger is worse than no trigger.
	APIToken string
}
