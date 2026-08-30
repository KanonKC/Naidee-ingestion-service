package config

import (
	"time"

	"event/ingestion-service/internal/constants"
)

// Configurations is the typed shape of every setting either binary in this
// module reads from the environment. cmd/ingestion and cmd/processing each
// load their own environment (own .env, own container), so only the fields
// relevant to whichever binary is running end up populated — the other
// binary's fields are simply left at their zero value and never read.
type Configurations struct {
	Env         constants.Environment
	Port        int
	BindAddress string
	DatabaseURL string

	Instagram InstagramConfigurations
	LLM       LLMConfigurations
	Geocode   GeocodeConfigurations

	IngestionCron  IngestionCronConfigurations
	ProcessingCron ProcessingCronConfigurations

	Alert AlertConfigurations
	Admin AdminConfigurations
}

type InstagramConfigurations struct {
	// AccessToken is a long-lived token (60 days). See README on rotation.
	AccessToken string
	// UserID is our own IG Business account, the node Business Discovery runs against.
	UserID string
	// APIVersion is pinned on purpose — never rely on the Graph API default.
	APIVersion string
	// MediaLimit is how many recent posts to pull per source.
	MediaLimit int
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

// IngestionCronConfigurations drives cmd/ingestion's schedule (IG_* env vars
// unprefixed, since it was the first cron job this module had).
type IngestionCronConfigurations struct {
	Schedule          string
	WorkerConcurrency int
	RunOnStartup      bool
}

// ProcessingCronConfigurations drives cmd/processing's schedule. Its env vars
// are PROCESSING_-prefixed so they don't collide with IngestionCron's.
type ProcessingCronConfigurations struct {
	Schedule     string
	RunOnStartup bool
}

type AlertConfigurations struct {
	// GoogleChatWebhookURL is optional. When empty, alerting is a no-op.
	GoogleChatWebhookURL string
}

type AdminConfigurations struct {
	// APIKey guards cmd/ingestion's manual trigger. When empty that route is not
	// registered at all — an unauthenticated trigger is worse than no trigger.
	APIKey string
	// APIToken guards cmd/processing's manual trigger. Same rule: empty disables
	// the route rather than registering it unauthenticated.
	APIToken string
}
