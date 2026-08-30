package config

import (
	"time"

	"event/ingestion-service/internal/constants"
)

// Configurations is the typed shape of every setting this service reads from
// the environment. It mirrors the Configurations interface in blaze-backend.
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

// IngestionCronConfigurations drives the ingestion job's schedule — pulling
// posts from Instagram. Unprefixed env vars, since it was this module's first
// cron job.
type IngestionCronConfigurations struct {
	Schedule          string
	WorkerConcurrency int
	RunOnStartup      bool
}

// ProcessingCronConfigurations drives the processing job's schedule —
// extracting events from raw posts. A separate, faster-ticking schedule from
// IngestionCron's, so its env vars are PROCESSING_-prefixed to avoid colliding.
type ProcessingCronConfigurations struct {
	Schedule     string
	RunOnStartup bool
}

type AlertConfigurations struct {
	// GoogleChatWebhookURL is optional. When empty, alerting is a no-op.
	GoogleChatWebhookURL string
}

type AdminConfigurations struct {
	// APIKey guards every admin route (ingestion's and processing's alike),
	// checked via the same x-api-key header and AuthenticateAdmin middleware.
	// When empty, none of the admin routes are registered at all — an
	// unauthenticated trigger is worse than no trigger.
	APIKey string
}
