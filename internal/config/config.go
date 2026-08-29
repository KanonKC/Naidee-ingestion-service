package config

import "event/ingestion-service/internal/constants"

// Configurations is the typed shape of every setting this service reads from
// the environment. It mirrors the Configurations interface in blaze-backend.
type Configurations struct {
	Env         constants.Environment
	Port        int
	DatabaseURL string
	Instagram   InstagramConfigurations
	Cron        CronConfigurations
	Alert       AlertConfigurations
	Admin       AdminConfigurations
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

type CronConfigurations struct {
	Schedule          string
	WorkerConcurrency int
	RunOnStartup      bool
}

type AlertConfigurations struct {
	// GoogleChatWebhookURL is optional. When empty, alerting is a no-op.
	GoogleChatWebhookURL string
}

type AdminConfigurations struct {
	// APIKey guards the manual ingest trigger. When empty that route is not
	// registered at all — an unauthenticated trigger is worse than no trigger.
	APIKey string
}
