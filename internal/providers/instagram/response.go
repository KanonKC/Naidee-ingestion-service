package instagram

import (
	"context"
	"encoding/json"
	"time"
)

// Client is the contract the ingestion service depends on.
type Client interface {
	// DiscoverMedia pulls the most recent posts of a single IG account.
	DiscoverMedia(ctx context.Context, username string, limit int) (*DiscoveryResult, error)
	// VerifyToken checks the configured access token at startup so a broken
	// token fails the boot instead of silently killing the 3am cron run.
	VerifyToken(ctx context.Context) error
}

type DiscoveryResult struct {
	Username string
	IGUserID string
	Media    []Media
}

type Media struct {
	ID           string
	Caption      string
	MediaType    string
	MediaURL     string
	ThumbnailURL string
	Permalink    string
	Timestamp    time.Time
	// Raw is the untouched JSON node, stored in ig_raw_posts.raw_payload so a
	// future schema change can backfill without re-hitting the API.
	Raw json.RawMessage
}

// --- wire types -------------------------------------------------------------

type businessDiscoveryResponse struct {
	BusinessDiscovery *struct {
		ID       string `json:"id"`
		Username string `json:"username"`
		Media    struct {
			Data []json.RawMessage `json:"data"`
		} `json:"media"`
	} `json:"business_discovery"`
	ID string `json:"id"`
}

type mediaNode struct {
	ID           string `json:"id"`
	Caption      string `json:"caption"`
	MediaType    string `json:"media_type"`
	MediaURL     string `json:"media_url"`
	ThumbnailURL string `json:"thumbnail_url"`
	Permalink    string `json:"permalink"`
	Timestamp    string `json:"timestamp"`
}

type graphErrorResponse struct {
	Error struct {
		Message      string `json:"message"`
		Type         string `json:"type"`
		Code         int    `json:"code"`
		ErrorSubcode int    `json:"error_subcode"`
		FBTraceID    string `json:"fbtrace_id"`
	} `json:"error"`
}

type meResponse struct {
	ID string `json:"id"`
}
