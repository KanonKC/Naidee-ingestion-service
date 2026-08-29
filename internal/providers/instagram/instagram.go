package instagram

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"event/ingestion-service/internal/config"
	"event/ingestion-service/internal/logging"
)

const (
	graphBaseURL = "https://graph.facebook.com"
	// igTimestampLayout is the format the Graph API returns, e.g. 2026-08-29T10:30:00+0000.
	igTimestampLayout = "2006-01-02T15:04:05-0700"
	requestTimeout    = 20 * time.Second
	// maxResponseBytes caps how much of a response we will read, so a runaway
	// payload cannot exhaust memory.
	maxResponseBytes = 8 << 20 // 8 MiB
)

// Instagram is the Business Discovery client.
type Instagram struct {
	cfg    *config.Configurations
	client *http.Client
	logger *logging.TLogger
	// baseURL is the Graph API root. Overridden only by tests.
	baseURL string
}

var _ Client = (*Instagram)(nil)

func New(cfg *config.Configurations) *Instagram {
	return &Instagram{
		cfg:     cfg,
		client:  &http.Client{Timeout: requestTimeout},
		logger:  logging.New(logging.LayerProvider),
		baseURL: graphBaseURL,
	}
}

// DiscoverMedia fetches the latest posts of username through our own IG
// Business account. One source costs exactly one API call.
func (p *Instagram) DiscoverMedia(ctx context.Context, username string, limit int) (*DiscoveryResult, error) {
	logger := p.logger.SetContext("provider.instagram.discoverMedia", logging.SetContextOptions{Silent: true})

	fields := fmt.Sprintf(
		"business_discovery.username(%s){id,username,media.limit(%d){id,caption,media_type,media_url,thumbnail_url,permalink,timestamp}}",
		username, limit,
	)

	query := url.Values{}
	query.Set("fields", fields)

	endpoint := fmt.Sprintf("%s/%s/%s?%s", p.baseURL, p.cfg.Instagram.APIVersion, p.cfg.Instagram.UserID, query.Encode())

	body, status, err := p.do(ctx, endpoint)
	if err != nil {
		logger.Warn(logging.Meta{Message: "Business Discovery request failed", Data: map[string]any{"username": username}, Error: err})
		return nil, err
	}

	if apiErr := decodeGraphError(body, status); apiErr != nil {
		return nil, apiErr
	}

	var payload businessDiscoveryResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, &APIError{Kind: ErrTransient, Message: fmt.Sprintf("decode business discovery response: %v", err), Err: err}
	}

	// A 200 with no business_discovery block means the target is not reachable
	// through Business Discovery at all — a personal account, most often.
	if payload.BusinessDiscovery == nil || payload.BusinessDiscovery.Username == "" {
		return nil, &APIError{
			Kind:    ErrPermanent,
			Code:    110,
			Message: fmt.Sprintf("business_discovery returned no data for %q (not a business or creator account)", username),
		}
	}

	result := &DiscoveryResult{
		Username: payload.BusinessDiscovery.Username,
		IGUserID: payload.BusinessDiscovery.ID,
		Media:    make([]Media, 0, len(payload.BusinessDiscovery.Media.Data)),
	}

	for _, raw := range payload.BusinessDiscovery.Media.Data {
		var node mediaNode
		if err := json.Unmarshal(raw, &node); err != nil {
			// One malformed node must not cost us the rest of the account.
			logger.Warn(logging.Meta{Message: "Skipping malformed media node", Data: map[string]any{"username": username}, Error: err})
			continue
		}

		timestamp, err := time.Parse(igTimestampLayout, node.Timestamp)
		if err != nil {
			logger.Warn(logging.Meta{
				Message: "Skipping media with unparsable timestamp",
				Data:    map[string]any{"username": username, "ig_media_id": node.ID, "timestamp": node.Timestamp},
				Error:   err,
			})
			continue
		}

		result.Media = append(result.Media, Media{
			ID:           node.ID,
			Caption:      node.Caption,
			MediaType:    node.MediaType,
			MediaURL:     node.MediaURL,
			ThumbnailURL: node.ThumbnailURL,
			Permalink:    node.Permalink,
			Timestamp:    timestamp,
			Raw:          raw,
		})
	}

	return result, nil
}

// VerifyToken hits /me?fields=id purely to prove the configured token works.
func (p *Instagram) VerifyToken(ctx context.Context) error {
	endpoint := fmt.Sprintf("%s/%s/me?fields=id", p.baseURL, p.cfg.Instagram.APIVersion)

	body, status, err := p.do(ctx, endpoint)
	if err != nil {
		return err
	}
	if apiErr := decodeGraphError(body, status); apiErr != nil {
		return apiErr
	}

	var payload meResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return &APIError{Kind: ErrTransient, Message: fmt.Sprintf("decode /me response: %v", err), Err: err}
	}
	if payload.ID == "" {
		return &APIError{Kind: ErrFatal, Code: 190, Message: "/me returned no id — the access token is not usable"}
	}
	return nil
}

// do issues the request and returns the raw body plus HTTP status. The token
// travels in the Authorization header rather than the query string so it never
// lands in access logs or proxy caches.
func (p *Instagram) do(ctx context.Context, endpoint string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, 0, &APIError{Kind: ErrPermanent, Message: fmt.Sprintf("build request: %v", err), Err: err}
	}
	req.Header.Set("Authorization", "Bearer "+p.cfg.Instagram.AccessToken)
	req.Header.Set("Accept", "application/json")

	res, err := p.client.Do(req)
	if err != nil {
		// Timeouts, DNS failures and connection resets are all worth a retry.
		if ctx.Err() != nil {
			return nil, 0, ctx.Err()
		}
		return nil, 0, &APIError{Kind: ErrTransient, Message: fmt.Sprintf("http request failed: %v", err), Err: err}
	}
	defer res.Body.Close()

	body, err := io.ReadAll(io.LimitReader(res.Body, maxResponseBytes))
	if err != nil {
		return nil, res.StatusCode, &APIError{Kind: ErrTransient, Message: fmt.Sprintf("read response body: %v", err), Err: err}
	}
	return body, res.StatusCode, nil
}

// decodeGraphError turns a Graph API error envelope into a classified APIError.
// It returns nil when the response carries no error.
func decodeGraphError(body []byte, status int) *APIError {
	var envelope graphErrorResponse
	if err := json.Unmarshal(body, &envelope); err == nil && envelope.Error.Message != "" {
		return &APIError{
			Kind:    classify(envelope.Error.Code, status),
			Code:    envelope.Error.Code,
			Message: envelope.Error.Message,
		}
	}

	// No parsable error envelope but a non-2xx status: classify on status alone.
	if status < 200 || status > 299 {
		return &APIError{
			Kind:    classify(0, status),
			Message: fmt.Sprintf("unexpected HTTP %d from Graph API", status),
		}
	}
	return nil
}
