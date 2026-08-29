package googlechat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"event/ingestion-service/internal/config"
	"event/ingestion-service/internal/logging"
)

// Notifier is the alerting contract. Fatal ingestion failures must reach a human
// rather than sit in a log file.
type Notifier interface {
	Notify(ctx context.Context, message string) error
}

type GoogleChat struct {
	cfg    *config.Configurations
	client *http.Client
	logger *logging.TLogger
}

var _ Notifier = (*GoogleChat)(nil)

func New(cfg *config.Configurations) *GoogleChat {
	return &GoogleChat{
		cfg:    cfg,
		client: &http.Client{Timeout: 10 * time.Second},
		logger: logging.New(logging.LayerProvider),
	}
}

// Notify posts a plain-text card to the configured Google Chat space. With no
// webhook configured it is a no-op, so alerting stays optional in local runs.
func (p *GoogleChat) Notify(ctx context.Context, message string) error {
	logger := p.logger.SetContext("provider.googleChat.notify", logging.SetContextOptions{Silent: true})

	if p.cfg.Alert.GoogleChatWebhookURL == "" {
		logger.Debug(logging.Meta{Message: "Alert skipped, no webhook configured", Data: map[string]any{"alert": message}})
		return nil
	}

	body, err := json.Marshal(webhookRequest{Text: message})
	if err != nil {
		return fmt.Errorf("marshal google chat payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.cfg.Alert.GoogleChatWebhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build google chat request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")

	res, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("post to google chat: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode < 200 || res.StatusCode > 299 {
		return fmt.Errorf("google chat responded with HTTP %d", res.StatusCode)
	}

	logger.Info(logging.Meta{Message: "Alert delivered"})
	return nil
}
