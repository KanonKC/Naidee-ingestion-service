package geocode

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"event/ingestion-service/internal/config"
	"event/ingestion-service/internal/logging"
)

const (
	requestTimeout = 15 * time.Second
	// maxResponseBytes caps how much of a response we will read, so a runaway
	// payload cannot exhaust memory.
	maxResponseBytes = 1 << 20 // 1 MiB
	// countryCode biases the search to Thailand. Every source in the whitelist
	// posts Thai events, and "Central World" matches things abroad otherwise.
	countryCode = "th"
)

// Coordinates is a resolved venue pin.
type Coordinates struct {
	Lat, Lng float64
	// DisplayName is Nominatim's canonical address, stored as venues.address_text.
	DisplayName string
}

// Geocoder resolves a venue name to a pin. A nil result with a nil error means
// "searched, found nothing" — that is a normal outcome, not a failure: the
// venue is still created, it just has no coordinates yet.
type Geocoder interface {
	Geocode(ctx context.Context, address string) (*Coordinates, error)
}

// Nominatim is the OpenStreetMap geocoding client.
//
// It is free and its results may be cached indefinitely, which Google's terms
// forbid — that is the whole reason it was chosen. The price is a hard usage
// policy: one request per second, and an identifiable User-Agent. Both are
// enforced here rather than trusted to callers, because the penalty for
// breaking either is an IP-level block that takes the whole service down.
type Nominatim struct {
	cfg    *config.Configurations
	client *http.Client
	logger *logging.TLogger

	// mu serialises requests so the rate limit holds even if the orchestrator
	// ever resolves venues concurrently.
	mu       sync.Mutex
	lastCall time.Time
}

var _ Geocoder = (*Nominatim)(nil)

func New(cfg *config.Configurations) *Nominatim {
	return &Nominatim{
		cfg:    cfg,
		client: &http.Client{Timeout: requestTimeout},
		logger: logging.New(logging.LayerProvider),
	}
}

// Geocode looks up one address, waiting out the rate limit first.
func (p *Nominatim) Geocode(ctx context.Context, address string) (*Coordinates, error) {
	logger := p.logger.SetContext("provider.geocode.geocode", logging.SetContextOptions{Silent: true})

	if err := p.waitForSlot(ctx); err != nil {
		return nil, err
	}

	query := url.Values{}
	query.Set("q", address)
	query.Set("format", "jsonv2")
	query.Set("limit", "1")
	query.Set("countrycodes", countryCode)

	endpoint := fmt.Sprintf("%s/search?%s", p.cfg.Geocode.BaseURL, query.Encode())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build geocode request: %w", err)
	}
	// Nominatim rejects clients it cannot identify. Config validation already
	// refuses to boot without this, so it is never empty here.
	req.Header.Set("User-Agent", p.cfg.Geocode.UserAgent)
	req.Header.Set("Accept", "application/json")

	res, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("geocode request failed: %w", err)
	}
	defer res.Body.Close()

	body, err := io.ReadAll(io.LimitReader(res.Body, maxResponseBytes))
	if err != nil {
		return nil, fmt.Errorf("read geocode response: %w", err)
	}

	if res.StatusCode < 200 || res.StatusCode > 299 {
		// 429 and 403 here mean the usage policy was breached. Surfacing the
		// status is what makes that diagnosable instead of looking like "no
		// venue on earth can be found any more".
		logger.Warn(logging.Meta{
			Message: "geocode request rejected",
			Data:    map[string]any{"status": res.StatusCode, "address": address},
		})
		return nil, fmt.Errorf("geocode responded with HTTP %d", res.StatusCode)
	}

	var places []searchResult
	if err := json.Unmarshal(body, &places); err != nil {
		return nil, fmt.Errorf("decode geocode response: %w", err)
	}
	if len(places) == 0 {
		return nil, nil
	}

	lat, err := strconv.ParseFloat(places[0].Lat, 64)
	if err != nil {
		return nil, fmt.Errorf("parse geocode lat %q: %w", places[0].Lat, err)
	}
	lng, err := strconv.ParseFloat(places[0].Lon, 64)
	if err != nil {
		return nil, fmt.Errorf("parse geocode lon %q: %w", places[0].Lon, err)
	}

	return &Coordinates{Lat: lat, Lng: lng, DisplayName: places[0].DisplayName}, nil
}

// waitForSlot blocks until at least MinInterval has passed since the previous
// request. It holds the lock across the sleep on purpose — that is what makes
// the spacing a real limit rather than a suggestion.
func (p *Nominatim) waitForSlot(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	wait := p.cfg.Geocode.MinInterval - time.Since(p.lastCall)
	if wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}

	p.lastCall = time.Now()
	return nil
}
