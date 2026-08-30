package ingestion

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"event/ingestion-service/internal/config"
	"event/ingestion-service/internal/providers/instagram"
	"event/ingestion-service/internal/repositories/igrawpost"
	"event/ingestion-service/internal/repositories/igsource"
	"event/ingestion-service/internal/repositories/ingestionrun"
	ingestionservice "event/ingestion-service/internal/services/ingestion"
)

const testAPIKey = "0123456789abcdef0123456789abcdef"

// --- fakes ------------------------------------------------------------------

type stubSourceRepo struct {
	mu      sync.Mutex
	sources []igsource.IgSource
}

func (s *stubSourceRepo) ListActive(ctx context.Context) ([]igsource.IgSource, error) {
	return s.sources, nil
}
func (s *stubSourceRepo) MarkSynced(ctx context.Context, id int64) error { return nil }
func (s *stubSourceRepo) MarkFailed(ctx context.Context, request igsource.MarkFailedIgSource) (int, error) {
	return 1, nil
}
func (s *stubSourceRepo) Deactivate(ctx context.Context, request igsource.DeactivateIgSource) error {
	return nil
}

type stubPostRepo struct{}

func (s *stubPostRepo) Upsert(ctx context.Context, request igrawpost.UpsertIgRawPost) (bool, error) {
	return true, nil
}

type stubRunRepo struct{}

func (s *stubRunRepo) Create(ctx context.Context, sourceKind string) (int64, error) { return 7, nil }
func (s *stubRunRepo) Finish(ctx context.Context, request ingestionrun.FinishIngestionRun) error {
	return nil
}

type stubInstagram struct {
	// block, when non-nil, holds DiscoverMedia until it is closed.
	block chan struct{}
}

func (s *stubInstagram) DiscoverMedia(ctx context.Context, username string, limit int) (*instagram.DiscoveryResult, error) {
	if s.block != nil {
		select {
		case <-s.block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &instagram.DiscoveryResult{
		Username: username,
		Media: []instagram.Media{{
			ID:        "m1",
			MediaType: "IMAGE",
			Permalink: "https://www.instagram.com/p/m1/",
			Timestamp: time.Now(),
			Raw:       json.RawMessage(`{"id":"m1"}`),
		}},
	}, nil
}

func (s *stubInstagram) VerifyToken(ctx context.Context) error { return nil }

type stubNotifier struct{}

func (s *stubNotifier) Notify(ctx context.Context, message string) error { return nil }

// --- helpers ----------------------------------------------------------------

func newController(t *testing.T, ig instagram.Client) *Controller {
	t.Helper()
	cfg := &config.Configurations{
		Instagram: config.InstagramConfigurations{MediaLimit: 25},
		Cron:      config.CronConfigurations{WorkerConcurrency: 3},
		Admin:     config.AdminConfigurations{APIKey: testAPIKey},
	}
	service := ingestionservice.NewService(
		cfg,
		&stubSourceRepo{sources: []igsource.IgSource{{ID: 1, Username: "bacc_bangkok", IsActive: true}}},
		&stubPostRepo{},
		&stubRunRepo{},
		ig,
		&stubNotifier{},
	)
	return NewController(context.Background(), cfg, service)
}

func triggerRequest(apiKey, query string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/ingest/instagram"+query, nil)
	if apiKey != "" {
		req.Header.Set("x-api-key", apiKey)
	}
	return req
}

// --- tests ------------------------------------------------------------------

func TestTriggerRejectsAMissingAPIKey(t *testing.T) {
	controller := newController(t, &stubInstagram{})
	res := httptest.NewRecorder()

	controller.Trigger(res, triggerRequest("", ""))

	if res.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without a key, got %d", res.Code)
	}
}

func TestTriggerRejectsAWrongAPIKey(t *testing.T) {
	controller := newController(t, &stubInstagram{})
	res := httptest.NewRecorder()

	controller.Trigger(res, triggerRequest("0123456789abcdef0123456789abcdeX", ""))

	if res.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for a wrong key, got %d", res.Code)
	}
}

func TestTriggerRejectsAPrefixOfTheAPIKey(t *testing.T) {
	controller := newController(t, &stubInstagram{})
	res := httptest.NewRecorder()

	controller.Trigger(res, triggerRequest(testAPIKey[:8], ""))

	if res.Code != http.StatusUnauthorized {
		t.Fatalf("a prefix of the key must not authenticate, got %d", res.Code)
	}
}

func TestTriggerAcknowledgesAndRunsInTheBackground(t *testing.T) {
	controller := newController(t, &stubInstagram{})
	res := httptest.NewRecorder()

	controller.Trigger(res, triggerRequest(testAPIKey, ""))

	if res.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d (%s)", res.Code, res.Body.String())
	}

	var body acceptedResponse
	if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil {
		t.Fatalf("response is not the documented shape: %v", err)
	}
	if body.Status != "accepted" {
		t.Fatalf("expected status accepted, got %q", body.Status)
	}

	// Let the background run finish so it does not leak into the next test.
	controller.ingestionService.WaitIdle(context.Background())
}

func TestTriggerWithWaitReturnsTheRunCounters(t *testing.T) {
	controller := newController(t, &stubInstagram{})
	res := httptest.NewRecorder()

	controller.Trigger(res, triggerRequest(testAPIKey, "?wait=true"))

	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var summary ingestionservice.RunSummary
	if err := json.Unmarshal(res.Body.Bytes(), &summary); err != nil {
		t.Fatalf("response is not a RunSummary: %v", err)
	}
	if summary.RunID != 7 {
		t.Fatalf("expected the run id in the response, got %d", summary.RunID)
	}
	if summary.Status != ingestionservice.StatusSuccess {
		t.Fatalf("expected status success, got %q", summary.Status)
	}
	if summary.PostsNew != 1 || summary.SourcesOK != 1 {
		t.Fatalf("expected 1 new post from 1 ok source, got %+v", summary)
	}
}

func TestTriggerConflictsWhileARunIsInProgress(t *testing.T) {
	block := make(chan struct{})
	controller := newController(t, &stubInstagram{block: block})

	// Start a run and let it take the slot.
	first := httptest.NewRecorder()
	controller.Trigger(first, triggerRequest(testAPIKey, ""))
	if first.Code != http.StatusAccepted {
		t.Fatalf("expected the first trigger to be accepted, got %d", first.Code)
	}
	// TriggerAsync takes the run slot synchronously before returning, so by the
	// time a 202 is written the slot is already held — no polling needed.

	second := httptest.NewRecorder()
	controller.Trigger(second, triggerRequest(testAPIKey, ""))
	if second.Code != http.StatusConflict {
		t.Fatalf("expected 409 while a run is in progress, got %d", second.Code)
	}

	third := httptest.NewRecorder()
	controller.Trigger(third, triggerRequest(testAPIKey, "?wait=true"))
	if third.Code != http.StatusConflict {
		t.Fatalf("expected 409 in wait mode too, got %d", third.Code)
	}

	close(block)
	controller.ingestionService.WaitIdle(context.Background())
}
