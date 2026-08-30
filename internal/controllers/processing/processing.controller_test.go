package processing

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"event/ingestion-service/internal/config"
	"event/ingestion-service/internal/providers/claude"
	"event/ingestion-service/internal/providers/geocode"
	"event/ingestion-service/internal/repositories/event"
	"event/ingestion-service/internal/repositories/igrawpost"
	"event/ingestion-service/internal/repositories/processingrun"
	"event/ingestion-service/internal/repositories/venue"
	processingservice "event/ingestion-service/internal/services/processing"
)

const testAdminToken = "0123456789abcdef0123456789abcdef"

func testConfig() *config.Configurations {
	return &config.Configurations{
		LLM: config.LLMConfigurations{
			Model:        "claude-haiku-4-5",
			MaxTokens:    500,
			PostLimit:    5000,
			PollInterval: time.Millisecond,
			PollTimeout:  5 * time.Second,
		},
		Admin: config.AdminConfigurations{APIToken: testAdminToken},
	}
}

// An unauthenticated caller must get nowhere near the trigger. This endpoint
// spends money and is only ever meant to be reachable from a trusted network.
func TestTriggerRunRejectsAMissingToken(t *testing.T) {
	controller := newTestController(t, nil)

	for _, header := range []string{"", "wrong-token"} {
		request := httptest.NewRequest(http.MethodPost, "/admin/runs", nil)
		if header != "" {
			request.Header.Set("X-Admin-Token", header)
		}
		recorder := httptest.NewRecorder()

		controller.TriggerRun(recorder, request)

		if recorder.Code != http.StatusUnauthorized {
			t.Fatalf("token %q: expected 401, got %d", header, recorder.Code)
		}
	}
}

func TestTriggerRunAcceptsAndReturnsARunID(t *testing.T) {
	controller := newTestController(t, nil)

	recorder := httptest.NewRecorder()
	controller.TriggerRun(recorder, authorizedRequest(http.MethodPost, "/admin/runs"))

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d (%s)", recorder.Code, recorder.Body)
	}

	var body startedResponse
	decodeBody(t, recorder, &body)
	if body.RunID == 0 || body.Status != "started" {
		t.Fatalf("unexpected body: %+v", body)
	}
}

// Two triggers in a row while the first run is still going must not start two
// runs over the same posts. The second gets a 409 naming the run in the way.
func TestTriggerRunConflictsWithARunInProgress(t *testing.T) {
	block := make(chan struct{})
	batch := newBlockingBatchClient(block)
	controller := newTestController(t, batch)

	first := httptest.NewRecorder()
	controller.TriggerRun(first, authorizedRequest(http.MethodPost, "/admin/runs"))
	if first.Code != http.StatusAccepted {
		t.Fatalf("expected the first trigger to be accepted, got %d", first.Code)
	}

	var started startedResponse
	decodeBody(t, first, &started)

	// Wait until the run is genuinely in flight before racing it.
	waitFor(t, func() bool { return batch.submitted() })

	second := httptest.NewRecorder()
	controller.TriggerRun(second, authorizedRequest(http.MethodPost, "/admin/runs"))

	if second.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d (%s)", second.Code, second.Body)
	}

	var conflict conflictResponse
	decodeBody(t, second, &conflict)
	if conflict.RunID != started.RunID {
		t.Fatalf("expected the 409 to name run %d, got %d", started.RunID, conflict.RunID)
	}
	if conflict.Error == "" {
		t.Fatal("expected an explanation in the 409 body")
	}

	close(block)
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	controller.processingService.WaitIdle(waitCtx)
}

// GET /admin/runs/{id} must answer straight from processing_runs, so the same
// run id handed out by the trigger is immediately pollable.
func TestGetRunReturnsTheStoredRow(t *testing.T) {
	controller := newTestController(t, nil)

	triggered := httptest.NewRecorder()
	controller.TriggerRun(triggered, authorizedRequest(http.MethodPost, "/admin/runs"))

	var started startedResponse
	decodeBody(t, triggered, &started)

	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	controller.processingService.WaitIdle(waitCtx)

	// The route is registered with a {id} wildcard, so drive it through a mux
	// rather than setting PathValue by hand — that is what production does.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/runs/{id}", controller.GetRun)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, authorizedRequest(http.MethodGet, "/admin/runs/1"))

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", recorder.Code, recorder.Body)
	}

	var run processingrun.ProcessingRun
	decodeBody(t, recorder, &run)
	if run.ID != started.RunID {
		t.Fatalf("expected run %d, got %d", started.RunID, run.ID)
	}
	if run.Status != processingrun.StatusCompleted {
		t.Fatalf("expected the finished run to report completed, got %q", run.Status)
	}
}

func TestGetRunReportsAMissingRun(t *testing.T) {
	controller := newTestController(t, nil)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/runs/{id}", controller.GetRun)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, authorizedRequest(http.MethodGet, "/admin/runs/999"))

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", recorder.Code)
	}
}

func TestGetRunRejectsANonNumericID(t *testing.T) {
	controller := newTestController(t, nil)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/runs/{id}", controller.GetRun)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, authorizedRequest(http.MethodGet, "/admin/runs/latest"))

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", recorder.Code)
	}
}

// ------------------------------------------------------------------- helpers

func authorizedRequest(method, target string) *http.Request {
	request := httptest.NewRequest(method, target, nil)
	request.Header.Set("X-Admin-Token", testAdminToken)
	return request
}

func decodeBody(t *testing.T, recorder *httptest.ResponseRecorder, into any) {
	t.Helper()
	if err := json.NewDecoder(recorder.Body).Decode(into); err != nil {
		t.Fatalf("could not decode the response body: %v", err)
	}
}

func newTestController(t *testing.T, batch claude.BatchClient) *Controller {
	t.Helper()

	if batch == nil {
		batch = &emptyBatchClient{}
	}

	service := processingservice.NewService(
		testConfig(),
		&stubIgRawPostRepo{},
		&stubEventRepo{},
		&stubVenueRepo{},
		newStubRunRepo(),
		batch,
		&stubGeocoder{},
	)
	return NewController(context.Background(), testConfig(), service)
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for condition")
}

// --------------------------------------------------------------------- stubs

// emptyBatchClient stands in for a queue with nothing in it: the run opens,
// finds no pending post and closes without touching the API.
type emptyBatchClient struct{}

func (emptyBatchClient) Submit(context.Context, []claude.ExtractionRequest) (string, error) {
	return "", nil
}
func (emptyBatchClient) Poll(context.Context, string) (claude.BatchStatus, error) {
	return claude.BatchStatus{Ended: true}, nil
}
func (emptyBatchClient) FetchResults(context.Context, string) ([]claude.ExtractionResult, error) {
	return nil, nil
}

// blockingBatchClient holds a run open so a second trigger has something to
// collide with. Both channels are created up front and signalled with
// sync.Once, so the test goroutine and the run goroutine never touch a field
// without a happens-before edge between them.
type blockingBatchClient struct {
	emptyBatchClient
	block      chan struct{}
	done       chan struct{}
	signalOnce sync.Once
}

func newBlockingBatchClient(block chan struct{}) *blockingBatchClient {
	return &blockingBatchClient{block: block, done: make(chan struct{})}
}

func (b *blockingBatchClient) Submit(context.Context, []claude.ExtractionRequest) (string, error) {
	b.signalOnce.Do(func() { close(b.done) })
	return "msgbatch_test", nil
}

func (b *blockingBatchClient) Poll(context.Context, string) (claude.BatchStatus, error) {
	<-b.block
	return claude.BatchStatus{Ended: true}, nil
}

func (b *blockingBatchClient) submitted() bool {
	select {
	case <-b.done:
		return true
	default:
		return false
	}
}

// stubIgRawPostRepo hands out one pending post so a triggered run has work to
// do, which is what keeps blockingBatchClient in play.
type stubIgRawPostRepo struct{}

func (stubIgRawPostRepo) ListUnprocessed(_ context.Context, _ int) ([]igrawpost.UnprocessedPost, error) {
	return []igrawpost.UnprocessedPost{{ID: 1, PostedAt: time.Now(), Caption: "caption"}}, nil
}
func (stubIgRawPostRepo) MarkProcessed(context.Context, int64) error { return nil }

type stubEventRepo struct{}

func (stubEventRepo) Upsert(context.Context, event.UpsertEvent) error { return nil }
func (stubEventRepo) SetVenue(context.Context, int64, int64) error    { return nil }

type stubVenueRepo struct{}

func (stubVenueRepo) FindByNormalizedName(context.Context, string) (*venue.Venue, error) {
	return nil, nil
}
func (stubVenueRepo) Create(context.Context, venue.CreateVenue) (int64, error) { return 1, nil }

type stubGeocoder struct{}

func (stubGeocoder) Geocode(context.Context, string) (*geocode.Coordinates, error) {
	return nil, nil
}

// stubRunRepo is an in-memory processing_runs table. It is written by the
// background run goroutine and read by the test, so it needs the lock a real
// database would give it for free.
type stubRunRepo struct {
	mu     sync.Mutex
	nextID int64
	runs   map[int64]*processingrun.ProcessingRun
}

func newStubRunRepo() *stubRunRepo {
	return &stubRunRepo{nextID: 1, runs: map[int64]*processingrun.ProcessingRun{}}
}

func (s *stubRunRepo) Create(_ context.Context, status string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := s.nextID
	s.nextID++
	s.runs[id] = &processingrun.ProcessingRun{ID: id, Status: status, StartedAt: time.Now()}
	return id, nil
}

func (s *stubRunRepo) SetBatch(_ context.Context, id int64, batchID string, postsSubmitted int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	run := s.runs[id]
	run.BatchID = &batchID
	run.PostsSubmitted = postsSubmitted
	run.Status = processingrun.StatusPolling
	return nil
}

func (s *stubRunRepo) Finish(_ context.Context, request processingrun.FinishProcessingRun) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	run := s.runs[request.ID]
	run.Status = request.Status
	run.PostsSubmitted = request.PostsSubmitted
	run.PostsSucceeded = request.PostsSucceeded
	run.PostsFailed = request.PostsFailed
	run.EventsGeocoded = request.EventsGeocoded
	run.Error = request.Error
	now := time.Now()
	run.FinishedAt = &now
	return nil
}

func (s *stubRunRepo) Get(_ context.Context, id int64) (*processingrun.ProcessingRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.runs[id], nil
}
