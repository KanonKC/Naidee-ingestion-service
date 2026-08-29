package processing

import (
	"context"
	"fmt"
	"sync"
	"time"

	"event/ingestion-service/internal/processing/config"
	"event/ingestion-service/internal/processing/providers/claude"
	"event/ingestion-service/internal/processing/providers/geocode"
	"event/ingestion-service/internal/processing/repositories/event"
	"event/ingestion-service/internal/processing/repositories/igrawpost"
	"event/ingestion-service/internal/processing/repositories/processingrun"
	"event/ingestion-service/internal/processing/repositories/venue"
)

func testConfig() *config.Configurations {
	return &config.Configurations{
		LLM: config.LLMConfigurations{
			Model:        "claude-haiku-4-5",
			MaxTokens:    500,
			PostLimit:    5000,
			PollInterval: time.Millisecond,
			PollTimeout:  time.Second,
		},
		Geocode: config.GeocodeConfigurations{MinInterval: 0},
	}
}

// ---------------------------------------------------------------- raw posts

type fakeIgRawPostRepo struct {
	mu        sync.Mutex
	posts     []igrawpost.UnprocessedPost
	processed map[int64]bool
	listErr   error
}

func newFakeIgRawPostRepo(posts ...igrawpost.UnprocessedPost) *fakeIgRawPostRepo {
	return &fakeIgRawPostRepo{posts: posts, processed: map[int64]bool{}}
}

func (f *fakeIgRawPostRepo) ListUnprocessed(_ context.Context, limit int) ([]igrawpost.UnprocessedPost, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	pending := make([]igrawpost.UnprocessedPost, 0, len(f.posts))
	for _, post := range f.posts {
		if !f.processed[post.ID] && len(pending) < limit {
			pending = append(pending, post)
		}
	}
	return pending, nil
}

func (f *fakeIgRawPostRepo) MarkProcessed(_ context.Context, id int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.processed[id] = true
	return nil
}

func (f *fakeIgRawPostRepo) isProcessed(id int64) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.processed[id]
}

// -------------------------------------------------------------------- events

type fakeEventRepo struct {
	mu      sync.Mutex
	upserts map[int64]event.UpsertEvent
	venues  map[int64]int64
}

func newFakeEventRepo() *fakeEventRepo {
	return &fakeEventRepo{upserts: map[int64]event.UpsertEvent{}, venues: map[int64]int64{}}
}

func (f *fakeEventRepo) Upsert(_ context.Context, request event.UpsertEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.upserts[request.RawPostID] = request
	return nil
}

func (f *fakeEventRepo) SetVenue(_ context.Context, rawPostID int64, venueID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.venues[rawPostID] = venueID
	return nil
}

func (f *fakeEventRepo) get(rawPostID int64) (event.UpsertEvent, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	stored, ok := f.upserts[rawPostID]
	return stored, ok
}

// -------------------------------------------------------------------- venues

type fakeVenueRepo struct {
	mu      sync.Mutex
	byName  map[string]*venue.Venue
	nextID  int64
	creates []venue.CreateVenue
}

func newFakeVenueRepo() *fakeVenueRepo {
	return &fakeVenueRepo{byName: map[string]*venue.Venue{}, nextID: 1}
}

func (f *fakeVenueRepo) FindByNormalizedName(_ context.Context, nameNormalized string) (*venue.Venue, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.byName[nameNormalized], nil
}

func (f *fakeVenueRepo) Create(_ context.Context, request venue.CreateVenue) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if existing, ok := f.byName[request.NameNormalized]; ok {
		return existing.ID, nil
	}

	id := f.nextID
	f.nextID++
	f.creates = append(f.creates, request)
	f.byName[request.NameNormalized] = &venue.Venue{
		ID:             id,
		Name:           request.Name,
		NameNormalized: request.NameNormalized,
		Lat:            request.Lat,
		Lng:            request.Lng,
		GeocodedAt:     request.GeocodedAt,
	}
	return id, nil
}

// ---------------------------------------------------------------------- runs

type fakeProcessingRunRepo struct {
	mu       sync.Mutex
	nextID   int64
	runs     map[int64]*processingrun.ProcessingRun
	finished []processingrun.FinishProcessingRun
}

func newFakeProcessingRunRepo() *fakeProcessingRunRepo {
	return &fakeProcessingRunRepo{nextID: 1, runs: map[int64]*processingrun.ProcessingRun{}}
}

func (f *fakeProcessingRunRepo) Create(_ context.Context, status string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	id := f.nextID
	f.nextID++
	f.runs[id] = &processingrun.ProcessingRun{ID: id, Status: status, StartedAt: time.Now()}
	return id, nil
}

func (f *fakeProcessingRunRepo) SetBatch(_ context.Context, id int64, batchID string, postsSubmitted int) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	run := f.runs[id]
	run.BatchID = &batchID
	run.PostsSubmitted = postsSubmitted
	run.Status = processingrun.StatusPolling
	return nil
}

func (f *fakeProcessingRunRepo) Finish(_ context.Context, request processingrun.FinishProcessingRun) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.finished = append(f.finished, request)
	run := f.runs[request.ID]
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

func (f *fakeProcessingRunRepo) Get(_ context.Context, id int64) (*processingrun.ProcessingRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.runs[id], nil
}

func (f *fakeProcessingRunRepo) last() processingrun.FinishProcessingRun {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.finished) == 0 {
		return processingrun.FinishProcessingRun{}
	}
	return f.finished[len(f.finished)-1]
}

// --------------------------------------------------------------- batch client

type fakeBatchClient struct {
	mu sync.Mutex

	results   []claude.ExtractionResult
	submitErr error

	// pollsBeforeEnd delays the "ended" answer, so the poll loop is genuinely
	// exercised rather than short-circuited on the first tick.
	pollsBeforeEnd int
	polls          int
	neverEnds      bool

	submitted [][]claude.ExtractionRequest
	// block, when set, holds Submit until it is closed. Used to keep a run in
	// flight while a second one tries to start.
	block chan struct{}
}

func (f *fakeBatchClient) Submit(_ context.Context, reqs []claude.ExtractionRequest) (string, error) {
	f.mu.Lock()
	f.submitted = append(f.submitted, reqs)
	block := f.block
	f.mu.Unlock()

	if block != nil {
		<-block
	}
	if f.submitErr != nil {
		return "", f.submitErr
	}
	return "msgbatch_test", nil
}

func (f *fakeBatchClient) Poll(_ context.Context, _ string) (claude.BatchStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.polls++
	if f.neverEnds {
		return claude.BatchStatus{Ended: false}, nil
	}
	return claude.BatchStatus{Ended: f.polls > f.pollsBeforeEnd}, nil
}

func (f *fakeBatchClient) FetchResults(_ context.Context, _ string) ([]claude.ExtractionResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.results, nil
}

func (f *fakeBatchClient) submitCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.submitted)
}

// ------------------------------------------------------------------ geocoder

type fakeGeocoder struct {
	mu     sync.Mutex
	calls  []string
	result *geocode.Coordinates
	err    error
}

func (f *fakeGeocoder) Geocode(_ context.Context, address string) (*geocode.Coordinates, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, address)
	return f.result, f.err
}

func (f *fakeGeocoder) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// ------------------------------------------------------------------- helpers

func stringPtr(value string) *string { return &value }

// newTestService takes the repository interfaces rather than the fakes, so a
// test can swap in a deliberately broken one without a second constructor.
func newTestService(
	postRepo IgRawPostRepository,
	eventRepo EventRepository,
	venueRepo VenueRepository,
	runRepo ProcessingRunRepository,
	batch claude.BatchClient,
	geocoder geocode.Geocoder,
) *Service {
	return NewService(testConfig(), postRepo, eventRepo, venueRepo, runRepo, batch, geocoder)
}

func samplePost(id int64) igrawpost.UnprocessedPost {
	return igrawpost.UnprocessedPost{
		ID:       id,
		PostedAt: time.Date(2026, 8, 20, 10, 0, 0, 0, time.UTC),
		Caption:  fmt.Sprintf("caption for post %d", id),
	}
}
