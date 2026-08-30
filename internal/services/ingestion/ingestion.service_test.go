package ingestion

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"event/ingestion-service/internal/config"
	"event/ingestion-service/internal/providers/instagram"
	"event/ingestion-service/internal/repositories/igrawpost"
	"event/ingestion-service/internal/repositories/igsource"
	"event/ingestion-service/internal/repositories/ingestionrun"
)

// --- fakes ------------------------------------------------------------------

type fakeIgSourceRepo struct {
	mu          sync.Mutex
	sources     []igsource.IgSource
	listErr     error
	synced      []int64
	failed      []igsource.MarkFailedIgSource
	deactivated []igsource.DeactivateIgSource
	// failureCount is what MarkFailed reports back as consecutive_failures.
	failureCount int
}

func (f *fakeIgSourceRepo) ListActive(ctx context.Context) ([]igsource.IgSource, error) {
	return f.sources, f.listErr
}

func (f *fakeIgSourceRepo) MarkSynced(ctx context.Context, id int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.synced = append(f.synced, id)
	return nil
}

func (f *fakeIgSourceRepo) MarkFailed(ctx context.Context, request igsource.MarkFailedIgSource) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed = append(f.failed, request)
	if f.failureCount > 0 {
		return f.failureCount, nil
	}
	return len(f.failed), nil
}

func (f *fakeIgSourceRepo) Deactivate(ctx context.Context, request igsource.DeactivateIgSource) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deactivated = append(f.deactivated, request)
	return nil
}

func (f *fakeIgSourceRepo) deactivatedIDs() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make([]int64, 0, len(f.deactivated))
	for _, d := range f.deactivated {
		ids = append(ids, d.ID)
	}
	return ids
}

type fakeIgRawPostRepo struct {
	mu sync.Mutex
	// inserted decides whether an upsert reports a new row.
	inserted map[string]bool
	upserts  []igrawpost.UpsertIgRawPost
	err      error
}

func (f *fakeIgRawPostRepo) Upsert(ctx context.Context, request igrawpost.UpsertIgRawPost) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return false, f.err
	}
	f.upserts = append(f.upserts, request)
	return f.inserted[request.IgMediaID], nil
}

type fakeIngestionRunRepo struct {
	mu       sync.Mutex
	createID int64
	finished []ingestionrun.FinishIngestionRun
}

func (f *fakeIngestionRunRepo) Create(ctx context.Context, sourceKind string) (int64, error) {
	if f.createID == 0 {
		f.createID = 42
	}
	return f.createID, nil
}

func (f *fakeIngestionRunRepo) Finish(ctx context.Context, request ingestionrun.FinishIngestionRun) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.finished = append(f.finished, request)
	return nil
}

func (f *fakeIngestionRunRepo) last() ingestionrun.FinishIngestionRun {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.finished[len(f.finished)-1]
}

type fakeInstagram struct {
	mu       sync.Mutex
	results  map[string]*instagram.DiscoveryResult
	errs     map[string]error
	calls    map[string]int
	tokenErr error
}

func (f *fakeInstagram) DiscoverMedia(ctx context.Context, username string, limit int) (*instagram.DiscoveryResult, error) {
	f.mu.Lock()
	if f.calls == nil {
		f.calls = map[string]int{}
	}
	f.calls[username]++
	err := f.errs[username]
	result := f.results[username]
	f.mu.Unlock()

	if err != nil {
		return nil, err
	}
	return result, nil
}

func (f *fakeInstagram) VerifyToken(ctx context.Context) error { return f.tokenErr }

func (f *fakeInstagram) callCount(username string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[username]
}

type fakeNotifier struct {
	mu       sync.Mutex
	messages []string
}

func (f *fakeNotifier) Notify(ctx context.Context, message string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.messages = append(f.messages, message)
	return nil
}

func (f *fakeNotifier) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.messages)
}

// --- helpers ----------------------------------------------------------------

func testConfig() *config.Configurations {
	return &config.Configurations{
		Instagram:     config.InstagramConfigurations{MediaLimit: 25},
		IngestionCron: config.IngestionCronConfigurations{WorkerConcurrency: 3},
	}
}

func media(id string) instagram.Media {
	return instagram.Media{
		ID:        id,
		Caption:   "งานเปิดวันเสาร์นี้",
		MediaType: "IMAGE",
		Permalink: "https://www.instagram.com/p/" + id + "/",
		Timestamp: time.Date(2026, 8, 29, 10, 0, 0, 0, time.UTC),
		Raw:       json.RawMessage(`{"id":"` + id + `"}`),
	}
}

func source(id int64, username string) igsource.IgSource {
	return igsource.IgSource{ID: id, Username: username, IsActive: true}
}

func init() {
	// Keep retry backoff out of the test runtime.
	retryBaseDelay = time.Millisecond
}

// --- tests ------------------------------------------------------------------

func TestRunCountsNewAndUpdatedPosts(t *testing.T) {
	sourceRepo := &fakeIgSourceRepo{sources: []igsource.IgSource{source(1, "bacc_bangkok")}}
	postRepo := &fakeIgRawPostRepo{inserted: map[string]bool{"m1": true, "m2": false}}
	runRepo := &fakeIngestionRunRepo{}
	ig := &fakeInstagram{results: map[string]*instagram.DiscoveryResult{
		"bacc_bangkok": {Username: "bacc_bangkok", Media: []instagram.Media{media("m1"), media("m2")}},
	}}

	service := NewService(testConfig(), sourceRepo, postRepo, runRepo, ig, &fakeNotifier{})
	summary, err := service.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if summary.Status != StatusSuccess {
		t.Fatalf("expected status success, got %q", summary.Status)
	}
	if summary.PostsNew != 1 || summary.PostsUpdated != 1 {
		t.Fatalf("expected 1 new / 1 updated, got %d / %d", summary.PostsNew, summary.PostsUpdated)
	}
	if summary.SourcesOK != 1 || summary.SourcesFailed != 0 {
		t.Fatalf("expected 1 ok / 0 failed, got %d / %d", summary.SourcesOK, summary.SourcesFailed)
	}
	if len(sourceRepo.synced) != 1 {
		t.Fatalf("expected the source to be marked synced, got %v", sourceRepo.synced)
	}

	// The run row must carry the same counters.
	finished := runRepo.last()
	if finished.Status != StatusSuccess || finished.PostsNew != 1 || finished.PostsUpdated != 1 || finished.SourcesTotal != 1 {
		t.Fatalf("ingestion_runs row does not match the summary: %+v", finished)
	}
}

func TestRunIsIdempotentOnASecondPass(t *testing.T) {
	sourceRepo := &fakeIgSourceRepo{sources: []igsource.IgSource{source(1, "museumsiam")}}
	runRepo := &fakeIngestionRunRepo{}
	ig := &fakeInstagram{results: map[string]*instagram.DiscoveryResult{
		"museumsiam": {Username: "museumsiam", Media: []instagram.Media{media("m1"), media("m2")}},
	}}

	// First pass: both rows are new. Second pass: the unique index makes both updates.
	firstPass := &fakeIgRawPostRepo{inserted: map[string]bool{"m1": true, "m2": true}}
	service := NewService(testConfig(), sourceRepo, firstPass, runRepo, ig, &fakeNotifier{})
	first, _ := service.Run(context.Background())

	secondPass := &fakeIgRawPostRepo{inserted: map[string]bool{}}
	service = NewService(testConfig(), sourceRepo, secondPass, runRepo, ig, &fakeNotifier{})
	second, _ := service.Run(context.Background())

	if first.PostsNew != 2 {
		t.Fatalf("expected 2 new posts on the first run, got %d", first.PostsNew)
	}
	if second.PostsNew != 0 || second.PostsUpdated != 2 {
		t.Fatalf("expected 0 new / 2 updated on the second run, got %d / %d", second.PostsNew, second.PostsUpdated)
	}
}

func TestRunAlwaysPersistsTheEditedCaption(t *testing.T) {
	sourceRepo := &fakeIgSourceRepo{sources: []igsource.IgSource{source(1, "museumsiam")}}
	postRepo := &fakeIgRawPostRepo{inserted: map[string]bool{}}
	runRepo := &fakeIngestionRunRepo{}

	edited := media("m1")
	edited.Caption = "เลื่อนเป็นวันอาทิตย์"
	ig := &fakeInstagram{results: map[string]*instagram.DiscoveryResult{
		"museumsiam": {Username: "museumsiam", Media: []instagram.Media{edited}},
	}}

	service := NewService(testConfig(), sourceRepo, postRepo, runRepo, ig, &fakeNotifier{})
	summary, _ := service.Run(context.Background())

	if summary.PostsNew != 0 || summary.PostsUpdated != 1 {
		t.Fatalf("an edited caption must count as an update, got %d new / %d updated", summary.PostsNew, summary.PostsUpdated)
	}
	if postRepo.upserts[0].Caption == nil || *postRepo.upserts[0].Caption != "เลื่อนเป็นวันอาทิตย์" {
		t.Fatalf("the edited caption was not written: %+v", postRepo.upserts[0].Caption)
	}
}

func TestRunDeactivatesAPermanentlyBrokenSourceWithoutFailingTheRun(t *testing.T) {
	sourceRepo := &fakeIgSourceRepo{sources: []igsource.IgSource{
		source(1, "personal_account"),
		source(2, "bacc_bangkok"),
	}}
	postRepo := &fakeIgRawPostRepo{inserted: map[string]bool{"m1": true}}
	runRepo := &fakeIngestionRunRepo{}
	ig := &fakeInstagram{
		errs: map[string]error{
			"personal_account": &instagram.APIError{Kind: instagram.ErrPermanent, Code: 110, Message: "not a business account"},
		},
		results: map[string]*instagram.DiscoveryResult{
			"bacc_bangkok": {Username: "bacc_bangkok", Media: []instagram.Media{media("m1")}},
		},
	}

	service := NewService(testConfig(), sourceRepo, postRepo, runRepo, ig, &fakeNotifier{})
	summary, err := service.Run(context.Background())
	if err != nil {
		t.Fatalf("a permanent source failure must not fail the run: %v", err)
	}

	if summary.Status != StatusPartial {
		t.Fatalf("expected status partial, got %q", summary.Status)
	}
	if summary.SourcesOK != 1 || summary.SourcesFailed != 1 {
		t.Fatalf("expected 1 ok / 1 failed, got %d / %d", summary.SourcesOK, summary.SourcesFailed)
	}

	deactivated := sourceRepo.deactivatedIDs()
	if len(deactivated) != 1 || deactivated[0] != 1 {
		t.Fatalf("expected only the broken source to be deactivated, got %v", deactivated)
	}
	// A permanent error is not worth retrying.
	if calls := ig.callCount("personal_account"); calls != 1 {
		t.Fatalf("expected exactly 1 attempt for a permanent error, got %d", calls)
	}
}

func TestRunRetriesTransientFailuresThenCountsTheSourceFailed(t *testing.T) {
	sourceRepo := &fakeIgSourceRepo{sources: []igsource.IgSource{source(1, "flaky_venue")}}
	runRepo := &fakeIngestionRunRepo{}
	ig := &fakeInstagram{errs: map[string]error{
		"flaky_venue": &instagram.APIError{Kind: instagram.ErrTransient, Code: 2, Message: "temporary outage"},
	}}

	service := NewService(testConfig(), sourceRepo, &fakeIgRawPostRepo{}, runRepo, ig, &fakeNotifier{})
	summary, err := service.Run(context.Background())
	if err != nil {
		t.Fatalf("a transient source failure must not fail the run: %v", err)
	}

	if want := maxRetries + 1; ig.callCount("flaky_venue") != want {
		t.Fatalf("expected %d attempts, got %d", want, ig.callCount("flaky_venue"))
	}
	if summary.SourcesFailed != 1 || summary.Status != StatusFailed {
		t.Fatalf("the only source failed, expected status failed with 1 failure, got %q / %d", summary.Status, summary.SourcesFailed)
	}
	if len(sourceRepo.failed) != 1 {
		t.Fatalf("expected consecutive_failures to be bumped once, got %d", len(sourceRepo.failed))
	}
	if len(sourceRepo.deactivatedIDs()) != 0 {
		t.Fatal("a single transient failure must not deactivate the source")
	}
}

func TestRunAutoDeactivatesAfterTooManyConsecutiveFailures(t *testing.T) {
	sourceRepo := &fakeIgSourceRepo{
		sources:      []igsource.IgSource{source(1, "dead_venue")},
		failureCount: autoDeactivateThreshold,
	}
	runRepo := &fakeIngestionRunRepo{}
	ig := &fakeInstagram{errs: map[string]error{
		"dead_venue": &instagram.APIError{Kind: instagram.ErrTransient, Code: 1, Message: "unknown error"},
	}}

	service := NewService(testConfig(), sourceRepo, &fakeIgRawPostRepo{}, runRepo, ig, &fakeNotifier{})
	if _, err := service.Run(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	deactivated := sourceRepo.deactivatedIDs()
	if len(deactivated) != 1 || deactivated[0] != 1 {
		t.Fatalf("expected the source to be auto-deactivated at %d failures, got %v", autoDeactivateThreshold, deactivated)
	}
}

func TestRunAbortsEverythingOnAFatalError(t *testing.T) {
	sources := make([]igsource.IgSource, 0, 30)
	for i := int64(1); i <= 30; i++ {
		sources = append(sources, source(i, "venue"))
	}
	sourceRepo := &fakeIgSourceRepo{sources: sources}
	runRepo := &fakeIngestionRunRepo{}
	notifier := &fakeNotifier{}
	ig := &fakeInstagram{errs: map[string]error{
		// Rate limiting: continuing would only lengthen the cooldown.
		"venue": &instagram.APIError{Kind: instagram.ErrFatal, Code: 4, Message: "Application request limit reached"},
	}}

	service := NewService(testConfig(), sourceRepo, &fakeIgRawPostRepo{}, runRepo, ig, notifier)

	summary, err := service.Run(context.Background())
	if err == nil {
		t.Fatal("expected the fatal error to be returned")
	}
	if summary.Status != StatusFailed {
		t.Fatalf("expected status failed, got %q", summary.Status)
	}
	if runRepo.last().Error == nil {
		t.Fatal("expected the fatal error to be recorded on the run row")
	}
	// The pool must stop early rather than burn a call on all 30 sources.
	if calls := ig.callCount("venue"); calls >= len(sources) {
		t.Fatalf("expected the run to abort early, but %d of %d sources were called", calls, len(sources))
	}
	if notifier.count() == 0 {
		t.Fatal("a fatal error must raise an alert, not just a log line")
	}
}

func TestRunAlertsOnAnExpiredToken(t *testing.T) {
	sourceRepo := &fakeIgSourceRepo{sources: []igsource.IgSource{source(1, "bacc_bangkok")}}
	runRepo := &fakeIngestionRunRepo{}
	notifier := &fakeNotifier{}
	ig := &fakeInstagram{errs: map[string]error{
		"bacc_bangkok": &instagram.APIError{Kind: instagram.ErrFatal, Code: 190, Message: "Session has expired"},
	}}

	service := NewService(testConfig(), sourceRepo, &fakeIgRawPostRepo{}, runRepo, ig, notifier)
	if _, err := service.Run(context.Background()); err == nil {
		t.Fatal("expected the fatal error to be returned")
	}

	if notifier.count() != 1 {
		t.Fatalf("expected exactly one alert, got %d", notifier.count())
	}
	if !strings.Contains(notifier.messages[0], "OAuth") {
		t.Fatalf("an expired token alert must say a new OAuth flow is needed, got %q", notifier.messages[0])
	}
}

func TestRunSkipsAnOverlappingInvocation(t *testing.T) {
	release := make(chan struct{})
	sourceRepo := &fakeIgSourceRepo{sources: []igsource.IgSource{source(1, "slow_venue")}}
	runRepo := &fakeIngestionRunRepo{}
	ig := &blockingInstagram{release: release}

	service := NewService(testConfig(), sourceRepo, &fakeIgRawPostRepo{}, runRepo, ig, &fakeNotifier{})

	started := make(chan struct{})
	go func() {
		close(started)
		_, _ = service.Run(context.Background())
	}()
	<-started
	// Give the first run time to take the lock.
	time.Sleep(50 * time.Millisecond)

	summary, err := service.Run(context.Background())
	if err != nil || summary != nil {
		t.Fatalf("an overlapping run must be skipped, got summary=%v err=%v", summary, err)
	}
	close(release)
}

type blockingInstagram struct {
	release chan struct{}
}

func (b *blockingInstagram) DiscoverMedia(ctx context.Context, username string, limit int) (*instagram.DiscoveryResult, error) {
	<-b.release
	return &instagram.DiscoveryResult{Username: username}, nil
}

func (b *blockingInstagram) VerifyToken(ctx context.Context) error { return nil }
