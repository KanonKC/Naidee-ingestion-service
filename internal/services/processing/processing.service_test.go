package processing

import (
	"context"
	"errors"
	"testing"
	"time"

	"event/ingestion-service/internal/providers/claude"
	"event/ingestion-service/internal/providers/geocode"
	"event/ingestion-service/internal/repositories/processingrun"
)

// An empty queue is the common case — the cron fires every thirty minutes and
// most ticks have nothing to do. Submitting an empty batch would cost an API
// call and a batch id for no reason at all.
func TestRunWithNoPendingPostsSubmitsNothing(t *testing.T) {
	postRepo := newFakeIgRawPostRepo()
	runRepo := newFakeProcessingRunRepo()
	batch := &fakeBatchClient{}

	service := newTestService(postRepo, newFakeEventRepo(), newFakeVenueRepo(), runRepo, batch, &fakeGeocoder{})

	summary, err := service.Run(context.Background())
	if err != nil {
		t.Fatalf("an empty queue is not an error: %v", err)
	}

	if batch.submitCount() != 0 {
		t.Fatal("expected no batch to be submitted")
	}
	if summary.Status != processingrun.StatusCompleted {
		t.Fatalf("expected status completed, got %q", summary.Status)
	}
	if summary.PostsSubmitted != 0 {
		t.Fatalf("expected 0 posts submitted, got %d", summary.PostsSubmitted)
	}

	// The run must still be recorded — "nothing to do" is an outcome worth
	// seeing in the audit log, not a silent no-op.
	if finished := runRepo.last(); finished.Status != processingrun.StatusCompleted {
		t.Fatalf("processing_runs row does not match: %+v", finished)
	}
}

// The happy path: a caption that clearly describes an event lands as an event
// row, with the venue geocoded and the post marked processed.
func TestRunStoresEventAndGeocodesVenue(t *testing.T) {
	postRepo := newFakeIgRawPostRepo(samplePost(1))
	eventRepo := newFakeEventRepo()
	venueRepo := newFakeVenueRepo()
	runRepo := newFakeProcessingRunRepo()
	geocoder := &fakeGeocoder{result: &geocode.Coordinates{Lat: 13.7466, Lng: 100.5316, DisplayName: "BACC, Bangkok"}}

	start := time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
	batch := &fakeBatchClient{
		pollsBeforeEnd: 2,
		results: []claude.ExtractionResult{{
			RawPostID:  1,
			IsEvent:    true,
			Confidence: claude.ConfidenceHigh,
			Title:      stringPtr("นิทรรศการภาพถ่าย"),
			VenueName:  stringPtr("BACC"),
			StartDate:  &start,
		}},
	}

	service := newTestService(postRepo, eventRepo, venueRepo, runRepo, batch, geocoder)

	summary, err := service.Run(context.Background())
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}

	stored, ok := eventRepo.get(1)
	if !ok {
		t.Fatal("expected an events row for post 1")
	}
	if !stored.IsEvent || stored.Confidence != claude.ConfidenceHigh {
		t.Fatalf("event does not match the extraction: %+v", stored)
	}
	if eventRepo.venues[1] == 0 {
		t.Fatal("expected the event to be linked to a venue")
	}
	if !postRepo.isProcessed(1) {
		t.Fatal("expected the post to be marked processed")
	}
	if summary.PostsSucceeded != 1 || summary.PostsFailed != 0 || summary.EventsGeocoded != 1 {
		t.Fatalf("counters do not match: %+v", summary)
	}
	// The poll loop must actually have waited rather than assuming the batch
	// was ready on the first look.
	if batch.polls < 3 {
		t.Fatalf("expected the poll loop to run until the batch ended, got %d polls", batch.polls)
	}
}

// A post that is not about an event still gets a row — that is how we remember
// not to look at it again — but it must never cost a geocoding call.
func TestRunDoesNotGeocodeNonEvents(t *testing.T) {
	postRepo := newFakeIgRawPostRepo(samplePost(1))
	eventRepo := newFakeEventRepo()
	geocoder := &fakeGeocoder{}

	batch := &fakeBatchClient{
		results: []claude.ExtractionResult{{
			RawPostID:  1,
			IsEvent:    false,
			Confidence: claude.ConfidenceHigh,
		}},
	}

	service := newTestService(postRepo, eventRepo, newFakeVenueRepo(), newFakeProcessingRunRepo(), batch, geocoder)

	if _, err := service.Run(context.Background()); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	stored, ok := eventRepo.get(1)
	if !ok {
		t.Fatal("expected a row even for a non-event")
	}
	if stored.IsEvent {
		t.Fatal("expected is_event false")
	}
	if geocoder.callCount() != 0 {
		t.Fatalf("expected no geocoding for a non-event, got %d calls", geocoder.callCount())
	}
	if !postRepo.isProcessed(1) {
		t.Fatal("a non-event is still processed — it must not be retried forever")
	}
}

// The same venue seen in two posts must resolve to one row and cost exactly one
// geocode call. Without this, a popular venue would be re-geocoded on every
// post that mentions it and would blow through Nominatim's usage policy.
func TestRunReusesVenueAcrossPosts(t *testing.T) {
	postRepo := newFakeIgRawPostRepo(samplePost(1), samplePost(2))
	eventRepo := newFakeEventRepo()
	venueRepo := newFakeVenueRepo()
	geocoder := &fakeGeocoder{result: &geocode.Coordinates{Lat: 13.7466, Lng: 100.5316}}

	batch := &fakeBatchClient{
		results: []claude.ExtractionResult{
			{RawPostID: 1, IsEvent: true, Confidence: claude.ConfidenceHigh, VenueName: stringPtr("BACC")},
			// Same venue, written differently. Normalisation is what makes
			// these one row rather than two.
			{RawPostID: 2, IsEvent: true, Confidence: claude.ConfidenceMedium, VenueName: stringPtr("  bacc.  ")},
		},
	}

	service := newTestService(postRepo, eventRepo, venueRepo, newFakeProcessingRunRepo(), batch, geocoder)

	summary, err := service.Run(context.Background())
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}

	if geocoder.callCount() != 1 {
		t.Fatalf("expected exactly one geocode call, got %d", geocoder.callCount())
	}
	if len(venueRepo.creates) != 1 {
		t.Fatalf("expected exactly one venue row, got %d", len(venueRepo.creates))
	}
	if eventRepo.venues[1] != eventRepo.venues[2] {
		t.Fatalf("expected both events on the same venue, got %d and %d", eventRepo.venues[1], eventRepo.venues[2])
	}
	if summary.EventsGeocoded != 1 {
		t.Fatalf("expected events_geocoded 1, got %d", summary.EventsGeocoded)
	}
}

// One bad item must not cost the batch its other results, and the failed post
// must stay pending so the next run gets another go at it.
func TestRunIsolatesAFailedItem(t *testing.T) {
	postRepo := newFakeIgRawPostRepo(samplePost(1), samplePost(2))
	eventRepo := newFakeEventRepo()
	runRepo := newFakeProcessingRunRepo()

	batch := &fakeBatchClient{
		results: []claude.ExtractionResult{
			{RawPostID: 1, Error: errors.New("invalid json: unexpected end of JSON input")},
			{RawPostID: 2, IsEvent: true, Confidence: claude.ConfidenceMedium},
		},
	}

	service := newTestService(postRepo, eventRepo, newFakeVenueRepo(), runRepo, batch, &fakeGeocoder{})

	summary, err := service.Run(context.Background())
	if err != nil {
		t.Fatalf("a single bad item must not fail the run: %v", err)
	}

	if postRepo.isProcessed(1) {
		t.Fatal("a failed post must stay unprocessed so the next run retries it")
	}
	if !postRepo.isProcessed(2) {
		t.Fatal("the healthy post must still have been processed")
	}
	if _, ok := eventRepo.get(1); ok {
		t.Fatal("a failed extraction must not write an events row")
	}
	if summary.PostsFailed != 1 || summary.PostsSucceeded != 1 {
		t.Fatalf("counters do not match: %+v", summary)
	}
	if finished := runRepo.last(); finished.PostsFailed != 1 {
		t.Fatalf("processing_runs must record the failure: %+v", finished)
	}
}

// A post submitted but missing from the results is a silent data-loss risk: it
// would look processed to nobody and never be retried. It must count as failed.
func TestRunCountsMissingResultsAsFailed(t *testing.T) {
	postRepo := newFakeIgRawPostRepo(samplePost(1), samplePost(2))
	batch := &fakeBatchClient{
		results: []claude.ExtractionResult{{RawPostID: 1, IsEvent: true, Confidence: claude.ConfidenceLow}},
	}

	service := newTestService(postRepo, newFakeEventRepo(), newFakeVenueRepo(), newFakeProcessingRunRepo(), batch, &fakeGeocoder{})

	summary, err := service.Run(context.Background())
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}

	if summary.PostsFailed != 1 {
		t.Fatalf("expected the missing result to count as failed, got %+v", summary)
	}
	if postRepo.isProcessed(2) {
		t.Fatal("a post with no result must stay unprocessed")
	}
}

// A batch that never ends must not hold the run forever. The posts stay
// pending, so the next run resubmits them — nothing is lost.
func TestRunFailsWhenPollTimesOut(t *testing.T) {
	postRepo := newFakeIgRawPostRepo(samplePost(1))
	runRepo := newFakeProcessingRunRepo()
	batch := &fakeBatchClient{neverEnds: true}

	service := newTestService(postRepo, newFakeEventRepo(), newFakeVenueRepo(), runRepo, batch, &fakeGeocoder{})
	service.cfg.LLM.PollTimeout = 10 * time.Millisecond

	summary, err := service.Run(context.Background())
	if err == nil {
		t.Fatal("expected a timeout error")
	}
	if summary.Status != processingrun.StatusFailed {
		t.Fatalf("expected status failed, got %q", summary.Status)
	}
	if postRepo.isProcessed(1) {
		t.Fatal("a timed-out batch must leave its posts unprocessed")
	}
	// The batch id has to survive the failure or the batch cannot be recovered
	// by hand within the 29 days its results live for.
	if run, _ := runRepo.Get(context.Background(), summary.RunID); run.BatchID == nil {
		t.Fatal("expected the batch id to be recorded on a timed-out run")
	}
}

// A submit failure means no post was touched at all, so the whole set is simply
// retried on the next tick.
func TestRunFailsCleanlyWhenSubmitFails(t *testing.T) {
	postRepo := newFakeIgRawPostRepo(samplePost(1))
	runRepo := newFakeProcessingRunRepo()
	batch := &fakeBatchClient{submitErr: errors.New("401 authentication_error")}

	service := newTestService(postRepo, newFakeEventRepo(), newFakeVenueRepo(), runRepo, batch, &fakeGeocoder{})

	summary, err := service.Run(context.Background())
	if err == nil {
		t.Fatal("expected the submit error to surface")
	}
	if summary.Status != processingrun.StatusFailed {
		t.Fatalf("expected status failed, got %q", summary.Status)
	}
	if postRepo.isProcessed(1) {
		t.Fatal("no post may be marked processed when the batch never landed")
	}
}

// The cron fires every thirty minutes but a batch can be polled for two hours.
// Overlapping runs would submit the same posts twice, so the second one must be
// skipped outright.
func TestOverlappingRunIsSkipped(t *testing.T) {
	postRepo := newFakeIgRawPostRepo(samplePost(1))
	batch := &fakeBatchClient{
		block:   make(chan struct{}),
		results: []claude.ExtractionResult{{RawPostID: 1, IsEvent: true, Confidence: claude.ConfidenceLow}},
	}

	service := newTestService(postRepo, newFakeEventRepo(), newFakeVenueRepo(), newFakeProcessingRunRepo(), batch, &fakeGeocoder{})

	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		if _, err := service.Run(context.Background()); err != nil {
			t.Errorf("first run failed: %v", err)
		}
	}()

	// Wait until the first run is genuinely inside Submit before racing it.
	waitFor(t, func() bool { return batch.submitCount() == 1 })

	summary, err := service.Run(context.Background())
	if err != nil {
		t.Fatalf("a skipped run is not an error: %v", err)
	}
	if summary != nil {
		t.Fatalf("expected the overlapping run to be skipped, got %+v", summary)
	}

	close(batch.block)
	<-firstDone

	if batch.submitCount() != 1 {
		t.Fatalf("expected exactly one batch submission, got %d", batch.submitCount())
	}
}

// The manual trigger and the cron share one guard, so a trigger during a run
// has to report the run that is in the way rather than starting a second one.
func TestTriggerAsyncReportsTheRunInProgress(t *testing.T) {
	postRepo := newFakeIgRawPostRepo(samplePost(1))
	batch := &fakeBatchClient{
		block:   make(chan struct{}),
		results: []claude.ExtractionResult{{RawPostID: 1, IsEvent: true, Confidence: claude.ConfidenceLow}},
	}

	service := newTestService(postRepo, newFakeEventRepo(), newFakeVenueRepo(), newFakeProcessingRunRepo(), batch, &fakeGeocoder{})

	firstID, started, err := service.TriggerAsync(context.Background())
	if err != nil || !started {
		t.Fatalf("expected the first trigger to start: started=%v err=%v", started, err)
	}

	waitFor(t, func() bool { return batch.submitCount() == 1 })

	secondID, started, err := service.TriggerAsync(context.Background())
	if err != nil {
		t.Fatalf("a conflicting trigger is not an error: %v", err)
	}
	if started {
		t.Fatal("expected the second trigger to be refused")
	}
	if secondID != firstID {
		t.Fatalf("expected the 409 to name run %d, got %d", firstID, secondID)
	}

	close(batch.block)

	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	service.WaitIdle(waitCtx)

	// Once the run is done the guard must have been released, or the service
	// would refuse every future run for the rest of its life.
	if _, started, _ := service.TriggerAsync(context.Background()); !started {
		t.Fatal("expected the guard to be released after the run finished")
	}
	service.WaitIdle(waitCtx)
}

// waitFor polls a condition instead of sleeping a fixed amount, so the test is
// neither flaky on a slow machine nor slower than it needs to be on a fast one.
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
