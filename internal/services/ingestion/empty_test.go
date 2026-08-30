package ingestion

import (
	"context"
	"testing"

	"event/ingestion-service/internal/repositories/igsource"
	"event/ingestion-service/internal/repositories/ingestionrun"
)

// An unseeded or fully deactivated whitelist is the most likely reason a run
// reports sources_total = 0, so pin down exactly what that run records.
func TestRunWithNoActiveSourcesRecordsZeroTotal(t *testing.T) {
	sourceRepo := &fakeIgSourceRepo{sources: []igsource.IgSource{}}
	runRepo := &fakeIngestionRunRepo{}
	ig := &fakeInstagram{}

	service := NewService(testConfig(), sourceRepo, &fakeIgRawPostRepo{}, runRepo, ig, &fakeNotifier{})

	summary, err := service.Run(context.Background())
	if err != nil {
		t.Fatalf("an empty whitelist is not an error: %v", err)
	}

	if summary.SourcesTotal != 0 {
		t.Fatalf("expected sources_total 0, got %d", summary.SourcesTotal)
	}
	if summary.PostsNew != 0 || summary.PostsUpdated != 0 {
		t.Fatalf("expected no posts, got %d new / %d updated", summary.PostsNew, summary.PostsUpdated)
	}
	// Nothing failed, so the run is honestly a success — the warning log is what
	// tells an operator the whitelist is empty.
	if summary.Status != StatusSuccess {
		t.Fatalf("expected status success, got %q", summary.Status)
	}
	// It must not call Instagram at all.
	if ig.callCount("") != 0 {
		t.Fatal("expected no Business Discovery calls")
	}

	var finished ingestionrun.FinishIngestionRun = runRepo.last()
	if finished.SourcesTotal != 0 || finished.Status != StatusSuccess {
		t.Fatalf("ingestion_runs row does not match: %+v", finished)
	}
}
