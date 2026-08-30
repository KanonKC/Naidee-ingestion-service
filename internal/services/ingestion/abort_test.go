package ingestion

import (
	"context"
	"sync"
	"testing"

	"event/ingestion-service/internal/providers/instagram"
	"event/ingestion-service/internal/repositories/igsource"
)

// selectiveInstagram fails one named source fatally and blocks the rest until
// the pool is cancelled, so we can observe what happens to sources that were
// mid-flight or never started when the abort landed.
type selectiveInstagram struct {
	fatalFor string

	mu    sync.Mutex
	calls map[string]int
}

func (s *selectiveInstagram) DiscoverMedia(ctx context.Context, username string, limit int) (*instagram.DiscoveryResult, error) {
	s.mu.Lock()
	if s.calls == nil {
		s.calls = map[string]int{}
	}
	s.calls[username]++
	s.mu.Unlock()

	if username == s.fatalFor {
		return nil, &instagram.APIError{Kind: instagram.ErrFatal, Code: 32, Message: "Page request limit reached"}
	}

	// Every other source waits until the fatal error cancels the pool.
	<-ctx.Done()
	return nil, ctx.Err()
}

func (s *selectiveInstagram) VerifyToken(ctx context.Context) error { return nil }

func TestAbortDoesNotPenaliseSourcesThatWereNeverAtFault(t *testing.T) {
	sources := []igsource.IgSource{
		source(1, "rate_limited_venue"),
		source(2, "healthy_venue_a"),
		source(3, "healthy_venue_b"),
		source(4, "healthy_venue_c"),
		source(5, "healthy_venue_d"),
	}
	sourceRepo := &fakeIgSourceRepo{sources: sources}
	runRepo := &fakeIngestionRunRepo{}
	ig := &selectiveInstagram{fatalFor: "rate_limited_venue"}

	service := NewService(testConfig(), sourceRepo, &fakeIgRawPostRepo{}, runRepo, ig, &fakeNotifier{})

	summary, err := service.Run(context.Background())
	if err == nil {
		t.Fatal("expected the fatal error to be returned")
	}
	if summary.Status != StatusFailed {
		t.Fatalf("expected status failed, got %q", summary.Status)
	}

	// The healthy sources were cut short by the abort, not broken. Their
	// consecutive_failures must not move, or a few rate-limited runs would
	// auto-deactivate the whole whitelist.
	sourceRepo.mu.Lock()
	failedCount := len(sourceRepo.failed)
	sourceRepo.mu.Unlock()
	if failedCount != 0 {
		t.Fatalf("expected no consecutive_failures bumps, got %d: %+v", failedCount, sourceRepo.failed)
	}

	if deactivated := sourceRepo.deactivatedIDs(); len(deactivated) != 0 {
		t.Fatalf("an aborted run must not deactivate any source, got %v", deactivated)
	}
}
