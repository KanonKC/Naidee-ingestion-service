package processing

import (
	"context"
	"errors"
	"testing"

	"event/ingestion-service/internal/logging"
	"event/ingestion-service/internal/providers/claude"
	"event/ingestion-service/internal/providers/geocode"
	"event/ingestion-service/internal/repositories/venue"
)

// normalizeVenueName decides which venue rows are considered the same place, so
// both halves matter: what it unifies, and what it deliberately leaves alone.
func TestNormalizeVenueName(t *testing.T) {
	unified := []struct {
		name string
		raw  []string
	}{
		{"case and padding", []string{"BACC", "bacc", "  BaCC  "}},
		{"collapsed whitespace", []string{"central world", "central   world", "central\tworld"}},
		{"trailing punctuation", []string{"หอศิลป์กรุงเทพ", "หอศิลป์กรุงเทพ.", "หอศิลป์กรุงเทพ !"}},
	}

	for _, group := range unified {
		t.Run(group.name, func(t *testing.T) {
			want := normalizeVenueName(group.raw[0])
			for _, raw := range group.raw[1:] {
				if got := normalizeVenueName(raw); got != want {
					t.Errorf("normalizeVenueName(%q) = %q, want %q", raw, got, want)
				}
			}
		})
	}

	// Phase 2 does exact matching only. These two are the same building, and
	// they are supposed to stay two rows until fuzzy matching lands — pinning
	// it here so the limitation is a decision, not a surprise.
	if normalizeVenueName("BACC") == normalizeVenueName("หอศิลป์กรุงเทพ") {
		t.Fatal("exact matching must not unify an abbreviation with its full name")
	}
}

// A geocoding outage must cost the venue its pin and nothing else: the venue
// row is still created and the event still gets linked to it.
func TestResolveVenueSurvivesGeocodeFailure(t *testing.T) {
	venueRepo := newFakeVenueRepo()
	geocoder := &fakeGeocoder{err: errors.New("nominatim: HTTP 503")}

	service := newTestService(newFakeIgRawPostRepo(), newFakeEventRepo(), venueRepo, newFakeProcessingRunRepo(), &fakeBatchClient{}, geocoder)
	logger := logging.New(logging.LayerService).SetContext("test", logging.SetContextOptions{Silent: true})

	venueID, geocoded, err := service.resolveVenue(context.Background(), logger, "BACC", nil)
	if err != nil {
		t.Fatalf("a geocode failure must not fail venue resolution: %v", err)
	}
	if venueID == 0 {
		t.Fatal("expected the venue to be created anyway")
	}
	if geocoded {
		t.Fatal("expected geocoded=false when the lookup failed")
	}
	if venueRepo.creates[0].Lat != nil || venueRepo.creates[0].GeocodedAt != nil {
		t.Fatal("expected the venue to be stored without coordinates")
	}
}

// "Searched, found nothing" is a normal outcome, not an error. The event still
// exists; it just will not have a pin on the map until someone fixes the address.
func TestResolveVenueStoresAVenueWithNoMatch(t *testing.T) {
	venueRepo := newFakeVenueRepo()
	geocoder := &fakeGeocoder{result: nil}

	service := newTestService(newFakeIgRawPostRepo(), newFakeEventRepo(), venueRepo, newFakeProcessingRunRepo(), &fakeBatchClient{}, geocoder)
	logger := logging.New(logging.LayerService).SetContext("test", logging.SetContextOptions{Silent: true})

	venueID, geocoded, err := service.resolveVenue(context.Background(), logger, "ลานหน้าเซ็นทรัลเวิลด์", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if venueID == 0 || geocoded {
		t.Fatalf("expected an ungeocoded venue row, got id=%d geocoded=%v", venueID, geocoded)
	}
}

// A venue name that normalises away to nothing (punctuation only) must not
// create a junk row keyed on the empty string.
func TestResolveVenueIgnoresAnEmptyName(t *testing.T) {
	venueRepo := newFakeVenueRepo()
	geocoder := &fakeGeocoder{result: &geocode.Coordinates{Lat: 1, Lng: 1}}

	service := newTestService(newFakeIgRawPostRepo(), newFakeEventRepo(), venueRepo, newFakeProcessingRunRepo(), &fakeBatchClient{}, geocoder)
	logger := logging.New(logging.LayerService).SetContext("test", logging.SetContextOptions{Silent: true})

	venueID, _, err := service.resolveVenue(context.Background(), logger, " -- ", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if venueID != 0 {
		t.Fatal("expected no venue row for a name that normalises to nothing")
	}
	if geocoder.callCount() != 0 {
		t.Fatal("expected no geocode call for an empty name")
	}
}

// An event whose venue resolution blows up must still be counted as processed:
// the events row is already written, and retrying the whole extraction to
// recover a venue would cost another model call for nothing.
func TestRunKeepsEventWhenVenueResolutionFails(t *testing.T) {
	postRepo := newFakeIgRawPostRepo(samplePost(1))
	eventRepo := newFakeEventRepo()
	venueRepo := &failingVenueRepo{}

	batch := &fakeBatchClient{
		results: []claude.ExtractionResult{{
			RawPostID:  1,
			IsEvent:    true,
			Confidence: claude.ConfidenceHigh,
			VenueName:  stringPtr("BACC"),
		}},
	}

	service := newTestService(postRepo, eventRepo, venueRepo, newFakeProcessingRunRepo(), batch, &fakeGeocoder{})

	summary, err := service.Run(context.Background())
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}

	if _, ok := eventRepo.get(1); !ok {
		t.Fatal("expected the event row to survive a venue failure")
	}
	if !postRepo.isProcessed(1) {
		t.Fatal("expected the post to still be marked processed")
	}
	if summary.PostsSucceeded != 1 {
		t.Fatalf("expected the post to count as succeeded, got %+v", summary)
	}
}

// failingVenueRepo stands in for a database that is momentarily unreachable.
type failingVenueRepo struct{}

func (f *failingVenueRepo) FindByNormalizedName(context.Context, string) (*venue.Venue, error) {
	return nil, errors.New("connection reset")
}

func (f *failingVenueRepo) Create(context.Context, venue.CreateVenue) (int64, error) {
	return 0, errors.New("connection reset")
}
