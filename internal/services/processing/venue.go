package processing

import (
	"context"
	"strings"
	"time"
	"unicode"

	"event/ingestion-service/internal/logging"
	"event/ingestion-service/internal/repositories/venue"
)

// normalizeVenueName is the whole of venue matching in this phase: exact match
// on a lightly normalised name.
//
// It lowercases (captions mix Thai and English freely), collapses runs of
// whitespace, and trims trailing punctuation. That is enough to unify
// "BACC", "bacc" and "BACC  " — and deliberately not enough to unify "BACC"
// with "หอศิลป์กรุงเทพ", which stay two rows until fuzzy matching arrives.
//
// Changing this function changes which rows are considered the same venue, so
// a future edit needs a backfill to merge what it newly considers duplicates.
func normalizeVenueName(raw string) string {
	lowered := strings.ToLower(strings.TrimSpace(raw))

	// strings.Fields splits on any run of whitespace, which collapses double
	// spaces, tabs and stray newlines in one pass.
	collapsed := strings.Join(strings.Fields(lowered), " ")

	return strings.TrimRightFunc(collapsed, func(r rune) bool {
		return unicode.IsPunct(r) || unicode.IsSpace(r)
	})
}

// resolveVenue maps an extracted venue name to a venues row, geocoding it only
// the first time the name is seen. It returns the venue id and whether a
// geocode call was actually spent.
//
// A failure here is never fatal to the post: the event row is already written,
// and an event with no venue is worth more than no event at all.
func (s *Service) resolveVenue(
	ctx context.Context,
	logger *logging.TLogger,
	rawName string,
	rawNameTH *string,
) (venueID int64, geocoded bool, err error) {
	nameNormalized := normalizeVenueName(rawName)
	if nameNormalized == "" {
		return 0, false, nil
	}

	existing, err := s.venueRepo.FindByNormalizedName(ctx, nameNormalized)
	if err != nil {
		return 0, false, err
	}
	if existing != nil {
		// Already known. Nothing is re-geocoded, so geocoded_at stays put —
		// which is exactly what makes "same venue, second post" free.
		logger.Info(logging.Meta{
			Message: "venue resolved",
			Data:    map[string]any{"venue_id": existing.ID, "name": existing.Name, "cached": true},
		})
		return existing.ID, false, nil
	}

	request := venue.CreateVenue{
		Name:           strings.TrimSpace(rawName),
		NameNormalized: nameNormalized,
		NameTH:         trimmedOrNil(rawNameTH),
	}

	coordinates, geocodeErr := s.geocoder.Geocode(ctx, rawName)
	switch {
	case geocodeErr != nil:
		// A geocode outage must not stop events from landing. The venue is
		// created without a pin and can be filled in later by hand or by the
		// retry job in a future phase.
		logger.Warn(logging.Meta{
			Message: "geocode failed",
			Data:    map[string]any{"venue_name": rawName},
			Error:   geocodeErr,
		})

	case coordinates == nil:
		logger.Warn(logging.Meta{
			Message: "geocode not found",
			Data:    map[string]any{"venue_name": rawName},
		})

	default:
		now := time.Now()
		request.Lat = &coordinates.Lat
		request.Lng = &coordinates.Lng
		request.GeocodedAt = &now
		if coordinates.DisplayName != "" {
			request.AddressText = &coordinates.DisplayName
		}
		geocoded = true
	}

	venueID, err = s.venueRepo.Create(ctx, request)
	if err != nil {
		return 0, false, err
	}

	logger.Info(logging.Meta{
		Message: "venue resolved",
		Data:    map[string]any{"venue_id": venueID, "name": request.Name, "cached": false, "geocoded": geocoded},
	})
	return venueID, geocoded, nil
}

// trimmedOrNil trims whitespace and turns an empty result into nil, so a blank
// or missing venue_name_th is stored as NULL rather than an empty string.
func trimmedOrNil(raw *string) *string {
	if raw == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*raw)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}
