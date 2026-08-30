package venue

import "time"

// Venue is a row of the venues table.
type Venue struct {
	ID             int64
	Name           string
	NameNormalized string
	AddressText    *string
	Lat            *float64
	Lng            *float64
	GeocodedAt     *time.Time
}

// CreateVenue is a newly discovered venue. Lat/Lng/GeocodedAt are all nil when
// geocoding found nothing — the venue still exists, it just has no pin yet.
type CreateVenue struct {
	Name           string
	NameNormalized string
	AddressText    *string
	Lat            *float64
	Lng            *float64
	GeocodedAt     *time.Time
}
