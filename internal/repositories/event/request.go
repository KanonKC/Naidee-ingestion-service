package event

import "time"

// UpsertEvent is one extracted event landing in events.
type UpsertEvent struct {
	RawPostID       int64
	Title           *string
	AddressDetail   *string
	StartAt         *time.Time
	EndAt           *time.Time
	StartTimeKnown  bool
	EndTimeKnown    bool
	PriceText       *string
	Categories      []string
	Tags            []string
	RegistrationURL *string
	IsEvent         bool
	Confidence      string
}
