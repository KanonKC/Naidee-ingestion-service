package igsource

import "time"

// IgSource is a row of the ig_sources whitelist.
type IgSource struct {
	ID                  int64
	Username            string
	DisplayName         *string
	IsActive            bool
	LastSyncedAt        *time.Time
	LastError           *string
	ConsecutiveFailures int
	CreatedAt           time.Time
	UpdatedAt           time.Time
}
