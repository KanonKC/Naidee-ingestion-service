package igrawpost

import "time"

// UnprocessedPost is one raw post still owed an extraction pass.
type UnprocessedPost struct {
	ID       int64
	PostedAt time.Time
	Caption  string
}
