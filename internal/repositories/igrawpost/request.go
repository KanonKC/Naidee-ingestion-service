package igrawpost

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

// UpsertIgRawPost is one raw post landing in ig_raw_posts.
type UpsertIgRawPost struct {
	SourceID     int64
	IgMediaID    string
	Permalink    string
	Caption      *string
	MediaType    string
	MediaURL     *string
	ThumbnailURL *string
	PostedAt     time.Time
	RawPayload   json.RawMessage
}

// CaptionHash is the fingerprint the upsert compares against the stored one to
// decide whether the caption really changed. A NULL caption has no hash, which
// keeps "never had a caption" distinct from "had one, now empty".
//
// This exists because the processing side reprocesses on demand: when the hash
// moves, the upsert clears processed_at and the next processing run picks the
// post up again. Comparing the caption text directly would work too, but a
// fixed-width hash keeps the comparison cheap on a wide table.
func (r UpsertIgRawPost) CaptionHash() *string {
	if r.Caption == nil {
		return nil
	}
	sum := sha256.Sum256([]byte(*r.Caption))
	hash := hex.EncodeToString(sum[:])
	return &hash
}

// UnprocessedPost is one raw post still owed an extraction pass.
type UnprocessedPost struct {
	ID       int64
	PostedAt time.Time
	Caption  string
}
