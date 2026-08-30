package ingestion

import "sync"

// runCounters aggregates per-source results across the worker pool.
type runCounters struct {
	mu            sync.Mutex
	sourcesOK     int
	sourcesFailed int
	postsNew      int
	postsUpdated  int
}

func (c *runCounters) addSuccess(postsNew, postsUpdated int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sourcesOK++
	c.postsNew += postsNew
	c.postsUpdated += postsUpdated
}

func (c *runCounters) addFailure() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sourcesFailed++
}

func (c *runCounters) snapshot() (sourcesOK, sourcesFailed, postsNew, postsUpdated int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sourcesOK, c.sourcesFailed, c.postsNew, c.postsUpdated
}

// RunSummary is what a completed run reports back to its caller.
type RunSummary struct {
	RunID         int64  `json:"run_id"`
	Status        string `json:"status"`
	SourcesTotal  int    `json:"sources_total"`
	SourcesOK     int    `json:"sources_ok"`
	SourcesFailed int    `json:"sources_failed"`
	PostsNew      int    `json:"posts_new"`
	PostsUpdated  int    `json:"posts_updated"`
}
