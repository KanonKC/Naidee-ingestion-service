package processing

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"event/ingestion-service/internal/config"
	"event/ingestion-service/internal/logging"
	"event/ingestion-service/internal/providers/claude"
	"event/ingestion-service/internal/providers/geocode"
	"event/ingestion-service/internal/repositories/event"
	"event/ingestion-service/internal/repositories/igrawpost"
	"event/ingestion-service/internal/repositories/processingrun"
)

// Service orchestrates one processing run: pick up the raw posts nobody has
// extracted yet, put them through one Claude batch, and land the results as
// events and venues.
//
// It knows nothing about Instagram. Its whole view of the world is "there are
// rows in ig_raw_posts with processed_at IS NULL" — so a future Eventpop or
// Zipevent ingester that lands rows in the same shape works here for free.
type Service struct {
	cfg               *config.Configurations
	igRawPostRepo     IgRawPostRepository
	eventRepo         EventRepository
	venueRepo         VenueRepository
	processingRunRepo ProcessingRunRepository
	batchClient       claude.BatchClient
	geocoder          geocode.Geocoder
	logger            *logging.TLogger

	// running guards against a run overlapping the next cron tick. A batch can
	// be polled for up to two hours while the cron fires every thirty minutes,
	// so without this the same posts would be submitted four times over.
	//
	// This is an in-process flag: it protects a single replica, which is what
	// this service is designed to be. Running two replicas needs
	// pg_try_advisory_lock instead — the flag would not see the other process.
	running atomic.Bool
	// currentRunID lets the trigger endpoint say *which* run is in the way when
	// it answers 409.
	currentRunID atomic.Int64
	// inFlight is what shutdown waits on, so a run started by the manual
	// trigger still gets to close out its processing_runs row.
	inFlight sync.WaitGroup
}

func NewService(
	cfg *config.Configurations,
	igRawPostRepo IgRawPostRepository,
	eventRepo EventRepository,
	venueRepo VenueRepository,
	processingRunRepo ProcessingRunRepository,
	batchClient claude.BatchClient,
	geocoder geocode.Geocoder,
) *Service {
	return &Service{
		cfg:               cfg,
		igRawPostRepo:     igRawPostRepo,
		eventRepo:         eventRepo,
		venueRepo:         venueRepo,
		processingRunRepo: processingRunRepo,
		batchClient:       batchClient,
		geocoder:          geocoder,
		logger:            logging.New(logging.LayerService),
	}
}

// Run executes a full processing pass and blocks until it finishes. An
// overlapping call returns (nil, nil) immediately rather than double-submitting
// every pending post.
func (s *Service) Run(ctx context.Context) (*RunSummary, error) {
	if !s.running.CompareAndSwap(false, true) {
		s.logger.SetContext("service.processing.run", logging.SetContextOptions{Silent: true}).
			Warn(logging.Meta{Message: "processing run skipped, previous run still in progress"})
		return nil, nil
	}
	s.inFlight.Add(1)
	defer func() {
		s.inFlight.Done()
		s.running.Store(false)
	}()

	return s.run(ctx, s.cfg.LLM.PostLimit)
}

// ErrInvalidLimit is returned by TriggerAsync when the caller-supplied limit
// falls outside what a single batch can hold.
var ErrInvalidLimit = errors.New("limit must be between 1 and the Batch API cap")

// TriggerAsync starts a run in the background and reports whether it started,
// along with the id of the relevant run — the new one when it started, the one
// already in flight when it did not.
//
// limit caps how many pending posts this one run submits. Pass 0 to fall back
// to LLM_POST_LIMIT (the cron's default); any other value must be between 1
// and config.MaxBatchRequests or ErrInvalidLimit is returned before a run row
// is even opened.
//
// The manual trigger endpoint uses this: a run can take hours, which is far
// longer than a request should be held open, so the caller gets a run id and
// polls GET /api/v1/admin/processing/runs/{id} for the outcome.
//
// ctx must outlive the request — pass the application context, not
// r.Context(), or the run is cancelled the moment the response is written.
func (s *Service) TriggerAsync(ctx context.Context, limit int) (runID int64, started bool, err error) {
	logger := s.logger.SetContext("service.processing.triggerAsync", logging.SetContextOptions{Silent: true})

	if limit == 0 {
		limit = s.cfg.LLM.PostLimit
	} else if limit < 0 || limit > config.MaxBatchRequests {
		return 0, false, ErrInvalidLimit
	}

	if !s.running.CompareAndSwap(false, true) {
		logger.Warn(logging.Meta{Message: "processing run skipped, previous run still in progress"})
		return s.currentRunID.Load(), false, nil
	}

	// The row is opened synchronously so the caller gets a run id it can poll
	// immediately. Doing this inside the goroutine would mean answering 202
	// with a run that does not exist yet.
	runID, err = s.processingRunRepo.Create(ctx, processingrun.StatusSubmitted)
	if err != nil {
		s.running.Store(false)
		logger.Error(logging.Meta{Message: "Failed to open processing run", Error: err})
		return 0, false, err
	}
	s.currentRunID.Store(runID)

	s.inFlight.Add(1)
	go func() {
		defer func() {
			s.inFlight.Done()
			s.running.Store(false)
		}()
		if _, err := s.runWithID(ctx, runID, limit); err != nil {
			logger.Error(logging.Meta{Message: "Manually triggered processing run failed", Error: err})
		}
	}()

	return runID, true, nil
}

// GetRun serves GET /api/v1/admin/processing/runs/{id} straight from processing_runs. Everything
// the caller wants to know is already written there by the normal flow, so
// there is no reason to ask Anthropic anything.
func (s *Service) GetRun(ctx context.Context, id int64) (*processingrun.ProcessingRun, error) {
	return s.processingRunRepo.Get(ctx, id)
}

// WaitIdle blocks until no run is in progress, or until ctx expires. Shutdown
// uses it so a run in flight gets a chance to close out its processing_runs row
// instead of being left stuck on `polling`.
func (s *Service) WaitIdle(ctx context.Context) {
	idle := make(chan struct{})
	go func() {
		s.inFlight.Wait()
		close(idle)
	}()

	select {
	case <-idle:
	case <-ctx.Done():
	}
}

// run opens the processing_runs row and delegates. Every caller must already
// hold the running flag.
func (s *Service) run(ctx context.Context, limit int) (*RunSummary, error) {
	logger := s.logger.SetContext("service.processing.run", logging.SetContextOptions{Silent: true})

	runID, err := s.processingRunRepo.Create(ctx, processingrun.StatusSubmitted)
	if err != nil {
		logger.Error(logging.Meta{Message: "Failed to open processing run", Error: err})
		return nil, err
	}
	s.currentRunID.Store(runID)

	return s.runWithID(ctx, runID, limit)
}

// runWithID is the body of a run, against an already-open processing_runs row.
//
// The shape is deliberately linear rather than fan-out: one batch, one poll
// loop, one pass over the results. The expensive part (the model) is already
// batched, and the second most expensive part (geocoding) is capped at one
// call per second, so there is nothing here that concurrency would speed up.
func (s *Service) runWithID(ctx context.Context, runID int64, limit int) (*RunSummary, error) {
	logger := s.logger.SetContext("service.processing.run", logging.SetContextOptions{Silent: true}).With("run_id", runID)

	counters := &runCounters{}

	posts, err := s.igRawPostRepo.ListUnprocessed(ctx, limit)
	if err != nil {
		logger.Error(logging.Meta{Message: "Failed to load pending posts", Error: err})
		return s.finishRun(ctx, logger, runID, nil, processingrun.StatusFailed, counters, err), err
	}

	logger.Info(logging.Meta{Message: "processing run started", Data: map[string]any{"posts_pending": len(posts)}})

	// Nothing to do is a completed run, not a failure — and emphatically not a
	// reason to submit an empty batch.
	if len(posts) == 0 {
		logger.Info(logging.Meta{Message: "processing run finished", Data: map[string]any{"status": processingrun.StatusCompleted, "posts_pending": 0}})
		return s.finishRun(ctx, logger, runID, nil, processingrun.StatusCompleted, counters, nil), nil
	}

	requests := make([]claude.ExtractionRequest, 0, len(posts))
	for _, post := range posts {
		requests = append(requests, claude.ExtractionRequest{
			RawPostID: post.ID,
			PostedAt:  post.PostedAt,
			Caption:   post.Caption,
		})
	}

	batchID, err := s.batchClient.Submit(ctx, requests)
	if err != nil {
		// No post was marked processed, so the next run picks up the whole set
		// again. Nothing is lost by failing here.
		logger.Error(logging.Meta{Message: "Failed to submit batch", Error: err})
		return s.finishRun(ctx, logger, runID, nil, processingrun.StatusFailed, counters, err), err
	}

	counters.postsSubmitted = len(requests)
	logger.Info(logging.Meta{
		Message: "batch submitted",
		Data:    map[string]any{"batch_id": batchID, "requests": len(requests)},
	})

	if err := s.processingRunRepo.SetBatch(ctx, runID, batchID, len(requests)); err != nil {
		// Losing the batch id is bad enough to abort on: without it a timed-out
		// poll leaves no way to find the batch again.
		logger.Error(logging.Meta{Message: "Failed to record batch id", Error: err})
		return s.finishRun(ctx, logger, runID, &batchID, processingrun.StatusFailed, counters, err), err
	}

	startedAt := time.Now()
	if err := s.pollUntilEnded(ctx, logger, batchID); err != nil {
		logger.Error(logging.Meta{Message: "batch poll failed", Data: map[string]any{"batch_id": batchID}, Error: err})
		return s.finishRun(ctx, logger, runID, &batchID, processingrun.StatusFailed, counters, err), err
	}

	logger.Info(logging.Meta{
		Message: "batch completed",
		Data:    map[string]any{"batch_id": batchID, "elapsed": time.Since(startedAt).String()},
	})

	results, err := s.batchClient.FetchResults(ctx, batchID)
	if err != nil {
		logger.Error(logging.Meta{Message: "Failed to fetch batch results", Data: map[string]any{"batch_id": batchID}, Error: err})
		return s.finishRun(ctx, logger, runID, &batchID, processingrun.StatusFailed, counters, err), err
	}

	s.applyResults(ctx, logger, posts, results, counters)

	summary := s.finishRun(ctx, logger, runID, &batchID, processingrun.StatusCompleted, counters, nil)
	logger.Info(logging.Meta{
		Message: "processing run finished",
		Data: map[string]any{
			"status":    summary.Status,
			"succeeded": summary.PostsSucceeded,
			"failed":    summary.PostsFailed,
			"geocoded":  summary.EventsGeocoded,
		},
	})
	return summary, nil
}

// pollUntilEnded waits for the batch to finish, or gives up.
//
// Giving up does not cancel anything on Anthropic's side — the batch keeps
// running and its results stay fetchable for 29 days. What it does mean is that
// no post from this batch gets marked processed, so the next run resubmits
// them. That costs a second batch but never loses a post.
func (s *Service) pollUntilEnded(ctx context.Context, logger *logging.TLogger, batchID string) error {
	deadline := time.Now().Add(s.cfg.LLM.PollTimeout)

	ticker := time.NewTicker(s.cfg.LLM.PollInterval)
	defer ticker.Stop()

	for {
		status, err := s.batchClient.Poll(ctx, batchID)
		if err != nil {
			// A single failed poll is not worth abandoning a batch that may
			// have been running for an hour — keep trying until the deadline.
			logger.Warn(logging.Meta{Message: "batch poll attempt failed", Data: map[string]any{"batch_id": batchID}, Error: err})
		} else if status.Ended {
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("batch %s did not end within %s", batchID, s.cfg.LLM.PollTimeout)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// applyResults writes every result to the database.
//
// Results come back unordered, so they are keyed by raw post id. A post that
// gets no result line at all — which a partially delivered batch would produce
// — is counted as failed and left unprocessed, so the next run retries it.
func (s *Service) applyResults(
	ctx context.Context,
	logger *logging.TLogger,
	posts []igrawpost.UnprocessedPost,
	results []claude.ExtractionResult,
	counters *runCounters,
) {
	seen := make(map[int64]bool, len(results))

	for _, result := range results {
		seen[result.RawPostID] = true

		if result.Error != nil {
			// processed_at stays NULL, so the next run tries this post again.
			logger.Warn(logging.Meta{
				Message: "extraction parse failed",
				Data:    map[string]any{"raw_post_id": result.RawPostID},
				Error:   result.Error,
			})
			counters.postsFailed++
			continue
		}

		if err := s.applyResult(ctx, logger, result, counters); err != nil {
			logger.Error(logging.Meta{
				Message: "Failed to store extraction result",
				Data:    map[string]any{"raw_post_id": result.RawPostID},
				Error:   err,
			})
			counters.postsFailed++
			continue
		}

		counters.postsSucceeded++
	}

	for _, post := range posts {
		if !seen[post.ID] {
			logger.Warn(logging.Meta{
				Message: "batch returned no result for post",
				Data:    map[string]any{"raw_post_id": post.ID},
			})
			counters.postsFailed++
		}
	}
}

// applyResult lands one post: the event row first, then its venue, then the
// processed stamp.
//
// The order matters. processed_at is written last so that a failure anywhere
// above it leaves the post pending rather than silently dropped.
func (s *Service) applyResult(
	ctx context.Context,
	logger *logging.TLogger,
	result claude.ExtractionResult,
	counters *runCounters,
) error {
	// A non-event earns no row at all: ig_raw_posts.processed_at (set below) is
	// what actually stops it being picked up again, so there is nothing worth
	// keeping in events for it.
	if result.IsEvent {
		if err := s.eventRepo.Upsert(ctx, event.UpsertEvent{
			RawPostID:       result.RawPostID,
			Title:           result.Title,
			AddressDetail:   result.AddressDetail,
			StartAt:         result.StartDate,
			EndAt:           result.EndDate,
			StartTimeKnown:  result.StartTimeKnown,
			EndTimeKnown:    result.EndTimeKnown,
			PriceText:       result.PriceText,
			Categories:      result.Categories,
			Tags:            result.Tags,
			RegistrationURL: result.RegistrationURL,
			IsEvent:         result.IsEvent,
			Confidence:      result.Confidence,
		}); err != nil {
			return err
		}
	}

	// Only a real event earns a venue lookup. Geocoding a non-event would spend
	// a rate-limited call on a place nobody will ever look for.
	if result.IsEvent && result.VenueName != nil && *result.VenueName != "" {
		venueID, geocoded, err := s.resolveVenue(ctx, logger, *result.VenueName)
		if err != nil {
			// The event row is already written and is useful without a venue,
			// so this is a warning and the post still counts as processed.
			logger.Warn(logging.Meta{
				Message: "venue resolution failed",
				Data:    map[string]any{"raw_post_id": result.RawPostID, "venue_name": *result.VenueName},
				Error:   err,
			})
		} else if venueID != 0 {
			if err := s.eventRepo.SetVenue(ctx, result.RawPostID, venueID); err != nil {
				logger.Warn(logging.Meta{
					Message: "Failed to attach venue to event",
					Data:    map[string]any{"raw_post_id": result.RawPostID, "venue_id": venueID},
					Error:   err,
				})
			}
			if geocoded {
				counters.eventsGeocoded++
			}
		}
	}

	return s.igRawPostRepo.MarkProcessed(ctx, result.RawPostID)
}

// finishRun closes out the processing_runs row and builds the summary.
func (s *Service) finishRun(
	ctx context.Context,
	logger *logging.TLogger,
	runID int64,
	batchID *string,
	status string,
	counters *runCounters,
	runErr error,
) *RunSummary {
	var errMessage *string
	if runErr != nil {
		message := runErr.Error()
		errMessage = &message
	}

	// Detached from cancellation so the status write still lands when the run
	// was cut short by shutdown.
	bookCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bookkeepingTimeout)
	defer cancel()

	if err := s.processingRunRepo.Finish(bookCtx, processingrun.FinishProcessingRun{
		ID:             runID,
		Status:         status,
		PostsSubmitted: counters.postsSubmitted,
		PostsSucceeded: counters.postsSucceeded,
		PostsFailed:    counters.postsFailed,
		EventsGeocoded: counters.eventsGeocoded,
		Error:          errMessage,
	}); err != nil {
		logger.Error(logging.Meta{Message: "Failed to close processing run", Error: err})
	}

	return &RunSummary{
		RunID:          runID,
		BatchID:        batchID,
		Status:         status,
		PostsSubmitted: counters.postsSubmitted,
		PostsSucceeded: counters.postsSucceeded,
		PostsFailed:    counters.postsFailed,
		EventsGeocoded: counters.eventsGeocoded,
	}
}
