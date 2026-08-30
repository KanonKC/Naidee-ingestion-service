package ingestion

import (
	"context"
	stderrors "errors"
	"fmt"
	"sync"
	"time"

	"event/ingestion-service/internal/config"
	"event/ingestion-service/internal/logging"
	"event/ingestion-service/internal/providers/googlechat"
	"event/ingestion-service/internal/providers/instagram"
	"event/ingestion-service/internal/repositories/igrawpost"
	"event/ingestion-service/internal/repositories/igsource"
	"event/ingestion-service/internal/repositories/ingestionrun"
)

// Service orchestrates one ingestion run: read the whitelist, fetch each
// account through Business Discovery, land the posts, record the outcome.
type Service struct {
	cfg               *config.Configurations
	igSourceRepo      IgSourceRepository
	igRawPostRepo     IgRawPostRepository
	ingestionRunRepo  IngestionRunRepository
	instagramProvider instagram.Client
	notifier          googlechat.Notifier
	logger            *logging.TLogger

	// running guards against a slow run overlapping the next cron tick.
	running sync.Mutex
}

func NewService(
	cfg *config.Configurations,
	igSourceRepo IgSourceRepository,
	igRawPostRepo IgRawPostRepository,
	ingestionRunRepo IngestionRunRepository,
	instagramProvider instagram.Client,
	notifier googlechat.Notifier,
) *Service {
	return &Service{
		cfg:               cfg,
		igSourceRepo:      igSourceRepo,
		igRawPostRepo:     igRawPostRepo,
		ingestionRunRepo:  ingestionRunRepo,
		instagramProvider: instagramProvider,
		notifier:          notifier,
		logger:            logging.New(logging.LayerService),
	}
}

// VerifyToken proves the access token works. Called at startup so a dead token
// fails the boot loudly instead of killing the cron run at 3am.
func (s *Service) VerifyToken(ctx context.Context) error {
	logger := s.logger.SetContext("service.ingestion.verifyToken")

	if err := s.instagramProvider.VerifyToken(ctx); err != nil {
		logger.Error(logging.Meta{Message: "Instagram access token verification failed", Error: err})
		return err
	}

	logger.Info(logging.Meta{Message: "Instagram access token verified"})
	return nil
}

// Run executes a full ingestion pass and blocks until it finishes. It is safe to
// call concurrently: an overlapping call returns (nil, nil) immediately rather
// than double-fetching every source.
func (s *Service) Run(ctx context.Context) (*RunSummary, error) {
	if !s.running.TryLock() {
		s.logger.SetContext("service.ingestion.run", logging.SetContextOptions{Silent: true}).
			Warn(logging.Meta{Message: "ingestion run skipped, previous run still in progress"})
		return nil, nil
	}
	defer s.running.Unlock()

	return s.run(ctx)
}

// TriggerAsync starts a run in the background and returns whether it started.
// The manual trigger endpoint uses this: an ingestion pass takes far longer than
// a request should, so the caller gets an acknowledgement and watches
// ingestion_runs for the outcome.
//
// ctx must outlive the request — pass the application context, not r.Context(),
// or the run is cancelled the moment the response is written.
func (s *Service) TriggerAsync(ctx context.Context) bool {
	if !s.running.TryLock() {
		s.logger.SetContext("service.ingestion.triggerAsync", logging.SetContextOptions{Silent: true}).
			Warn(logging.Meta{Message: "ingestion run skipped, previous run still in progress"})
		return false
	}

	go func() {
		defer s.running.Unlock()
		logger := s.logger.SetContext("service.ingestion.triggerAsync", logging.SetContextOptions{Silent: true})
		if _, err := s.run(ctx); err != nil {
			logger.Error(logging.Meta{Message: "Manually triggered ingestion run failed", Error: err})
		}
	}()
	return true
}

// WaitIdle blocks until no run is in progress, or until ctx expires. Shutdown
// uses it so a manually triggered run gets a chance to close out its
// ingestion_runs row instead of being left stuck on `running`.
func (s *Service) WaitIdle(ctx context.Context) {
	idle := make(chan struct{})
	go func() {
		s.running.Lock()
		s.running.Unlock()
		close(idle)
	}()

	select {
	case <-idle:
	case <-ctx.Done():
	}
}

// run is the unlocked body of a run. Every caller must already hold s.running.
func (s *Service) run(ctx context.Context) (*RunSummary, error) {
	logger := s.logger.SetContext("service.ingestion.run", logging.SetContextOptions{Silent: true})

	runID, err := s.ingestionRunRepo.Create(ctx, SourceKindInstagram)
	if err != nil {
		logger.Error(logging.Meta{Message: "Failed to open ingestion run", Error: err})
		return nil, err
	}
	logger = logger.With("run_id", runID)

	sources, err := s.igSourceRepo.ListActive(ctx)
	if err != nil {
		logger.Error(logging.Meta{Message: "Failed to load active sources", Error: err})
		s.finishRun(ctx, logger, runID, StatusFailed, 0, &runCounters{}, err)
		return nil, err
	}

	logger.Info(logging.Meta{Message: "ingestion run started", Data: map[string]any{"sources": len(sources)}})

	// A run with nothing to do is not a failure, but it must not look like a
	// healthy run either: an empty whitelist almost always means the seed never
	// landed, or every source has been deactivated.
	if len(sources) == 0 {
		logger.Warn(logging.Meta{Message: "ingestion run has no active sources — check that ig_sources is seeded and is_active = true"})
	}

	counters := &runCounters{}
	fatalErr := s.processSources(ctx, logger, sources, counters)

	status := s.resolveStatus(len(sources), counters, fatalErr)
	summary := s.finishRun(ctx, logger, runID, status, len(sources), counters, fatalErr)

	logger.Info(logging.Meta{
		Message: "ingestion run finished",
		Data: map[string]any{
			"status":  summary.Status,
			"ok":      summary.SourcesOK,
			"failed":  summary.SourcesFailed,
			"new":     summary.PostsNew,
			"updated": summary.PostsUpdated,
		},
	})

	if fatalErr != nil {
		return summary, fatalErr
	}
	return summary, nil
}

// processSources drives the bounded worker pool. It returns the fatal error that
// aborted the run, or nil.
func (s *Service) processSources(
	ctx context.Context,
	logger *logging.TLogger,
	sources []igsource.IgSource,
	counters *runCounters,
) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg        sync.WaitGroup
		fatalOnce sync.Once
		fatalErr  error
		semaphore = make(chan struct{}, s.cfg.IngestionCron.WorkerConcurrency)
	)

	for _, source := range sources {
		// A fatal error cancels the pool; every source not yet started is skipped.
		if runCtx.Err() != nil {
			break
		}

		wg.Add(1)
		semaphore <- struct{}{}

		go func(source igsource.IgSource) {
			defer wg.Done()
			defer func() { <-semaphore }()

			// The pool may have been cancelled while this worker waited for a
			// slot. Starting the fetch anyway would waste a call and, worse,
			// record a failure against a source that was never really tried.
			if runCtx.Err() != nil {
				return
			}

			if err := s.processSource(runCtx, logger, source, counters); err != nil {
				if instagram.IsFatal(err) {
					fatalOnce.Do(func() {
						fatalErr = err
						cancel()
					})
				}
			}
		}(source)
	}

	wg.Wait()
	return fatalErr
}

// processSource fetches and lands one account. The returned error is only used
// to detect the fatal case; everything else is already accounted for in counters.
func (s *Service) processSource(
	ctx context.Context,
	logger *logging.TLogger,
	source igsource.IgSource,
	counters *runCounters,
) error {
	result, err := s.discoverWithRetry(ctx, logger, source)
	if err != nil {
		return s.handleSourceError(ctx, logger, source, err, counters)
	}

	postsNew, postsUpdated := 0, 0
	for _, media := range result.Media {
		inserted, upsertErr := s.igRawPostRepo.Upsert(ctx, igrawpost.UpsertIgRawPost{
			SourceID:     source.ID,
			IgMediaID:    media.ID,
			Permalink:    media.Permalink,
			Caption:      optional(media.Caption),
			MediaType:    media.MediaType,
			MediaURL:     optional(media.MediaURL),
			ThumbnailURL: optional(media.ThumbnailURL),
			PostedAt:     media.Timestamp,
			RawPayload:   media.Raw,
		})
		if upsertErr != nil {
			// A database problem is not the source's fault, but the source did
			// not fully land either — count it as failed and move on.
			return s.handleSourceError(ctx, logger, source, upsertErr, counters)
		}

		if inserted {
			postsNew++
		} else {
			postsUpdated++
		}
	}

	bookCtx, cancel := s.bookkeepingContext(ctx)
	defer cancel()

	if err := s.igSourceRepo.MarkSynced(bookCtx, source.ID); err != nil {
		logger.Error(logging.Meta{Message: "Failed to mark source synced", Data: map[string]any{"username": source.Username}, Error: err})
	}

	counters.addSuccess(postsNew, postsUpdated)
	logger.Info(logging.Meta{
		Message: "source ingested",
		Data:    map[string]any{"username": source.Username, "new": postsNew, "updated": postsUpdated},
	})
	return nil
}

// discoverWithRetry retries transient failures with exponential backoff.
// Permanent and fatal errors return immediately — retrying either is pointless.
func (s *Service) discoverWithRetry(
	ctx context.Context,
	logger *logging.TLogger,
	source igsource.IgSource,
) (*instagram.DiscoveryResult, error) {
	var lastErr error

	for attempt := 1; attempt <= maxRetries+1; attempt++ {
		result, err := s.instagramProvider.DiscoverMedia(ctx, source.Username, s.cfg.Instagram.MediaLimit)
		if err == nil {
			return result, nil
		}
		lastErr = err

		var apiErr *instagram.APIError
		if !stderrors.As(err, &apiErr) || apiErr.Kind != instagram.ErrTransient {
			return nil, err
		}

		logger.Warn(logging.Meta{
			Message: "source failed",
			Data:    map[string]any{"username": source.Username, "kind": apiErr.Kind.String(), "attempt": attempt},
			Error:   err,
		})

		if attempt > maxRetries {
			break
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(retryBaseDelay << (attempt - 1)):
		}
	}

	return nil, lastErr
}

// handleSourceError applies the per-kind policy from the spec and always leaves
// the source counted exactly once.
func (s *Service) handleSourceError(
	ctx context.Context,
	logger *logging.TLogger,
	source igsource.IgSource,
	err error,
	counters *runCounters,
) error {
	// The run context may already be cancelled by a fatal error in a sibling
	// goroutine, so bookkeeping writes get their own detached context.
	bookCtx, cancel := s.bookkeepingContext(ctx)
	defer cancel()

	// A source cut short by the abort is not a source at fault: count it as not
	// succeeded, but leave consecutive_failures alone. Otherwise a few
	// rate-limited runs would auto-deactivate a whole healthy whitelist.
	if stderrors.Is(err, context.Canceled) || stderrors.Is(err, context.DeadlineExceeded) {
		logger.Warn(logging.Meta{
			Message: "source skipped, run aborted",
			Data:    map[string]any{"username": source.Username},
		})
		counters.addFailure()
		return nil
	}

	var apiErr *instagram.APIError
	if stderrors.As(err, &apiErr) {
		switch apiErr.Kind {
		case instagram.ErrFatal:
			logger.Error(logging.Meta{
				Message: "ingestion run aborted",
				Data:    map[string]any{"username": source.Username, "kind": apiErr.Kind.String(), "code": apiErr.Code},
				Error:   err,
			})
			counters.addFailure()
			s.alertFatal(bookCtx, logger, apiErr)
			return err

		case instagram.ErrPermanent:
			if deactivateErr := s.igSourceRepo.Deactivate(bookCtx, igsource.DeactivateIgSource{
				ID:     source.ID,
				Reason: apiErr.Message,
			}); deactivateErr != nil {
				logger.Error(logging.Meta{Message: "Failed to deactivate source", Data: map[string]any{"username": source.Username}, Error: deactivateErr})
			}
			logger.Error(logging.Meta{
				Message: "source deactivated",
				Data:    map[string]any{"username": source.Username, "reason": apiErr.Message},
			})
			counters.addFailure()
			return nil
		}
	}

	// Transient (or a database error): record the failure and auto-deactivate
	// once the source has been failing for too many runs in a row.
	failures, markErr := s.igSourceRepo.MarkFailed(bookCtx, igsource.MarkFailedIgSource{
		ID:        source.ID,
		LastError: err.Error(),
	})
	if markErr != nil {
		logger.Error(logging.Meta{Message: "Failed to record source failure", Data: map[string]any{"username": source.Username}, Error: markErr})
		counters.addFailure()
		return nil
	}

	logger.Warn(logging.Meta{
		Message: "source failed",
		Data:    map[string]any{"username": source.Username, "consecutive_failures": failures},
		Error:   err,
	})

	if failures >= autoDeactivateThreshold {
		reason := fmt.Sprintf("auto-deactivated after %d consecutive failures: %s", failures, err.Error())
		if deactivateErr := s.igSourceRepo.Deactivate(bookCtx, igsource.DeactivateIgSource{ID: source.ID, Reason: reason}); deactivateErr != nil {
			logger.Error(logging.Meta{Message: "Failed to auto-deactivate source", Data: map[string]any{"username": source.Username}, Error: deactivateErr})
		} else {
			logger.Error(logging.Meta{
				Message: "source deactivated",
				Data:    map[string]any{"username": source.Username, "reason": reason},
			})
		}
	}

	counters.addFailure()
	return nil
}

// alertFatal pages a human. An expired token needs a full OAuth flow to recover,
// so it must never be a log line nobody reads.
func (s *Service) alertFatal(ctx context.Context, logger *logging.TLogger, apiErr *instagram.APIError) {
	message := fmt.Sprintf("[ingestion-service] run aborted — Instagram API error code %d: %s", apiErr.Code, apiErr.Message)
	if hint := instagram.RemediationHint(apiErr.Code); hint != "" {
		message += "\n" + hint
	}

	if err := s.notifier.Notify(ctx, message); err != nil {
		logger.Error(logging.Meta{Message: "Failed to deliver fatal alert", Error: err})
	}
}

func (s *Service) resolveStatus(sourcesTotal int, counters *runCounters, fatalErr error) string {
	_, sourcesFailed, _, _ := counters.snapshot()

	switch {
	case fatalErr != nil:
		return StatusFailed
	case sourcesTotal == 0 || sourcesFailed == 0:
		return StatusSuccess
	case sourcesFailed == sourcesTotal:
		return StatusFailed
	default:
		return StatusPartial
	}
}

func (s *Service) finishRun(
	ctx context.Context,
	logger *logging.TLogger,
	runID int64,
	status string,
	sourcesTotal int,
	counters *runCounters,
	runErr error,
) *RunSummary {
	sourcesOK, sourcesFailed, postsNew, postsUpdated := counters.snapshot()

	var errMessage *string
	if runErr != nil {
		message := runErr.Error()
		errMessage = &message
	}

	bookCtx, cancel := s.bookkeepingContext(ctx)
	defer cancel()

	if err := s.ingestionRunRepo.Finish(bookCtx, ingestionrun.FinishIngestionRun{
		ID:            runID,
		Status:        status,
		SourcesTotal:  sourcesTotal,
		SourcesOK:     sourcesOK,
		SourcesFailed: sourcesFailed,
		PostsNew:      postsNew,
		PostsUpdated:  postsUpdated,
		Error:         errMessage,
	}); err != nil {
		logger.Error(logging.Meta{Message: "Failed to close ingestion run", Error: err})
	}

	return &RunSummary{
		RunID:         runID,
		Status:        status,
		SourcesTotal:  sourcesTotal,
		SourcesOK:     sourcesOK,
		SourcesFailed: sourcesFailed,
		PostsNew:      postsNew,
		PostsUpdated:  postsUpdated,
	}
}

// bookkeepingContext detaches from cancellation so status writes still land
// after a fatal error has torn down the worker pool.
func (s *Service) bookkeepingContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), bookkeepingTimeout)
}

// optional maps the empty string to NULL, keeping "no caption" distinct from "".
func optional(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
