package claude

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"event/ingestion-service/internal/config"
	"event/ingestion-service/internal/logging"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
)

// bangkokTZ is the timezone every date in a Thai IG caption is written in.
// The model gets posted_at in it and answers in it, so a bare "2026-09-05"
// becomes midnight Bangkok rather than midnight UTC (which is the day before).
var bangkokTZ = time.FixedZone("Asia/Bangkok", 7*60*60)

// dateOnlyLayout is the shape used when the post gives no time-of-day.
const dateOnlyLayout = "2006-01-02"

// dateTimeLayouts are the shapes an "ISO 8601 datetime" answer arrives in when
// the post does give a time-of-day. The prompt asks for the first one; the
// rest are cheap insurance.
var dateTimeLayouts = []string{
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
	time.RFC3339,
}

// Claude is the Message Batches client. Batch is the right surface here: this
// is a cron job with nobody waiting on it, and batching halves the token bill.
type Claude struct {
	cfg    *config.Configurations
	client anthropic.Client
	logger *logging.TLogger
}

var _ BatchClient = (*Claude)(nil)

func New(cfg *config.Configurations) *Claude {
	return &Claude{
		cfg:    cfg,
		client: anthropic.NewClient(option.WithAPIKey(cfg.LLM.APIKey)),
		logger: logging.New(logging.LayerProvider),
	}
}

// Submit sends every pending post as one batch and returns the batch id.
//
// custom_id is the raw post id as a decimal string. Results come back in an
// arbitrary order, so that id is the only thing tying a result to its post —
// position in the response means nothing.
func (p *Claude) Submit(ctx context.Context, reqs []ExtractionRequest) (string, error) {
	logger := p.logger.SetContext("provider.claude.submit", logging.SetContextOptions{Silent: true})

	if len(reqs) == 0 {
		return "", fmt.Errorf("submit: refusing to create an empty batch")
	}

	requests := make([]anthropic.MessageBatchNewParamsRequest, 0, len(reqs))
	for _, req := range reqs {
		requests = append(requests, anthropic.MessageBatchNewParamsRequest{
			CustomID: strconv.FormatInt(req.RawPostID, 10),
			Params: anthropic.MessageBatchNewParamsRequestParams{
				Model:     anthropic.Model(p.cfg.LLM.Model),
				MaxTokens: p.cfg.LLM.MaxTokens,
				System: []anthropic.TextBlockParam{{
					Text: systemPrompt,
					// The system prompt is byte-identical for every request in
					// the batch, so it is written to cache once and read back
					// for the rest instead of being billed N times.
					CacheControl: anthropic.NewCacheControlEphemeralParam(),
				}},
				Messages: []anthropic.MessageParam{
					anthropic.NewUserMessage(anthropic.NewTextBlock(
						userMessage(req.PostedAt.In(bangkokTZ).Format(time.RFC3339), req.Caption),
					)),
				},
			},
		})
	}

	batch, err := p.client.Messages.Batches.New(ctx, anthropic.MessageBatchNewParams{Requests: requests})
	if err != nil {
		logger.Error(logging.Meta{Message: "Failed to submit batch", Data: map[string]any{"requests": len(requests)}, Error: err})
		return "", fmt.Errorf("submit message batch: %w", err)
	}

	return batch.ID, nil
}

// Poll reports whether the batch has finished processing.
func (p *Claude) Poll(ctx context.Context, batchID string) (BatchStatus, error) {
	logger := p.logger.SetContext("provider.claude.poll", logging.SetContextOptions{Silent: true})

	batch, err := p.client.Messages.Batches.Get(ctx, batchID)
	if err != nil {
		logger.Warn(logging.Meta{Message: "Failed to poll batch", Data: map[string]any{"batch_id": batchID}, Error: err})
		return BatchStatus{}, fmt.Errorf("poll message batch %s: %w", batchID, err)
	}

	return BatchStatus{
		Ended:     batch.ProcessingStatus == anthropic.MessageBatchProcessingStatusEnded,
		Succeeded: batch.RequestCounts.Succeeded,
		Errored:   batch.RequestCounts.Errored,
		Canceled:  batch.RequestCounts.Canceled,
		Expired:   batch.RequestCounts.Expired,
	}, nil
}

// FetchResults streams the batch's .jsonl output and decodes every line.
//
// A per-item failure is carried in ExtractionResult.Error rather than returned:
// one post that came back as prose must not cost the other 4,999 their results.
// Only a failure to reach the results at all is a returned error.
func (p *Claude) FetchResults(ctx context.Context, batchID string) ([]ExtractionResult, error) {
	logger := p.logger.SetContext("provider.claude.fetchResults", logging.SetContextOptions{Silent: true})

	stream := p.client.Messages.Batches.ResultsStreaming(ctx, batchID)
	defer stream.Close()

	results := make([]ExtractionResult, 0)
	for stream.Next() {
		entry := stream.Current()

		rawPostID, err := strconv.ParseInt(entry.CustomID, 10, 64)
		if err != nil {
			// Nothing to attribute this line to, so there is no post to mark
			// failed either — all we can do is say that it happened.
			logger.Error(logging.Meta{
				Message: "batch result has an unusable custom_id",
				Data:    map[string]any{"batch_id": batchID, "custom_id": entry.CustomID},
				Error:   err,
			})
			continue
		}

		results = append(results, decodeEntry(rawPostID, entry))
	}
	if err := stream.Err(); err != nil {
		logger.Error(logging.Meta{Message: "Failed to read batch results", Data: map[string]any{"batch_id": batchID}, Error: err})
		return nil, fmt.Errorf("read results of message batch %s: %w", batchID, err)
	}

	return results, nil
}

// decodeEntry turns one .jsonl line into an ExtractionResult, folding every
// per-item failure mode into the result's Error field.
func decodeEntry(rawPostID int64, entry anthropic.MessageBatchIndividualResponse) ExtractionResult {
	switch entry.Result.Type {
	case "succeeded":
		// Parsed below.

	case "errored":
		return ExtractionResult{
			RawPostID: rawPostID,
			Error:     fmt.Errorf("batch request errored: %s: %s", entry.Result.Error.Error.Type, entry.Result.Error.Error.Message),
		}

	case "canceled", "expired":
		return ExtractionResult{RawPostID: rawPostID, Error: fmt.Errorf("batch request %s", entry.Result.Type)}

	default:
		return ExtractionResult{RawPostID: rawPostID, Error: fmt.Errorf("unknown batch result type %q", entry.Result.Type)}
	}

	text := messageText(entry.Result.Message)
	if text == "" {
		return ExtractionResult{RawPostID: rawPostID, Error: fmt.Errorf("model returned no text content")}
	}

	var payload extractionPayload
	if err := json.Unmarshal([]byte(stripCodeFence(text)), &payload); err != nil {
		// The raw response goes into the error so a prompt regression is
		// diagnosable from the logs alone, without re-running the batch.
		return ExtractionResult{
			RawPostID: rawPostID,
			Error:     fmt.Errorf("invalid json: %w (response: %s)", err, truncate(text, rawResponseLogLimit)),
		}
	}

	result := ExtractionResult{
		RawPostID:       rawPostID,
		IsEvent:         payload.IsEvent,
		Confidence:      NormalizeConfidence(payload.Confidence),
		Title:           payload.Title,
		VenueName:       payload.VenueName,
		VenueNameTH:     payload.VenueNameTH,
		AddressDetail:   payload.AddressDetail,
		PriceText:       payload.PriceText,
		PriceMin:        sanitizePrice(payload.PriceMin),
		PriceMax:        sanitizePrice(payload.PriceMax),
		Categories:      validateCategories(payload.Categories),
		Tags:            cleanTags(payload.Tags),
		RegistrationURL: payload.RegistrationURL,
	}

	// An unparsable date is not worth failing the whole post over: the title,
	// venue and price are still useful. Drop the date and downgrade confidence,
	// which is the same outcome the "never guess a date" prompt rule asks for.
	if start, hasTime, ok := parseDate(payload.StartDate); ok {
		result.StartDate = start
		result.StartTimeKnown = hasTime && payload.StartTimeKnown
	} else if payload.StartDate != nil {
		result.Confidence = ConfidenceLow
	}
	if end, hasTime, ok := parseDate(payload.EndDate); ok {
		result.EndDate = end
		result.EndTimeKnown = hasTime && payload.EndTimeKnown
	}

	return result
}

// messageText concatenates every text block. Normally there is exactly one.
func messageText(message anthropic.Message) string {
	var builder strings.Builder
	for _, block := range message.Content {
		if text, ok := block.AsAny().(anthropic.TextBlock); ok {
			builder.WriteString(text.Text)
		}
	}
	return strings.TrimSpace(builder.String())
}

// stripCodeFence removes a fenced code block wrapper. The prompt forbids one,
// but models add it often enough that failing a post over punctuation is silly.
func stripCodeFence(text string) string {
	const fence = "```"

	trimmed := strings.TrimSpace(text)
	if !strings.HasPrefix(trimmed, fence) {
		return trimmed
	}

	trimmed = strings.TrimPrefix(trimmed, fence)
	if newline := strings.IndexByte(trimmed, '\n'); newline >= 0 {
		// Drop the language tag that follows the opening fence, e.g. "json".
		trimmed = trimmed[newline+1:]
	}
	return strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(trimmed), fence))
}

// Confidence values written to events.confidence.
const (
	ConfidenceHigh   = "high"
	ConfidenceMedium = "medium"
	ConfidenceLow    = "low"
)

// rawResponseLogLimit caps how much of a malformed response is kept in the
// error. Enough to see what the model actually said, not enough to flood logs.
const rawResponseLogLimit = 500

// NormalizeConfidence keeps events.confidence to the three documented values.
// Anything unexpected becomes low, so it lands in the review queue rather than
// being auto-published on the strength of a typo.
func NormalizeConfidence(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case ConfidenceHigh:
		return ConfidenceHigh
	case ConfidenceMedium:
		return ConfidenceMedium
	default:
		return ConfidenceLow
	}
}

// parseDate resolves a date string in Bangkok time. It reports ok=false for a
// null, empty or unparsable value — the caller decides what that costs.
// hasTime reports whether the value actually carried a time-of-day, which is
// what the *_time_known columns are trusted from rather than the model's own
// flag: a model can say true and still hand back a bare date.
func parseDate(raw *string) (parsed *time.Time, hasTime bool, ok bool) {
	if raw == nil {
		return nil, false, false
	}
	value := strings.TrimSpace(*raw)
	if value == "" || strings.EqualFold(value, "null") {
		return nil, false, false
	}

	for _, layout := range dateTimeLayouts {
		if t, err := time.ParseInLocation(layout, value, bangkokTZ); err == nil {
			return &t, true, true
		}
	}
	if t, err := time.ParseInLocation(dateOnlyLayout, value, bangkokTZ); err == nil {
		return &t, false, true
	}
	return nil, false, false
}

// allowedCategories are the only ids the frontend knows how to render. The
// prompt states this list too, but a prompt is guidance, not a guarantee, so
// anything else the model returns is dropped here rather than trusted.
var allowedCategories = map[string]bool{
	"music":    true,
	"art":      true,
	"workshop": true,
	"market":   true,
	"film":     true,
	"talk":     true,
}

// validateCategories keeps only known ids, trimmed, lowercased and deduped.
func validateCategories(raw []string) []string {
	seen := make(map[string]bool, len(raw))
	out := make([]string, 0, len(raw))
	for _, c := range raw {
		id := strings.ToLower(strings.TrimSpace(c))
		if id == "" || !allowedCategories[id] || seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	return out
}

// sanitizePrice drops prices the model had no business emitting. A negative
// number is never a real ticket price, so it is treated the same as "not
// stated" rather than written to the column as-is.
func sanitizePrice(value *int) *int {
	if value == nil || *value < 0 {
		return nil
	}
	return value
}

// cleanTags trims, drops empties and dedupes — no vocabulary restriction,
// tags are free-form.
func cleanTags(raw []string) []string {
	seen := make(map[string]bool, len(raw))
	out := make([]string, 0, len(raw))
	for _, t := range raw {
		tag := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(t), "#"))
		if tag == "" || seen[tag] {
			continue
		}
		seen[tag] = true
		out = append(out, tag)
	}
	return out
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit] + "…"
}
