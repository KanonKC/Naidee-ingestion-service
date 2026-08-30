package claude

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"
)

// decodeLine builds an entry the way the SDK does — by unmarshalling a real
// .jsonl line — so these tests exercise the actual wire shape rather than a
// hand-assembled struct that might not match it.
func decodeLine(t *testing.T, line string) anthropic.MessageBatchIndividualResponse {
	t.Helper()

	var entry anthropic.MessageBatchIndividualResponse
	if err := json.Unmarshal([]byte(line), &entry); err != nil {
		t.Fatalf("could not decode the test fixture: %v", err)
	}
	return entry
}

func succeededLine(customID, text string) string {
	body, err := json.Marshal(text)
	if err != nil {
		panic(err)
	}
	return `{"custom_id":"` + customID + `","result":{"type":"succeeded","message":{` +
		`"id":"msg_1","type":"message","role":"assistant","model":"claude-haiku-4-5",` +
		`"stop_reason":"end_turn","content":[{"type":"text","text":` + string(body) + `}],` +
		`"usage":{"input_tokens":10,"output_tokens":20}}}}`
}

func TestDecodeEntryParsesAnEvent(t *testing.T) {
	entry := decodeLine(t, succeededLine("42", `{"is_event":true,"confidence":"high","title":"เทศกาลดนตรี","venue_name":"BACC","start_date":"2026-09-05","end_date":null,"price_text":"ฟรี","category":"music","registration_url":null}`))

	result := decodeEntry(42, entry)
	if result.Error != nil {
		t.Fatalf("unexpected error: %v", result.Error)
	}
	if !result.IsEvent || result.Confidence != ConfidenceHigh {
		t.Fatalf("extraction does not match: %+v", result)
	}
	if result.StartDate == nil {
		t.Fatal("expected start_date to be parsed")
	}
	// A bare date must land as midnight *Bangkok*, not midnight UTC — the
	// latter is the previous day locally, which would show events a day early.
	if got := result.StartDate.Format("2006-01-02T15:04:05Z07:00"); got != "2026-09-05T00:00:00+07:00" {
		t.Fatalf("expected the date resolved in Bangkok time, got %s", got)
	}
	if result.EndDate != nil {
		t.Fatalf("expected end_date to stay nil, got %v", result.EndDate)
	}
}

// The prompt forbids code fences, but models add them anyway. Failing a post
// over three backticks would be a silly way to lose data.
func TestDecodeEntryStripsCodeFence(t *testing.T) {
	fenced := "```json\n{\"is_event\":false,\"confidence\":\"low\"}\n```"
	entry := decodeLine(t, succeededLine("7", fenced))

	result := decodeEntry(7, entry)
	if result.Error != nil {
		t.Fatalf("unexpected error: %v", result.Error)
	}
	if result.IsEvent {
		t.Fatal("expected is_event false")
	}
}

// A malformed response must be reported against its own post and carry enough
// of the raw text to diagnose a prompt regression from the logs alone.
func TestDecodeEntryReportsInvalidJSON(t *testing.T) {
	entry := decodeLine(t, succeededLine("8", "I'm sorry, I can't determine that."))

	result := decodeEntry(8, entry)
	if result.Error == nil {
		t.Fatal("expected an error for a non-JSON response")
	}
	if result.RawPostID != 8 {
		t.Fatalf("the error must stay attributed to post 8, got %d", result.RawPostID)
	}
	if !strings.Contains(result.Error.Error(), "I'm sorry") {
		t.Fatalf("expected the raw response in the error, got %q", result.Error)
	}
}

func TestDecodeEntryReportsErroredResult(t *testing.T) {
	line := `{"custom_id":"9","result":{"type":"errored","error":{"type":"error","request_id":"req_1","error":{"type":"invalid_request_error","message":"prompt is too long"}}}}`

	result := decodeEntry(9, decodeLine(t, line))
	if result.Error == nil {
		t.Fatal("expected an error for an errored result")
	}
	if !strings.Contains(result.Error.Error(), "prompt is too long") {
		t.Fatalf("expected the API message in the error, got %q", result.Error)
	}
}

func TestDecodeEntryReportsExpiredResult(t *testing.T) {
	result := decodeEntry(10, decodeLine(t, `{"custom_id":"10","result":{"type":"expired"}}`))
	if result.Error == nil {
		t.Fatal("expected an expired result to be reported as an error")
	}
}

// A date the model invented in some other format is dropped rather than
// guessed at, and the post is downgraded to low confidence — the same outcome
// the "never guess a date" prompt rule asks for.
func TestDecodeEntryDowngradesAnUnparsableDate(t *testing.T) {
	entry := decodeLine(t, succeededLine("11", `{"is_event":true,"confidence":"high","start_date":"เสาร์หน้า"}`))

	result := decodeEntry(11, entry)
	if result.Error != nil {
		t.Fatalf("an unparsable date must not fail the whole post: %v", result.Error)
	}
	if result.StartDate != nil {
		t.Fatalf("expected the date to be dropped, got %v", result.StartDate)
	}
	if result.Confidence != ConfidenceLow {
		t.Fatalf("expected confidence downgraded to low, got %q", result.Confidence)
	}
}

// Anything outside the three documented values has to land in the review queue.
// Treating an unknown value as high would auto-publish on the strength of a typo.
func TestNormalizeConfidence(t *testing.T) {
	cases := map[string]string{
		"high":      ConfidenceHigh,
		"HIGH":      ConfidenceHigh,
		" medium ":  ConfidenceMedium,
		"low":       ConfidenceLow,
		"very high": ConfidenceLow,
		"":          ConfidenceLow,
	}

	for input, want := range cases {
		if got := NormalizeConfidence(input); got != want {
			t.Errorf("NormalizeConfidence(%q) = %q, want %q", input, got, want)
		}
	}
}
