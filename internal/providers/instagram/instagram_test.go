package instagram

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"event/ingestion-service/internal/config"
)

// newTestClient points the provider at a stub Graph API.
func newTestClient(t *testing.T, handler http.HandlerFunc) *Instagram {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	provider := New(&config.Configurations{
		Instagram: config.InstagramConfigurations{
			AccessToken: "test-token",
			UserID:      "17841400000000000",
			APIVersion:  "v25.0",
			MediaLimit:  25,
		},
	})
	provider.baseURL = server.URL
	return provider
}

func TestDiscoverMediaSendsTokenInAuthorizationHeader(t *testing.T) {
	var gotAuth, gotQuery, gotPath string
	provider := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotQuery = r.URL.RawQuery
		gotPath = r.URL.Path
		w.Write([]byte(`{"business_discovery":{"id":"1","username":"bacc_bangkok","media":{"data":[]}}}`))
	})

	if _, err := provider.DiscoverMedia(context.Background(), "bacc_bangkok", 25); err != nil {
		t.Fatalf("DiscoverMedia returned an error: %v", err)
	}

	if gotAuth != "Bearer test-token" {
		t.Fatalf("expected a bearer token header, got %q", gotAuth)
	}
	// The token must never land in an access log or proxy cache.
	if strings.Contains(gotQuery, "access_token") {
		t.Fatalf("token must not travel in the query string, got %q", gotQuery)
	}
	if gotPath != "/v25.0/17841400000000000" {
		t.Fatalf("expected the pinned version and our IG user id in the path, got %q", gotPath)
	}
	if !strings.Contains(gotQuery, "business_discovery.username") || !strings.Contains(gotQuery, "media.limit") {
		t.Fatalf("expected a business discovery field expression, got %q", gotQuery)
	}
}

func TestDiscoverMediaParsesMediaAndKeepsRawPayload(t *testing.T) {
	const body = `{
	  "business_discovery": {
	    "id": "17841405309211844",
	    "username": "bacc_bangkok",
	    "media": {
	      "data": [
	        {
	          "id": "17895695668004550",
	          "caption": "นิทรรศการเปิดแล้ววันนี้",
	          "media_type": "IMAGE",
	          "media_url": "https://scontent.cdninstagram.com/signed.jpg",
	          "thumbnail_url": "https://scontent.cdninstagram.com/thumb.jpg",
	          "permalink": "https://www.instagram.com/p/abc/",
	          "timestamp": "2026-08-29T10:30:00+0000",
	          "some_future_field": "must survive in raw_payload"
	        }
	      ]
	    }
	  },
	  "id": "17841400000000000"
	}`

	provider := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(body))
	})

	result, err := provider.DiscoverMedia(context.Background(), "bacc_bangkok", 25)
	if err != nil {
		t.Fatalf("DiscoverMedia returned an error: %v", err)
	}

	if result.Username != "bacc_bangkok" || result.IGUserID != "17841405309211844" {
		t.Fatalf("unexpected discovery result: %+v", result)
	}
	if len(result.Media) != 1 {
		t.Fatalf("expected 1 media item, got %d", len(result.Media))
	}

	media := result.Media[0]
	if media.ID != "17895695668004550" || media.MediaType != "IMAGE" {
		t.Fatalf("unexpected media: %+v", media)
	}
	if media.Caption != "นิทรรศการเปิดแล้ววันนี้" {
		t.Fatalf("thai caption did not survive decoding: %q", media.Caption)
	}

	want := time.Date(2026, 8, 29, 10, 30, 0, 0, time.UTC)
	if !media.Timestamp.UTC().Equal(want) {
		t.Fatalf("expected %v, got %v", want, media.Timestamp.UTC())
	}

	// Fields we do not map today must still be recoverable from raw_payload.
	if !strings.Contains(string(media.Raw), "some_future_field") {
		t.Fatalf("raw payload dropped an unmapped field: %s", media.Raw)
	}
}

func TestDiscoverMediaSkipsUnparsableMediaWithoutFailingTheSource(t *testing.T) {
	const body = `{
	  "business_discovery": {
	    "id": "1", "username": "museumsiam",
	    "media": { "data": [
	      { "id": "a", "media_type": "IMAGE", "permalink": "p", "timestamp": "not-a-timestamp" },
	      { "id": "b", "media_type": "VIDEO", "permalink": "p", "timestamp": "2026-08-29T10:30:00+0000" }
	    ] }
	  }
	}`

	provider := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(body))
	})

	result, err := provider.DiscoverMedia(context.Background(), "museumsiam", 25)
	if err != nil {
		t.Fatalf("one bad node must not fail the whole source: %v", err)
	}
	if len(result.Media) != 1 || result.Media[0].ID != "b" {
		t.Fatalf("expected only the well-formed node, got %+v", result.Media)
	}
}

func TestDiscoverMediaTreatsMissingBusinessDiscoveryAsPermanent(t *testing.T) {
	provider := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		// Personal accounts come back as a 200 with no business_discovery block.
		w.Write([]byte(`{"id":"17841400000000000"}`))
	})

	_, err := provider.DiscoverMedia(context.Background(), "some_personal_account", 25)
	apiErr, ok := err.(*APIError)
	if !ok || apiErr.Kind != ErrPermanent {
		t.Fatalf("expected a permanent APIError, got %v", err)
	}
}

func TestDiscoverMediaClassifiesRateLimitAsFatal(t *testing.T) {
	provider := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":{"message":"Application request limit reached","type":"OAuthException","code":4}}`))
	})

	_, err := provider.DiscoverMedia(context.Background(), "anyone", 25)
	if !IsFatal(err) {
		t.Fatalf("expected a fatal error, got %v", err)
	}
}

func TestDiscoverMediaClassifiesServerErrorsAsTransient(t *testing.T) {
	provider := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		w.Write([]byte(`<html>bad gateway</html>`))
	})

	_, err := provider.DiscoverMedia(context.Background(), "anyone", 25)
	apiErr, ok := err.(*APIError)
	if !ok || apiErr.Kind != ErrTransient {
		t.Fatalf("expected a transient APIError, got %v", err)
	}
}

func TestVerifyTokenSucceedsForAHealthyToken(t *testing.T) {
	provider := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v25.0/me" {
			t.Errorf("expected the /me path, got %q", r.URL.Path)
		}
		w.Write([]byte(`{"id":"17841400000000000"}`))
	})

	if err := provider.VerifyToken(context.Background()); err != nil {
		t.Fatalf("expected the token to verify, got %v", err)
	}
}

func TestVerifyTokenRejectsAnExpiredToken(t *testing.T) {
	provider := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":{"message":"Error validating access token: Session has expired","type":"OAuthException","code":190}}`))
	})

	err := provider.VerifyToken(context.Background())
	apiErr, ok := err.(*APIError)
	if !ok || apiErr.Kind != ErrFatal || apiErr.Code != 190 {
		t.Fatalf("expected a fatal code-190 error, got %v", err)
	}
}
