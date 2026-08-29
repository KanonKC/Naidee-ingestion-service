package instagram

import "testing"

func TestClassify(t *testing.T) {
	cases := []struct {
		name       string
		code       int
		httpStatus int
		want       ErrorKind
	}{
		{"not a business account", 24, 400, ErrPermanent},
		{"user not found", 110, 400, ErrPermanent},
		{"unknown api error", 1, 500, ErrTransient},
		{"temporary outage", 2, 500, ErrTransient},
		{"app rate limit", 4, 400, ErrFatal},
		{"user rate limit", 17, 400, ErrFatal},
		{"page rate limit", 32, 400, ErrFatal},
		{"expired token", 190, 400, ErrFatal},
		{"http 429 without a code", 0, 429, ErrFatal},
		{"http 503 without a code", 0, 503, ErrTransient},
		{"app lacks permission", 10, 400, ErrFatal},
		{"permission block lower bound", 200, 403, ErrFatal},
		{"permission block upper bound", 299, 403, ErrFatal},
		{"unrecognised code defaults to transient", 9999, 400, ErrTransient},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classify(tc.code, tc.httpStatus); got != tc.want {
				t.Fatalf("classify(%d, %d) = %v, want %v", tc.code, tc.httpStatus, got, tc.want)
			}
		})
	}
}

func TestDecodeGraphError(t *testing.T) {
	t.Run("returns nil for a successful response", func(t *testing.T) {
		body := []byte(`{"business_discovery":{"id":"1","username":"someone"}}`)
		if err := decodeGraphError(body, 200); err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	})

	t.Run("classifies an error envelope", func(t *testing.T) {
		body := []byte(`{"error":{"message":"Unsupported get request.","type":"GraphMethodException","code":100,"error_subcode":33}}`)
		apiErr := decodeGraphError(body, 400)
		if apiErr == nil {
			t.Fatal("expected an APIError, got nil")
		}
		if apiErr.Code != 100 {
			t.Fatalf("expected code 100, got %d", apiErr.Code)
		}
	})

	t.Run("classifies an expired token as fatal", func(t *testing.T) {
		body := []byte(`{"error":{"message":"Error validating access token","type":"OAuthException","code":190}}`)
		apiErr := decodeGraphError(body, 400)
		if apiErr == nil || apiErr.Kind != ErrFatal {
			t.Fatalf("expected a fatal APIError, got %v", apiErr)
		}
	})

	t.Run("falls back to the http status when the body is not an error envelope", func(t *testing.T) {
		apiErr := decodeGraphError([]byte(`<html>gateway timeout</html>`), 504)
		if apiErr == nil || apiErr.Kind != ErrTransient {
			t.Fatalf("expected a transient APIError, got %v", apiErr)
		}
	})
}

func TestIsFatal(t *testing.T) {
	if !IsFatal(&APIError{Kind: ErrFatal}) {
		t.Fatal("expected a fatal APIError to report as fatal")
	}
	if IsFatal(&APIError{Kind: ErrTransient}) {
		t.Fatal("expected a transient APIError not to report as fatal")
	}
}

func TestRemediationHint(t *testing.T) {
	// A permission error is about our app, not the account, so the hint has to
	// point at the app setup rather than at the source.
	if hint := RemediationHint(10); hint == "" {
		t.Fatal("code 10 must carry a remediation hint")
	}
	if hint := RemediationHint(190); hint == "" {
		t.Fatal("code 190 must carry a remediation hint")
	}
	if hint := RemediationHint(9999); hint != "" {
		t.Fatalf("an unknown code should have no hint, got %q", hint)
	}
}

// Permission errors must never deactivate sources: they fail identically for
// every account, so treating one as permanent would empty the whitelist.
func TestPermissionErrorsAreFatalNotPermanent(t *testing.T) {
	for _, code := range []int{10, 200, 250, 299} {
		if got := classify(code, 400); got == ErrPermanent {
			t.Fatalf("code %d classified as permanent, which would deactivate a healthy source", code)
		}
		if got := classify(code, 400); got != ErrFatal {
			t.Fatalf("code %d should be fatal, got %v", code, got)
		}
	}
}
