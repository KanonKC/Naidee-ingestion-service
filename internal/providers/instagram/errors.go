package instagram

import "fmt"

// ErrorKind decides what the ingestion flow does next, which is the only reason
// this classification exists.
type ErrorKind int

const (
	// ErrPermanent — the source itself is broken (not a business account, user
	// gone). Deactivate it and move on; retrying will never help.
	ErrPermanent ErrorKind = iota
	// ErrTransient — worth another attempt with backoff.
	ErrTransient
	// ErrFatal — the whole run must stop. Hammering a rate-limited or
	// expired-token API only lengthens the cooldown.
	ErrFatal
)

func (k ErrorKind) String() string {
	switch k {
	case ErrPermanent:
		return "permanent"
	case ErrTransient:
		return "transient"
	case ErrFatal:
		return "fatal"
	default:
		return "unknown"
	}
}

// APIError is the classified form of every failure this provider returns.
type APIError struct {
	Kind    ErrorKind
	Code    int
	Message string
	// Err is the underlying transport error, when there was one.
	Err error
}

func (e *APIError) Error() string {
	return fmt.Sprintf("instagram api error (kind=%s code=%d): %s", e.Kind, e.Code, e.Message)
}

func (e *APIError) Unwrap() error {
	return e.Err
}

// Meta Graph API error codes, grouped by how we have to react to them.
var (
	permanentCodes = map[int]bool{
		24:  true, // target is not a business/creator account
		110: true, // user not found
	}
	transientCodes = map[int]bool{
		1: true, // unknown/temporary API error
		2: true, // temporary service outage
	}
	fatalCodes = map[int]bool{
		4:   true, // application request limit reached
		17:  true, // user request limit reached
		32:  true, // page request limit reached
		190: true, // access token expired or invalid
		10:  true, // app lacks permission for this action
	}
)

// isPermissionCode reports Meta's documented 200-299 permission block: the app
// or token is missing a scope the call requires.
func isPermissionCode(code int) bool {
	return code >= 200 && code <= 299
}

// classify maps a Graph API error code plus HTTP status onto a kind.
//
// Permission errors (code 10, and the 200-299 block) are fatal, not permanent.
// They describe our app or token, not the account being looked up, so they fail
// identically for every source. Classifying them as permanent would walk the
// whole whitelist deactivating healthy accounts one by one.
//
// Unknown codes are treated as transient on purpose: retrying then letting
// consecutive_failures trip the auto-deactivate threshold is far safer than
// deactivating a healthy source on the first unrecognised error.
func classify(code, httpStatus int) ErrorKind {
	switch {
	case fatalCodes[code]:
		return ErrFatal
	case isPermissionCode(code):
		return ErrFatal
	case permanentCodes[code]:
		return ErrPermanent
	case transientCodes[code]:
		return ErrTransient
	case httpStatus == 429:
		return ErrFatal
	case httpStatus >= 500:
		return ErrTransient
	default:
		return ErrTransient
	}
}

// IsFatal reports whether err aborts the entire run.
func IsFatal(err error) bool {
	apiErr, ok := err.(*APIError)
	return ok && apiErr.Kind == ErrFatal
}

// RemediationHint returns operator-facing guidance for the fatal codes whose fix
// is not obvious from the API message alone. It returns "" when there is none.
func RemediationHint(code int) string {
	switch {
	case code == 190:
		return "The long-lived access token is expired or invalid. A new OAuth flow is required."
	case code == 10 || isPermissionCode(code):
		return "The app or token is missing a permission Business Discovery needs. Check that the token carries instagram_basic, pages_show_list and pages_read_engagement, that the IG account is a Business/Creator account linked to a Facebook Page, and that IG_USER_ID is the Instagram Business Account ID (17841...) rather than the Facebook Page ID."
	case code == 4 || code == 17 || code == 32:
		return "Rate limit reached. Wait for the cooldown before triggering another run."
	default:
		return ""
	}
}
