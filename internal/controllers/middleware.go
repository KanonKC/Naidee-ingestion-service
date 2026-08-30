package controllers

import (
	"crypto/subtle"
	"net/http"

	"event/ingestion-service/internal/config"
	"event/ingestion-service/internal/logging"
)

const adminAPIKeyHeader = "x-api-key"

var middlewareLogger = logging.New(logging.LayerMiddleware)

// AuthenticateAdmin checks the admin API key, mirroring authenticateAdmin in
// blaze-backend. The comparison is constant time so the key cannot be
// recovered by timing the response. Shared by cmd/ingestion and
// cmd/processing — each reads cfg.Admin.APIKey from its own environment, so
// the two binaries authenticate against independent secrets even though the
// mechanism (header name, config field) is the same.
//
// An unset key never authenticates, so a service booted without ADMIN_API_KEY
// cannot be triggered even if the route were somehow reachable.
func AuthenticateAdmin(r *http.Request, cfg *config.Configurations) bool {
	logger := middlewareLogger.SetContext("middleware.admin.authenticate", logging.SetContextOptions{Silent: true})

	presented := r.Header.Get(adminAPIKeyHeader)
	secret := cfg.Admin.APIKey

	if presented == "" || secret == "" {
		logger.Warn(logging.Meta{Message: "Admin authentication rejected, missing key"})
		return false
	}

	// ConstantTimeCompare returns 0 for differing lengths without leaking more.
	return subtle.ConstantTimeCompare([]byte(presented), []byte(secret)) == 1
}
