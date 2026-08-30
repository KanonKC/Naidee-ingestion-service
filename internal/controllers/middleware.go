package controllers

import (
	"crypto/subtle"
	"net/http"

	"event/ingestion-service/internal/config"
	"event/ingestion-service/internal/logging"
)

const (
	adminAPIKeyHeader = "x-api-key"
	adminTokenHeader  = "X-Admin-Token"
)

var middlewareLogger = logging.New(logging.LayerMiddleware)

// AuthenticateIngestionAdmin checks cmd/ingestion's admin API key, mirroring
// authenticateAdmin in blaze-backend. The comparison is constant time so the
// key cannot be recovered by timing the response.
//
// An unset key never authenticates, so a service booted without ADMIN_API_KEY
// cannot be triggered even if the route were somehow reachable.
func AuthenticateIngestionAdmin(r *http.Request, cfg *config.Configurations) bool {
	logger := middlewareLogger.SetContext("middleware.admin.authenticateIngestion", logging.SetContextOptions{Silent: true})

	presented := r.Header.Get(adminAPIKeyHeader)
	secret := cfg.Admin.APIKey

	if presented == "" || secret == "" {
		logger.Warn(logging.Meta{Message: "Admin authentication rejected, missing key"})
		return false
	}

	// ConstantTimeCompare returns 0 for differing lengths without leaking more.
	return subtle.ConstantTimeCompare([]byte(presented), []byte(secret)) == 1
}

// AuthenticateProcessingAdmin checks cmd/processing's admin token, mirroring
// authenticateAdmin in blaze-backend. The comparison is constant time so the
// token cannot be recovered by timing the response.
//
// There is no user/role system on purpose: this is an internal tool with one
// operator. An unset token never authenticates, so a service booted without
// ADMIN_API_TOKEN cannot be triggered even if the route were somehow reachable.
func AuthenticateProcessingAdmin(r *http.Request, cfg *config.Configurations) bool {
	logger := middlewareLogger.SetContext("middleware.admin.authenticateProcessing", logging.SetContextOptions{Silent: true})

	presented := r.Header.Get(adminTokenHeader)
	secret := cfg.Admin.APIToken

	if presented == "" || secret == "" {
		logger.Warn(logging.Meta{Message: "Admin authentication rejected, missing token"})
		return false
	}

	// ConstantTimeCompare returns 0 for differing lengths without leaking more.
	return subtle.ConstantTimeCompare([]byte(presented), []byte(secret)) == 1
}
