package controllers

import (
	"crypto/subtle"
	"net/http"

	"event/ingestion-service/internal/processing/config"
	"event/ingestion-service/internal/processing/logging"
)

const adminTokenHeader = "X-Admin-Token"

var middlewareLogger = logging.New(logging.LayerMiddleware)

// AuthenticateAdmin checks the admin token, mirroring authenticateAdmin in
// blaze-backend. The comparison is constant time so the token cannot be
// recovered by timing the response.
//
// There is no user/role system on purpose: this is an internal tool with one
// operator. An unset token never authenticates, so a service booted without
// ADMIN_API_TOKEN cannot be triggered even if the route were somehow reachable.
func AuthenticateAdmin(r *http.Request, cfg *config.Configurations) bool {
	logger := middlewareLogger.SetContext("middleware.admin.authenticate", logging.SetContextOptions{Silent: true})

	presented := r.Header.Get(adminTokenHeader)
	secret := cfg.Admin.APIToken

	if presented == "" || secret == "" {
		logger.Warn(logging.Meta{Message: "Admin authentication rejected, missing token"})
		return false
	}

	// ConstantTimeCompare returns 0 for differing lengths without leaking more.
	return subtle.ConstantTimeCompare([]byte(presented), []byte(secret)) == 1
}
