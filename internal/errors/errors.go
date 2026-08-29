// Package errors mirrors the TError hierarchy in blaze-backend. Import it
// alongside the standard library package with an alias, e.g.
//
//	stderrors "errors"
//	"event/ingestion-service/internal/errors"
package errors

import (
	stderrors "errors"
	"net/http"
)

// TError is the base typed error carrying an HTTP status and a domain code.
type TError struct {
	Message   string `json:"message"`
	Status    int    `json:"status"`
	ErrorCode string `json:"error_code"`
}

func (e *TError) Error() string {
	return e.Message
}

func New(message string, status int) *TError {
	return &TError{Message: message, Status: status, ErrorCode: "0"}
}

func NewForbiddenError(message string) *TError {
	return New(orDefault(message, "Forbidden"), http.StatusForbidden)
}

func NewNotFoundError(message string) *TError {
	return New(orDefault(message, "Not Found"), http.StatusNotFound)
}

func NewBadRequestError(message string) *TError {
	return New(orDefault(message, "Bad Request"), http.StatusBadRequest)
}

func NewInternalServerError(message string) *TError {
	return New(orDefault(message, "Internal Server Error"), http.StatusInternalServerError)
}

func NewUnauthorizedError(message string) *TError {
	return New(orDefault(message, "Unauthorized"), http.StatusUnauthorized)
}

func NewBadGatewayError(message string) *TError {
	return New(orDefault(message, "Bad Gateway"), http.StatusBadGateway)
}

// AsTError reports whether err is (or wraps) a TError.
func AsTError(err error) (*TError, bool) {
	var target *TError
	if stderrors.As(err, &target) {
		return target, true
	}
	return nil, false
}

func orDefault(message, fallback string) string {
	if message == "" {
		return fallback
	}
	return message
}
