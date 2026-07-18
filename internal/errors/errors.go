package errors

import "errors"

var (
	// Domain sentinel errors are wrapped with context and translated by the HTTP layer.
	ErrNotFound          = errors.New("not found")
	ErrInvalidInput      = errors.New("invalid input")
	ErrInvalidTransition = errors.New("invalid status transition")
	ErrConflict          = errors.New("conflict")
	ErrIdempotency       = errors.New("idempotency conflict")
	ErrUnauthorized      = errors.New("unauthorized")
	ErrForbidden         = errors.New("forbidden")
	ErrDependency        = errors.New("dependency unavailable")
)
