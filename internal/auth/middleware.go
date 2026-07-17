package auth

import (
	"net/http"
	"strings"

	"github.com/bhanuteja/distributed-job-scheduler/internal/response"
)

func Middleware(authenticator Authenticator, enabled bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !enabled {
				next.ServeHTTP(w, r.WithContext(WithPrincipal(r.Context(), PrincipalOrDefault(r.Context()))))
				return
			}
			key := strings.TrimSpace(r.Header.Get("X-API-Key"))
			if key == "" {
				response.Error(w, r, http.StatusUnauthorized, "UNAUTHORIZED", "X-API-Key is required")
				return
			}
			principal, err := authenticator.Authenticate(r.Context(), key)
			if err != nil {
				response.Error(w, r, http.StatusUnauthorized, "UNAUTHORIZED", "invalid API key")
				return
			}
			next.ServeHTTP(w, r.WithContext(WithPrincipal(r.Context(), principal)))
		})
	}
}

func Require(role Role, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		principal, ok := FromContext(r.Context())
		if !ok || !principal.Allows(role) {
			response.Error(w, r, http.StatusForbidden, "FORBIDDEN", "insufficient permissions")
			return
		}
		next(w, r)
	}
}
