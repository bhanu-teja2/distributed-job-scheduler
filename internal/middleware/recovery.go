package middleware

import (
	"net/http"

	"github.com/bhanuteja/distributed-job-scheduler/internal/response"
	"go.uber.org/zap"
)

// Recovery converts panics into a logged HTTP 500 response without terminating
// the API process.
func Recovery(log *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if recovered := recover(); recovered != nil {
					log.Error("panic recovered", zap.Any("panic", recovered))
					response.Error(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "internal server error")
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}
