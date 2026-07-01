package middleware

import (
	"context"
	"net/http"

	"github.com/bhanuteja/distributed-job-scheduler/internal/response"
	"github.com/google/uuid"
)

func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get("X-Request-ID")
		if requestID == "" {
			requestID = uuid.NewString()
		}
		w.Header().Set("X-Request-ID", requestID)
		ctx := context.WithValue(r.Context(), response.ContextKey(), requestID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
