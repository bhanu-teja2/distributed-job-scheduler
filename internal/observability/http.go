package observability

import (
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"net/http"
	"strconv"
	"time"
)

var httpRequests = promauto.NewCounterVec(prometheus.CounterOpts{Name: "http_requests_total", Help: "HTTP requests by method, route, and status."}, []string{"method", "route", "status"})
var httpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: "http_request_duration_seconds", Help: "HTTP request duration by method and route."}, []string{"method", "route"})

func HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writer := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		started := time.Now()
		next.ServeHTTP(writer, r)
		route := chi.RouteContext(r.Context()).RoutePattern()
		if route == "" {
			route = "unmatched"
		}
		httpRequests.WithLabelValues(r.Method, route, strconv.Itoa(writer.Status())).Inc()
		httpDuration.WithLabelValues(r.Method, route).Observe(time.Since(started).Seconds())
	})
}
