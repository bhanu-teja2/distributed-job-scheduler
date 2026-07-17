package server

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/bhanuteja/distributed-job-scheduler/internal/auth"
	"github.com/bhanuteja/distributed-job-scheduler/internal/job"
	"github.com/bhanuteja/distributed-job-scheduler/internal/middleware"
	"github.com/bhanuteja/distributed-job-scheduler/internal/observability"
	"github.com/bhanuteja/distributed-job-scheduler/internal/outbox"
	"github.com/bhanuteja/distributed-job-scheduler/internal/response"
	"github.com/bhanuteja/distributed-job-scheduler/internal/worker"
	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type Options struct {
	Authenticator  auth.Authenticator
	AuthEnabled    bool
	RateLimiter    *auth.RateLimiter
	AllowedOrigins []string
	Outbox         *outbox.Store
	ReadyChecks    []func(context.Context) error
}

func New(log *zap.Logger, db *pgxpool.Pool, jobs *job.Handler, registry worker.Registry, metrics observability.Recorder, options ...Options) http.Handler {
	var opts Options
	if len(options) > 0 {
		opts = options[0]
	}
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Recovery(log))
	r.Use(chimiddleware.RealIP)
	r.Use(observability.HTTPMiddleware)
	r.Use(cors(opts.AllowedOrigins))

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		response.JSON(w, r, http.StatusOK, map[string]string{"status": "ok"})
	})
	r.Get("/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := db.Ping(ctx); err != nil {
			response.Error(w, r, http.StatusServiceUnavailable, "DATABASE_UNAVAILABLE", "database is not ready")
			return
		}
		for _, check := range opts.ReadyChecks {
			if err := check(ctx); err != nil {
				response.Error(w, r, http.StatusServiceUnavailable, "DEPENDENCY_UNAVAILABLE", "dependency is not ready")
				return
			}
		}
		response.JSON(w, r, http.StatusOK, map[string]string{"status": "ready"})
	})
	r.Handle("/metrics", observability.Handler())

	r.Route("/api/v1", func(api chi.Router) {
		api.Use(auth.Middleware(opts.Authenticator, opts.AuthEnabled))
		if opts.RateLimiter != nil {
			api.Use(opts.RateLimiter.Middleware)
		}
		api.Mount("/jobs", jobs.Routes())
		api.Mount("/dead-letter-jobs", jobs.DeadLetterRoutes())
		api.Get("/workers", func(w http.ResponseWriter, r *http.Request) {
			workers, err := registry.ActiveWorkers(r.Context())
			if err != nil {
				response.Error(w, r, http.StatusInternalServerError, "WORKER_REGISTRY_UNAVAILABLE", "worker registry is unavailable")
				return
			}
			metrics.SetActiveWorkers(len(workers))
			response.JSON(w, r, http.StatusOK, workers)
		})
		api.Get("/auth/whoami", func(w http.ResponseWriter, r *http.Request) {
			principal, _ := auth.FromContext(r.Context())
			response.JSON(w, r, http.StatusOK, principal)
		})
		api.Get("/dashboard/summary", func(w http.ResponseWriter, r *http.Request) {
			principal := auth.PrincipalOrDefault(r.Context())
			summary := job.DashboardSummary{StatusCounts: map[job.Status]int64{}}
			rows, err := db.Query(r.Context(), `SELECT status,count(*) FROM jobs WHERE tenant_id=$1 GROUP BY status`, principal.TenantID)
			if err != nil {
				response.Error(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to load summary")
				return
			}
			for rows.Next() {
				var status job.Status
				var count int64
				_ = rows.Scan(&status, &count)
				summary.StatusCounts[status] = count
			}
			rows.Close()
			workers, _ := registry.ActiveWorkers(r.Context())
			summary.ActiveWorkers = len(workers)
			_ = db.QueryRow(r.Context(), `SELECT count(*) FROM job_attempts WHERE tenant_id=$1 AND status='FAILED' AND failed_at>=now()-interval '24 hours'`, principal.TenantID).Scan(&summary.FailuresLast24Hour)
			if opts.Outbox != nil {
				summary.OutboxBacklog, _ = opts.Outbox.Backlog(r.Context(), &principal.TenantID)
			}
			response.JSON(w, r, http.StatusOK, summary)
		})
		api.Post("/events/{eventID}/replay", auth.Require(auth.RoleAdmin, func(w http.ResponseWriter, r *http.Request) {
			if opts.Outbox == nil {
				response.Error(w, r, http.StatusServiceUnavailable, "DEPENDENCY_UNAVAILABLE", "outbox unavailable")
				return
			}
			id, err := uuid.Parse(chi.URLParam(r, "eventID"))
			if err != nil {
				response.Error(w, r, http.StatusBadRequest, "INVALID_UUID", "eventID must be a UUID")
				return
			}
			principal := auth.PrincipalOrDefault(r.Context())
			if err := opts.Outbox.Replay(r.Context(), principal.TenantID, id); err != nil {
				response.Error(w, r, http.StatusNotFound, "NOT_FOUND", "event not found")
				return
			}
			response.JSON(w, r, http.StatusOK, map[string]string{"event_id": id.String(), "status": "queued"})
		}))
	})
	return r
}

func cors(origins []string) func(http.Handler) http.Handler {
	allowed := map[string]struct{}{}
	for _, origin := range origins {
		allowed[strings.TrimSpace(origin)] = struct{}{}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if _, ok := allowed[origin]; ok && origin != "" {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Vary", "Origin")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-API-Key, Idempotency-Key")
				w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
			}
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
