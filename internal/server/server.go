package server

import (
	"context"
	"net/http"
	"time"

	"github.com/bhanuteja/distributed-job-scheduler/internal/job"
	"github.com/bhanuteja/distributed-job-scheduler/internal/middleware"
	"github.com/bhanuteja/distributed-job-scheduler/internal/observability"
	"github.com/bhanuteja/distributed-job-scheduler/internal/response"
	"github.com/bhanuteja/distributed-job-scheduler/internal/worker"
	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

func New(log *zap.Logger, db *pgxpool.Pool, jobs *job.Handler, registry worker.Registry, metrics observability.Recorder) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Recovery(log))
	r.Use(chimiddleware.RealIP)

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
		response.JSON(w, r, http.StatusOK, map[string]string{"status": "ready"})
	})
	r.Handle("/metrics", observability.Handler())

	r.Route("/api/v1", func(api chi.Router) {
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
	})
	return r
}
