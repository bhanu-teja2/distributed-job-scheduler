package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bhanuteja/distributed-job-scheduler/internal/config"
	"github.com/bhanuteja/distributed-job-scheduler/internal/job"
	"github.com/bhanuteja/distributed-job-scheduler/internal/lock"
	"github.com/bhanuteja/distributed-job-scheduler/internal/logger"
	"github.com/bhanuteja/distributed-job-scheduler/internal/observability"
	"github.com/bhanuteja/distributed-job-scheduler/internal/postgres"
	appRedis "github.com/bhanuteja/distributed-job-scheduler/internal/redis"
	"github.com/bhanuteja/distributed-job-scheduler/internal/worker"
	"go.uber.org/zap"
)

func main() {
	cfg := config.Load()
	log, err := logger.New(cfg.LogLevel)
	if err != nil {
		panic(err)
	}
	defer func() { _ = log.Sync() }()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := postgres.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatal("failed to connect postgres", zap.Error(err))
	}
	defer db.Close()
	redisClient, err := appRedis.Connect(ctx, cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	if err != nil {
		log.Fatal("failed to connect redis", zap.Error(err))
	}
	defer func() { _ = redisClient.Close() }()

	repo := job.NewPostgresRepository(db)
	executor := worker.NewExecutorWithOptions(worker.ExecutorOptions{AllowedHosts: cfg.WebhookAllowedHosts, AllowPrivateNetworks: cfg.WebhookAllowPrivate})
	service := worker.NewService(repo, executor, log, "", cfg.WorkerConcurrency, cfg.WorkerBatchSize, cfg.WorkerPollInterval, cfg.JobLockTTL).
		WithLockManager(lock.NewRedisLock(redisClient)).
		WithMetrics(observability.NewPrometheusRecorder()).
		WithRegistry(worker.NewRedisRegistry(redisClient), cfg.WorkerHeartbeatTTL, cfg.WorkerHeartbeatInterval)
	health := &http.Server{Addr: ":" + cfg.WorkerHealthPort, ReadHeaderTimeout: 3 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			w.WriteHeader(http.StatusOK)
		case "/ready":
			check, cancel := context.WithTimeout(r.Context(), time.Second)
			defer cancel()
			if db.Ping(check) != nil || redisClient.Ping(check).Err() != nil {
				http.Error(w, "not ready", http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
		case "/metrics":
			observability.Handler().ServeHTTP(w, r)
		default:
			http.NotFound(w, r)
		}
	})}
	go func() {
		if err := health.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("worker health server failed", zap.Error(err))
		}
	}()
	if err := service.Run(ctx); err != nil {
		log.Fatal("worker failed", zap.Error(err))
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = health.Shutdown(shutdown)
}
