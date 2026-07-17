package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bhanuteja/distributed-job-scheduler/internal/auth"
	"github.com/bhanuteja/distributed-job-scheduler/internal/config"
	"github.com/bhanuteja/distributed-job-scheduler/internal/job"
	"github.com/bhanuteja/distributed-job-scheduler/internal/logger"
	"github.com/bhanuteja/distributed-job-scheduler/internal/observability"
	"github.com/bhanuteja/distributed-job-scheduler/internal/outbox"
	"github.com/bhanuteja/distributed-job-scheduler/internal/postgres"
	appRedis "github.com/bhanuteja/distributed-job-scheduler/internal/redis"
	"github.com/bhanuteja/distributed-job-scheduler/internal/server"
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
	metrics := observability.NewPrometheusRecorder()
	service := job.NewService(repo, cfg.JobDefaultMaxRetries, cfg.JobDefaultBackoffSeconds, int(cfg.JobDefaultTimeout.Seconds())).WithMetrics(metrics)
	handler := job.NewHandler(service)
	registry := worker.NewRedisRegistry(redisClient)

	httpServer := &http.Server{
		Addr:              ":" + cfg.APIPort,
		Handler:           server.New(log, db, handler, registry, metrics, server.Options{Authenticator: auth.NewStore(db), AuthEnabled: cfg.AuthEnabled, RateLimiter: auth.NewRateLimiter(redisClient, cfg.RateLimitPerMinute), AllowedOrigins: cfg.CORSAllowedOrigins, Outbox: outbox.NewStore(db), ReadyChecks: []func(context.Context) error{func(ctx context.Context) error { return redisClient.Ping(ctx).Err() }}}),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info("api server started", zap.String("addr", httpServer.Addr))
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal("api server failed", zap.Error(err))
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Error("api shutdown failed", zap.Error(err))
	}
}
