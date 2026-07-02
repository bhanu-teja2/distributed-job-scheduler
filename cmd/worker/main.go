package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/bhanuteja/distributed-job-scheduler/internal/config"
	"github.com/bhanuteja/distributed-job-scheduler/internal/job"
	"github.com/bhanuteja/distributed-job-scheduler/internal/kafka"
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
	executor := worker.NewExecutor()
	publisher := kafka.NewProducer(cfg.KafkaBrokers, cfg.KafkaEventsTopic)
	defer func() { _ = publisher.Close() }()
	service := worker.NewService(repo, executor, log, "", cfg.WorkerConcurrency, cfg.WorkerBatchSize, cfg.WorkerPollInterval, cfg.JobLockTTL).
		WithLockManager(lock.NewRedisLock(redisClient)).
		WithPublisher(publisher).
		WithMetrics(observability.NewPrometheusRecorder()).
		WithRegistry(worker.NewRedisRegistry(redisClient), cfg.WorkerHeartbeatTTL, cfg.WorkerHeartbeatInterval)
	if err := service.Run(ctx); err != nil {
		log.Fatal("worker failed", zap.Error(err))
	}
}
