package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/bhanuteja/distributed-job-scheduler/internal/config"
	"github.com/bhanuteja/distributed-job-scheduler/internal/job"
	"github.com/bhanuteja/distributed-job-scheduler/internal/logger"
	"github.com/bhanuteja/distributed-job-scheduler/internal/postgres"
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

	repo := job.NewPostgresRepository(db)
	executor := worker.NewExecutor()
	service := worker.NewService(repo, executor, log, "", cfg.WorkerConcurrency, cfg.WorkerBatchSize, cfg.WorkerPollInterval, cfg.JobLockTTL)
	if err := service.Run(ctx); err != nil {
		log.Fatal("worker failed", zap.Error(err))
	}
}
