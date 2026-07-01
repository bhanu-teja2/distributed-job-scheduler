package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bhanuteja/distributed-job-scheduler/internal/config"
	"github.com/bhanuteja/distributed-job-scheduler/internal/job"
	"github.com/bhanuteja/distributed-job-scheduler/internal/logger"
	"github.com/bhanuteja/distributed-job-scheduler/internal/postgres"
	"github.com/bhanuteja/distributed-job-scheduler/internal/server"
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
	service := job.NewService(repo, cfg.JobDefaultMaxRetries, cfg.JobDefaultBackoffSeconds, int(cfg.JobDefaultTimeout.Seconds()))
	handler := job.NewHandler(service)

	httpServer := &http.Server{
		Addr:              ":" + cfg.APIPort,
		Handler:           server.New(log, db, handler),
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
