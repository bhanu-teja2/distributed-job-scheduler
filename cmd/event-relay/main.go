package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bhanuteja/distributed-job-scheduler/internal/config"
	"github.com/bhanuteja/distributed-job-scheduler/internal/kafka"
	"github.com/bhanuteja/distributed-job-scheduler/internal/logger"
	"github.com/bhanuteja/distributed-job-scheduler/internal/observability"
	"github.com/bhanuteja/distributed-job-scheduler/internal/outbox"
	"github.com/bhanuteja/distributed-job-scheduler/internal/postgres"
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
	// Relay publication is isolated from API and worker transactions; durable
	// rows remain available for retry whenever Kafka is unavailable.
	publisher := kafka.NewProducer(cfg.KafkaBrokers, cfg.KafkaEventsTopic)
	defer func() { _ = publisher.Close() }()
	relay := outbox.NewRelay(outbox.NewStore(db), publisher, log, cfg.EventRelayBatchSize, cfg.EventRelayPollInterval)
	server := &http.Server{Addr: ":" + cfg.EventRelayHealthPort, ReadHeaderTimeout: 3 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			w.WriteHeader(http.StatusOK)
		case "/ready":
			c, cancel := context.WithTimeout(r.Context(), time.Second)
			defer cancel()
			if db.Ping(c) != nil || publisher.Ping(c) != nil {
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
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("relay health server failed", zap.Error(err))
		}
	}()
	if err := relay.Run(ctx); err != nil {
		log.Fatal("event relay failed", zap.Error(err))
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdown)
}
