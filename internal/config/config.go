package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	AppEnv                   string
	APIPort                  string
	WorkerConcurrency        int
	WorkerPollInterval       time.Duration
	WorkerBatchSize          int
	DatabaseURL              string
	RedisAddr                string
	RedisPassword            string
	RedisDB                  int
	KafkaBrokers             []string
	JobLockTTL               time.Duration
	JobDefaultTimeout        time.Duration
	JobDefaultMaxRetries     int
	JobDefaultBackoffSeconds int
	LogLevel                 string
}

func Load() Config {
	postgresHost := env("POSTGRES_HOST", "localhost")
	postgresPort := env("POSTGRES_PORT", "5432")
	postgresUser := env("POSTGRES_USER", "scheduler")
	postgresPassword := env("POSTGRES_PASSWORD", "scheduler")
	postgresDB := env("POSTGRES_DB", "scheduler_db")
	postgresSSLMode := env("POSTGRES_SSL_MODE", "disable")
	databaseURL := env("DATABASE_URL", fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", postgresUser, postgresPassword, postgresHost, postgresPort, postgresDB, postgresSSLMode))

	return Config{
		AppEnv:                   env("APP_ENV", "local"),
		APIPort:                  env("API_PORT", "8080"),
		WorkerConcurrency:        envInt("WORKER_CONCURRENCY", 5),
		WorkerPollInterval:       time.Duration(envInt("WORKER_POLL_INTERVAL_SECONDS", 5)) * time.Second,
		WorkerBatchSize:          envInt("WORKER_BATCH_SIZE", 10),
		DatabaseURL:              databaseURL,
		RedisAddr:                env("REDIS_ADDR", "localhost:6379"),
		RedisPassword:            env("REDIS_PASSWORD", ""),
		RedisDB:                  envInt("REDIS_DB", 0),
		KafkaBrokers:             split(env("KAFKA_BROKERS", "localhost:9092")),
		JobLockTTL:               time.Duration(envInt("JOB_LOCK_TTL_SECONDS", 360)) * time.Second,
		JobDefaultTimeout:        time.Duration(envInt("JOB_DEFAULT_TIMEOUT_SECONDS", 300)) * time.Second,
		JobDefaultMaxRetries:     envInt("JOB_DEFAULT_MAX_RETRIES", 3),
		JobDefaultBackoffSeconds: envInt("JOB_DEFAULT_BACKOFF_SECONDS", 30),
		LogLevel:                 env("LOG_LEVEL", "debug"),
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func split(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
