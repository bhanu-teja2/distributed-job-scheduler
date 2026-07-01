.PHONY: up down build logs logs-api logs-worker test race fmt lint migrate-up migrate-down seed load-test run-api run-worker

up:
	docker compose up -d

down:
	docker compose down

build:
	docker compose build

logs:
	docker compose logs -f

logs-api:
	docker compose logs -f api

logs-worker:
	docker compose logs -f worker

test:
	go test ./...

race:
	go test -race ./...

fmt:
	go fmt ./...

lint:
	golangci-lint run

migrate-up:
	migrate -path migrations -database "$$DATABASE_URL" up

migrate-down:
	migrate -path migrations -database "$$DATABASE_URL" down

seed:
	./scripts/seed_jobs.sh

load-test:
	./scripts/run_load_test.sh

run-api:
	go run ./cmd/api

run-worker:
	go run ./cmd/worker
