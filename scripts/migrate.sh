#!/usr/bin/env sh
set -eu

: "${DATABASE_URL:=postgres://scheduler:scheduler@localhost:5432/scheduler_db?sslmode=disable}"

migrate -path migrations -database "$DATABASE_URL" up
