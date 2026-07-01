#!/usr/bin/env sh
set -eu

: "${API_URL:=http://localhost:8080}"

curl -sS -X POST "$API_URL/api/v1/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Send welcome email",
    "job_type": "SEND_EMAIL",
    "payload": {"to":"user@example.com","subject":"Welcome","body":"Hello from the scheduler"},
    "run_at": "2026-07-01T12:00:00Z",
    "priority": 5
  }'
