#!/usr/bin/env sh
set -eu
: "${API_URL:=http://localhost:8080}"
: "${API_KEY:=djs_local_development_key_change_me}"
RUN_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
curl -sS -X POST "$API_URL/api/v1/jobs" -H "Content-Type: application/json" -H "X-API-Key: $API_KEY" -H "Idempotency-Key: seed-webhook-1" -d "{\"name\":\"Seed webhook\",\"job_type\":\"CALL_WEBHOOK\",\"payload\":{\"url\":\"http://webhook-sink:8080/seed\",\"method\":\"POST\",\"body\":{\"source\":\"seed\"}},\"run_at\":\"$RUN_AT\",\"priority\":5}"
