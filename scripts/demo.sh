#!/usr/bin/env sh
set -eu
: "${API_URL:=http://localhost:8080}"
: "${API_KEY:=djs_local_development_key_change_me}"
RUN_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
request() { curl -fsS "$@" -H "Content-Type: application/json" -H "X-API-Key: $API_KEY"; }
echo "Creating a successful webhook job"
request -X POST "$API_URL/api/v1/jobs" -H "Idempotency-Key: demo-success" -d "{\"name\":\"Successful demo webhook\",\"job_type\":\"CALL_WEBHOOK\",\"payload\":{\"url\":\"http://webhook-sink:8080/demo\",\"method\":\"POST\",\"body\":{\"demo\":true}},\"run_at\":\"$RUN_AT\",\"priority\":10,\"max_retries\":3,\"retry_backoff_seconds\":2,\"timeout_seconds\":10}"
echo "\nCreating a retrying webhook job"
request -X POST "$API_URL/api/v1/jobs" -H "Idempotency-Key: demo-failure" -d "{\"name\":\"Retry and dead-letter demo\",\"job_type\":\"CALL_WEBHOOK\",\"payload\":{\"url\":\"http://webhook-sink:9999/unavailable\",\"method\":\"POST\"},\"run_at\":\"$RUN_AT\",\"priority\":9,\"max_retries\":2,\"retry_backoff_seconds\":2,\"timeout_seconds\":5}"
echo "\nDashboard: http://localhost:3000"
