#!/usr/bin/env sh
set -eu

: "${API_URL:=http://localhost:8080}"
: "${COUNT:=25}"

i=1
while [ "$i" -le "$COUNT" ]; do
  curl -sS -o /dev/null -X POST "$API_URL/api/v1/jobs" \
    -H "Content-Type: application/json" \
    -d "{
      \"name\": \"Load test email $i\",
      \"job_type\": \"SEND_EMAIL\",
      \"payload\": {\"to\":\"user$i@example.com\",\"subject\":\"Load test\",\"body\":\"Hello\"},
      \"run_at\": \"2026-07-01T12:00:00Z\",
      \"priority\": 5
    }"
  i=$((i + 1))
done
