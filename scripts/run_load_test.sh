#!/usr/bin/env sh
set -eu
: "${API_URL:=http://localhost:8080}"
: "${API_KEY:=djs_local_development_key_change_me}"
: "${COUNT:=100}"
: "${RUN_ID:=$(date -u +%Y%m%dT%H%M%SZ)}"
RUN_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
i=1
while [ "$i" -le "$COUNT" ]; do
  curl -fsS -o /dev/null -X POST "$API_URL/api/v1/jobs" -H "Content-Type: application/json" -H "X-API-Key: $API_KEY" -H "Idempotency-Key: load-$RUN_ID-$i" -d "{\"name\":\"Load webhook $RUN_ID $i\",\"job_type\":\"CALL_WEBHOOK\",\"payload\":{\"url\":\"http://webhook-sink:8080/load/$RUN_ID/$i\",\"method\":\"POST\",\"body\":{\"run_id\":\"$RUN_ID\",\"sequence\":$i}},\"run_at\":\"$RUN_AT\",\"priority\":5,\"timeout_seconds\":20}"
  i=$((i + 1))
done
echo "run_id=$RUN_ID submitted=$COUNT"
