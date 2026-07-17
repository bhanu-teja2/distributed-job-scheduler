# Load and Concurrency Evidence

Run `make load-test` after `make up` to submit jobs, then compare job, attempt, and webhook receipt counts. Each run receives a unique `RUN_ID`, so repeated benchmarks do not replay an earlier idempotency key. The acceptance condition is zero duplicate job claims, no stuck `RUNNING` attempts after lease recovery, and an empty outbox after Kafka recovers.

## Recorded Local Baseline

Measured on 2026-07-17 on an arm64 macOS 26.5.2 host using OrbStack, PostgreSQL 16.4, Redis 7.2.5, Kafka 7.6.1, one worker replica, concurrency 5, claim batch size 10, and a five-second polling interval.

| Measurement | Result |
| --- | ---: |
| Jobs submitted | 100 |
| Submission time | 1.531 s |
| Submission throughput | 65.3 jobs/s |
| Successful jobs | 100 |
| Successful attempts / distinct jobs | 100 / 100 |
| Completion latency p50 / p95 / max | 26.623 / 48.391 / 48.459 s |
| Webhook execution p50 / p95 | 5.0 / 11.8 ms |
| Outbox publication p50 / p95 | 0.553 / 1.038 s |
| Outbox backlog after completion | 0 |
| Maximum publication attempts | 1 |

Completion latency includes the deliberately conservative five-second worker polling interval. This is a reproducible development baseline, not a cloud capacity claim; rerun it on the target hardware before quoting production throughput.
