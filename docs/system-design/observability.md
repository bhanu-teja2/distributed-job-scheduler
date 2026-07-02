# Observability

## Logs

API and worker services use structured Zap logs. Logs include worker IDs, job IDs, attempts, and errors.

## Metrics

The API exposes `/metrics` in Prometheus format.

Tracked metrics:

- `jobs_created_total`
- `jobs_completed_total`
- `jobs_failed_total`
- `jobs_dead_lettered_total`
- `job_execution_duration_seconds`
- `worker_claimed_jobs_total`
- `active_workers`

## Health

- `/health` reports process liveness.
- `/ready` checks PostgreSQL readiness.
- `/api/v1/workers` reports Redis heartbeat-backed active workers.

## Troubleshooting

- Check `/ready` before API traffic.
- Check worker logs for claim failures and Redis lock misses.
- Check `locked_until` and `locked_by` when jobs appear stuck.
- Check Kafka UI for lifecycle event visibility.
