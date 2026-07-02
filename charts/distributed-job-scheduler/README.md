# Distributed Job Scheduler Helm Chart

Install with external PostgreSQL, Redis, and Kafka:

```bash
helm install scheduler charts/distributed-job-scheduler \
  --set api.image.repository=ghcr.io/bhanu-teja2/distributed-job-scheduler-api \
  --set worker.image.repository=ghcr.io/bhanu-teja2/distributed-job-scheduler-worker \
  --set api.image.tag=latest \
  --set worker.image.tag=latest \
  --set postgres.host=postgres.default.svc.cluster.local \
  --set redis.addr=redis.default.svc.cluster.local:6379 \
  --set kafka.brokers=kafka.default.svc.cluster.local:9092
```

Render locally:

```bash
helm lint charts/distributed-job-scheduler
helm template scheduler charts/distributed-job-scheduler
```
