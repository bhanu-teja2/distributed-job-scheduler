# Deployment Design

```mermaid
flowchart TB
    Ingress["Ingress"] --> Dashboard["Dashboard Deployment"]
    Ingress --> API["API Deployment + HPA"]
    API --> PG[("External PostgreSQL")]
    API --> Redis[("External Redis")]
    Workers["Worker Deployment + HPA"] --> PG
    Workers --> Redis
    Relay["Event Relay Deployment"] --> PG
    Relay --> Kafka[("External Kafka")]
    Migration["Explicit Migration Job"] --> PG
    Prometheus["Prometheus"] -. scrape .-> API
    Prometheus -. scrape .-> Workers
    Prometheus -. scrape .-> Relay
```

The chart supports an externally managed Secret, non-root containers, read-only root filesystems, health probes, resource requests/limits, disruption budgets, optional autoscaling, and a packaged migration image.

The project supports Docker Compose for local development and Helm for Kubernetes deployment.

## Docker Compose

```mermaid
flowchart TB
    API["api container"] --> PG["postgres:16"]
    API --> Redis["redis:7"]
    API --> Kafka["cp-kafka"]
    Worker["worker container"] --> PG
    Worker --> Redis
    Worker --> Kafka
    KafkaUI["kafka-ui"] --> Kafka
    Kafka --> ZK["zookeeper"]
```

## Kubernetes

```mermaid
flowchart TB
    Ingress["Ingress optional"] --> Svc["API Service"]
    Svc --> ApiPods["API Deployment"]
    WorkerPods["Worker Deployment"] --> PG[("PostgreSQL external")]
    ApiPods --> PG
    ApiPods --> Redis[("Redis external")]
    WorkerPods --> Redis
    ApiPods --> Kafka[("Kafka external")]
    WorkerPods --> Kafka
    Config["ConfigMap"] --> ApiPods
    Config --> WorkerPods
    Secret["Secret"] --> ApiPods
    Secret --> WorkerPods
```

The Helm chart keeps migrations disabled by default. Operators can enable the migration Job explicitly during controlled rollout.
