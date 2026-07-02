# Deployment Design

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
