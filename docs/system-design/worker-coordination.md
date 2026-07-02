# Worker Coordination

PostgreSQL is the primary duplicate-execution guard. Redis adds an execution lock after a job is claimed.

## Claim and Execute

```mermaid
sequenceDiagram
    participant W1 as Worker A
    participant W2 as Worker B
    participant DB as PostgreSQL
    participant R as Redis
    W1->>DB: Claim due jobs FOR UPDATE SKIP LOCKED
    W2->>DB: Claim due jobs FOR UPDATE SKIP LOCKED
    DB-->>W1: job-1
    DB-->>W2: different jobs or none
    W1->>R: SET lock:job:job-1 worker-A NX EX
    R-->>W1: acquired
    W1->>W1: Execute handler with timeout
    W1->>DB: Ownership-aware status update
    W1->>R: Release lock if owner
```

## Crash Recovery

```mermaid
flowchart TD
    A["Worker claims job"] --> B["Worker crashes"]
    B --> C["locked_until expires"]
    C --> D["Recovery marks stale attempt failed"]
    D --> E["Job becomes retry scheduled"]
    E --> F["Another worker claims job"]
```
