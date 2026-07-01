# Local Development

Start dependencies and services:

```bash
make up
```

Run migrations:

```bash
export DATABASE_URL="postgres://scheduler:scheduler@localhost:5432/scheduler_db?sslmode=disable"
make migrate-up
```

Run locally without Docker:

```bash
make run-api
make run-worker
```
