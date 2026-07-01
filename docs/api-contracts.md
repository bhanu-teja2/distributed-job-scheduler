# API Contracts

Base path: `/api/v1`

- `POST /jobs` creates a job.
- `GET /jobs` lists jobs with `status`, `job_type`, `page`, and `page_size` filters.
- `GET /jobs/{job_id}` returns one job.
- `GET /jobs/{job_id}/attempts` returns execution attempts.
- `GET /dead-letter-jobs` lists dead-letter rows.
- `POST /dead-letter-jobs/{id}/requeue` creates a fresh job from a dead-letter row.

Responses use:

```json
{"success":true,"data":{},"error":null,"request_id":"uuid"}
```
