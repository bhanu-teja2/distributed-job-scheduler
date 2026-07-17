# API Contracts

The authoritative OpenAPI 3.1 contract is [openapi.yaml](openapi.yaml). All `/api/v1` endpoints require `X-API-Key`; mutation endpoints require operator or admin role. Job creation optionally accepts `Idempotency-Key` and returns the original job for equivalent replays or `409 IDEMPOTENCY_CONFLICT` for a different request.

Responses retain the envelope:

```json
{"success":true,"data":{},"error":null,"request_id":"uuid"}
```

Errors use stable codes including `UNAUTHORIZED`, `FORBIDDEN`, `RATE_LIMITED`, `INVALID_INPUT`, `INVALID_TRANSITION`, `IDEMPOTENCY_CONFLICT`, `NOT_FOUND`, and `DEPENDENCY_UNAVAILABLE`.
