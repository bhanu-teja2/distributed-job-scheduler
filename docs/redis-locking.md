# Redis Locking

The Redis lock helper supports `SET NX EX` acquisition and owner-checked release through a Lua script.

Key format: `lock:job:{job_id}`

The MVP keeps Redis out of the required execution path so local development can start with PostgreSQL alone. The next milestone should acquire this lock after PostgreSQL claiming and before handler execution.
