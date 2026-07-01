# Worker Design

The worker service:

1. Loads configuration.
2. Connects to PostgreSQL.
3. Starts a polling loop.
4. Claims due jobs in batches.
5. Sends claimed jobs into a fixed-size worker pool.
6. Records attempts and updates job status after execution.

Retry scheduling uses exponential backoff. Exhausted jobs are moved to the dead-letter table.
