package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	appErrors "github.com/bhanuteja/distributed-job-scheduler/internal/errors"
	"github.com/bhanuteja/distributed-job-scheduler/internal/scheduler"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Repository defines persistence operations required by API and worker services.
// Implementations must enforce tenant and worker ownership where parameters are
// present, and transactional methods must commit state and lifecycle events together.
type Repository interface {
	Create(ctx context.Context, job Job) (Job, error)
	List(ctx context.Context, tenantID uuid.UUID, filter ListFilter) (Page, error)
	GetByID(ctx context.Context, tenantID, id uuid.UUID) (Job, error)
	ListAttempts(ctx context.Context, tenantID, jobID uuid.UUID) ([]Attempt, error)
	ListEvents(ctx context.Context, tenantID, jobID uuid.UUID) ([]Event, error)
	ClaimDueJobs(ctx context.Context, workerID string, batchSize int, lockTTL time.Duration) ([]Job, error)
	CreateAttempt(ctx context.Context, jobID uuid.UUID, workerID string, attemptNumber int) (Attempt, error)
	CompleteAttempt(ctx context.Context, attemptID uuid.UUID, duration time.Duration) error
	FailAttempt(ctx context.Context, attemptID uuid.UUID, duration time.Duration, message string) error
	MarkSucceeded(ctx context.Context, jobID uuid.UUID) error
	MarkSucceededByWorker(ctx context.Context, jobID uuid.UUID, workerID string) error
	MarkFailedForRetry(ctx context.Context, job Job, message string, nextRunAt time.Time) error
	MarkFailedForRetryByWorker(ctx context.Context, job Job, workerID string, message string, nextRetryCount int, nextRunAt time.Time) error
	MoveToDeadLetter(ctx context.Context, job Job, message string) error
	MoveToDeadLetterByWorker(ctx context.Context, job Job, workerID string, message string, finalRetryCount int) error
	ReleaseClaim(ctx context.Context, jobID uuid.UUID, workerID string, status Status) error
	TransitionJob(ctx context.Context, tenantID, id uuid.UUID, from []Status, to Status) error
	ListDeadLetters(ctx context.Context, tenantID uuid.UUID, page, pageSize int) ([]DeadLetterJob, error)
	RequeueDeadLetter(ctx context.Context, tenantID, deadLetterID uuid.UUID, runAt time.Time) (Job, error)
	RecoverExpiredRunningJobs(ctx context.Context, reason string) (int64, error)
	CompleteExecution(ctx context.Context, job Job, workerID string, duration time.Duration, result json.RawMessage) error
	FailExecution(ctx context.Context, job Job, workerID string, duration time.Duration, outcome ExecutionResult, nextStatus Status, nextRetryCount int, nextRunAt time.Time) error
	ExtendLease(ctx context.Context, jobID uuid.UUID, workerID string, ttl time.Duration) (bool, error)
}

// PostgresRepository stores authoritative scheduler state in PostgreSQL.
type PostgresRepository struct {
	pool *pgxpool.Pool
}

// NewPostgresRepository creates a repository backed by the supplied pool.
func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

// Create inserts a tenant-owned job and its job.created outbox event atomically.
func (r *PostgresRepository) Create(ctx context.Context, j Job) (Job, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return Job{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	const query = `
INSERT INTO jobs (
    id, name, job_type, payload, status, priority, run_at, retry_count, max_retries,
    retry_backoff_seconds, timeout_seconds, created_by, tenant_id, idempotency_key, request_hash
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
RETURNING id, name, job_type, payload, status, priority, run_at, locked_until, locked_by,
    retry_count, max_retries, retry_backoff_seconds, timeout_seconds, last_error, created_by,
    created_at, updated_at, completed_at, failed_at, cancelled_at, tenant_id, idempotency_key, request_hash`
	inserted, err := scanJob(tx.QueryRow(ctx, query, j.ID, j.Name, j.JobType, j.Payload, j.Status, j.Priority, j.RunAt, j.RetryCount, j.MaxRetries, j.RetryBackoffSeconds, j.TimeoutSeconds, j.CreatedBy, j.TenantID, j.IdempotencyKey, j.RequestHash))
	if err != nil {
		// The unique tenant/key index arbitrates concurrent idempotent requests.
		// Matching hashes replay the original result; different hashes conflict.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" && j.IdempotencyKey != nil {
			_ = tx.Rollback(ctx)
			existing, lookupErr := r.getByIdempotencyKey(ctx, j.TenantID, *j.IdempotencyKey)
			if lookupErr != nil {
				return Job{}, lookupErr
			}
			if existing.RequestHash == nil || j.RequestHash == nil || *existing.RequestHash != *j.RequestHash {
				return Job{}, appErrors.ErrIdempotency
			}
			return existing, nil
		}
		return Job{}, err
	}
	if err := insertJobEvent(ctx, tx, inserted, "job.created", "scheduler-api", map[string]any{"status": inserted.Status, "run_at": inserted.RunAt, "priority": inserted.Priority}); err != nil {
		return Job{}, err
	}
	return inserted, tx.Commit(ctx)
}

// List returns a bounded, deterministically ordered page for one tenant.
func (r *PostgresRepository) List(ctx context.Context, tenantID uuid.UUID, filter ListFilter) (Page, error) {
	page := normalizePage(filter.Page)
	pageSize := normalizePageSize(filter.PageSize)
	offset := (page - 1) * pageSize

	where := "WHERE tenant_id=$1 AND ($2 = '' OR status = $2) AND ($3 = '' OR job_type = $3) AND ($4::timestamptz IS NULL OR created_at >= $4) AND ($5::timestamptz IS NULL OR created_at <= $5)"
	countQuery := "SELECT count(*) FROM jobs " + where
	var total int64
	if err := r.pool.QueryRow(ctx, countQuery, tenantID, string(filter.Status), filter.JobType, filter.CreatedAfter, filter.CreatedBefore).Scan(&total); err != nil {
		return Page{}, err
	}

	query := `SELECT id, name, job_type, payload, status, priority, run_at, locked_until, locked_by,
    retry_count, max_retries, retry_backoff_seconds, timeout_seconds, last_error, created_by,
    created_at, updated_at, completed_at, failed_at, cancelled_at, tenant_id, idempotency_key, request_hash
FROM jobs ` + where + ` ORDER BY ` + filter.Sort + ` ` + filter.Order + `, id ` + filter.Order + ` LIMIT $6 OFFSET $7`
	rows, err := r.pool.Query(ctx, query, tenantID, string(filter.Status), filter.JobType, filter.CreatedAfter, filter.CreatedBefore, pageSize, offset)
	if err != nil {
		return Page{}, err
	}
	defer rows.Close()

	items, err := scanJobs(rows)
	if err != nil {
		return Page{}, err
	}
	return Page{Items: items, Page: page, PageSize: pageSize, Total: total}, nil
}

// GetByID returns a job only when it belongs to tenantID.
func (r *PostgresRepository) GetByID(ctx context.Context, tenantID, id uuid.UUID) (Job, error) {
	const query = `SELECT id, name, job_type, payload, status, priority, run_at, locked_until, locked_by,
    retry_count, max_retries, retry_backoff_seconds, timeout_seconds, last_error, created_by,
    created_at, updated_at, completed_at, failed_at, cancelled_at, tenant_id, idempotency_key, request_hash FROM jobs WHERE tenant_id=$1 AND id=$2`
	j, err := scanJob(r.pool.QueryRow(ctx, query, tenantID, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, appErrors.ErrNotFound
	}
	return j, err
}

// ListAttempts returns execution attempts in attempt-number order.
func (r *PostgresRepository) ListAttempts(ctx context.Context, tenantID, jobID uuid.UUID) ([]Attempt, error) {
	const query = `SELECT id, job_id, worker_id, attempt_number, status, started_at, completed_at,
    failed_at, error_message, execution_duration_ms, created_at, result, error_code, retryable FROM job_attempts WHERE tenant_id=$1 AND job_id=$2 ORDER BY attempt_number ASC`
	rows, err := r.pool.Query(ctx, query, tenantID, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var attempts []Attempt
	for rows.Next() {
		var a Attempt
		if err := rows.Scan(&a.ID, &a.JobID, &a.WorkerID, &a.AttemptNumber, &a.Status, &a.StartedAt, &a.CompletedAt, &a.FailedAt, &a.ErrorMessage, &a.ExecutionDurationMS, &a.CreatedAt, &a.Result, &a.ErrorCode, &a.Retryable); err != nil {
			return nil, err
		}
		attempts = append(attempts, a)
	}
	return attempts, rows.Err()
}

// ListEvents returns the durable lifecycle timeline for a tenant-owned job.
func (r *PostgresRepository) ListEvents(ctx context.Context, tenantID, jobID uuid.UUID) ([]Event, error) {
	rows, err := r.pool.Query(ctx, `SELECT id, job_id, tenant_id, event_type, source, schema_version, correlation_id, causation_id, payload, created_at, published_at, publish_attempts, last_publish_error FROM job_events WHERE tenant_id=$1 AND job_id=$2 ORDER BY created_at ASC, id ASC`, tenantID, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Event
	for rows.Next() {
		var item Event
		if err := rows.Scan(&item.ID, &item.JobID, &item.TenantID, &item.EventType, &item.Source, &item.SchemaVersion, &item.CorrelationID, &item.CausationID, &item.Payload, &item.CreatedAt, &item.PublishedAt, &item.PublishAttempts, &item.LastPublishError); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// ClaimDueJobs atomically leases due jobs and creates their running attempts.
func (r *PostgresRepository) ClaimDueJobs(ctx context.Context, workerID string, batchSize int, lockTTL time.Duration) ([]Job, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// SKIP LOCKED lets worker replicas claim disjoint rows without serializing
	// behind work already selected by another transaction.
	const query = `SELECT id, name, job_type, payload, status, priority, run_at, locked_until, locked_by,
    retry_count, max_retries, retry_backoff_seconds, timeout_seconds, last_error, created_by,
    created_at, updated_at, completed_at, failed_at, cancelled_at, tenant_id, idempotency_key, request_hash
    FROM jobs
    WHERE status IN ('PENDING', 'SCHEDULED', 'RETRY_SCHEDULED')
      AND run_at <= now()
      AND (locked_until IS NULL OR locked_until < now())
    ORDER BY priority DESC, run_at ASC, created_at ASC, id ASC
	LIMIT $1 FOR UPDATE SKIP LOCKED`
	rows, err := tx.Query(ctx, query, batchSize)
	if err != nil {
		return nil, err
	}
	items, err := scanJobs(rows)
	rows.Close()
	if err != nil {
		return nil, err
	}
	for i := range items {
		j := &items[i]
		// Claim state, attempt creation, and job.started share this transaction;
		// observers can never see a RUNNING job without its active attempt/event.
		_, err = tx.Exec(ctx, `UPDATE jobs SET status='RUNNING',locked_by=$2,locked_until=now()+$3*interval '1 second',updated_at=now() WHERE id=$1`, j.ID, workerID, int(lockTTL.Seconds()))
		if err != nil {
			return nil, err
		}
		j.Status = StatusRunning
		j.LockedBy = &workerID
		until := time.Now().UTC().Add(lockTTL)
		j.LockedUntil = &until
		j.AttemptNumber = j.RetryCount + 1
		j.ActiveAttemptID = uuid.New()
		_, err = tx.Exec(ctx, `INSERT INTO job_attempts(id,job_id,tenant_id,worker_id,attempt_number,status,started_at) VALUES($1,$2,$3,$4,$5,'RUNNING',now())`, j.ActiveAttemptID, j.ID, j.TenantID, workerID, j.AttemptNumber)
		if err != nil {
			return nil, err
		}
		if err := insertJobEvent(ctx, tx, *j, "job.started", "worker", map[string]any{"worker_id": workerID, "attempt_number": j.AttemptNumber}); err != nil {
			return nil, err
		}
	}
	return items, tx.Commit(ctx)
}

// CompleteExecution atomically finalizes an owned attempt and job as successful.
func (r *PostgresRepository) CompleteExecution(ctx context.Context, j Job, workerID string, duration time.Duration, result json.RawMessage) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// The ownership predicate prevents a stale worker from finalizing work after
	// lease recovery transferred the job to another worker.
	tag, err := tx.Exec(ctx, `UPDATE jobs SET status='SUCCEEDED',completed_at=now(),locked_by=NULL,locked_until=NULL,updated_at=now() WHERE id=$1 AND tenant_id=$2 AND locked_by=$3 AND status='RUNNING'`, j.ID, j.TenantID, workerID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return appErrors.ErrConflict
	}
	tag, err = tx.Exec(ctx, `UPDATE job_attempts SET status='SUCCEEDED',completed_at=now(),execution_duration_ms=$2,result=$3 WHERE id=$1 AND status='RUNNING'`, j.ActiveAttemptID, duration.Milliseconds(), nullableJSON(result))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return appErrors.ErrConflict
	}
	if err := insertJobEvent(ctx, tx, j, "job.completed", "worker", map[string]any{"worker_id": workerID, "execution_duration_ms": duration.Milliseconds()}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// FailExecution atomically records failure and schedules retry or dead-lettering.
func (r *PostgresRepository) FailExecution(ctx context.Context, j Job, workerID string, duration time.Duration, outcome ExecutionResult, nextStatus Status, nextRetryCount int, nextRunAt time.Time) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `UPDATE job_attempts SET status='FAILED',failed_at=now(),error_message=$2,error_code=$3,retryable=$4,execution_duration_ms=$5,result=$6 WHERE id=$1 AND status='RUNNING'`, j.ActiveAttemptID, outcome.Message, outcome.ErrorCode, outcome.Retryable, duration.Milliseconds(), nullableJSON(outcome.Metadata))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return appErrors.ErrConflict
	}
	if err := insertJobEvent(ctx, tx, j, "job.failed", "worker", map[string]any{"worker_id": workerID, "attempt_number": j.AttemptNumber, "error": outcome.Message, "error_code": outcome.ErrorCode, "retryable": outcome.Retryable}); err != nil {
		return err
	}
	// Both branches finalize the attempt, change job state, and append all
	// lifecycle events atomically. The DLQ branch also creates its audit record.
	if nextStatus == StatusDeadLettered {
		tag, err = tx.Exec(ctx, `UPDATE jobs SET status='DEAD_LETTERED',retry_count=$2,last_error=$3,failed_at=now(),locked_by=NULL,locked_until=NULL,updated_at=now() WHERE id=$1 AND locked_by=$4 AND status='RUNNING'`, j.ID, nextRetryCount, outcome.Message, workerID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return appErrors.ErrConflict
		}
		_, err = tx.Exec(ctx, `INSERT INTO dead_letter_jobs(id,original_job_id,tenant_id,name,job_type,payload,final_error,retry_count,failed_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,now())`, uuid.New(), j.ID, j.TenantID, j.Name, j.JobType, j.Payload, outcome.Message, nextRetryCount)
		if err != nil {
			return err
		}
		if err := insertJobEvent(ctx, tx, j, "job.dead_lettered", "worker", map[string]any{"retry_count": nextRetryCount, "final_error": outcome.Message}); err != nil {
			return err
		}
	} else {
		tag, err = tx.Exec(ctx, `UPDATE jobs SET status='RETRY_SCHEDULED',retry_count=$2,run_at=$3,last_error=$4,failed_at=now(),locked_by=NULL,locked_until=NULL,updated_at=now() WHERE id=$1 AND locked_by=$5 AND status='RUNNING'`, j.ID, nextRetryCount, nextRunAt, outcome.Message, workerID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return appErrors.ErrConflict
		}
		if err := insertJobEvent(ctx, tx, j, "job.retry_scheduled", "worker", map[string]any{"retry_count": nextRetryCount, "next_run_at": nextRunAt}); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// ExtendLease renews a RUNNING job only while workerID owns it.
func (r *PostgresRepository) ExtendLease(ctx context.Context, jobID uuid.UUID, workerID string, ttl time.Duration) (bool, error) {
	tag, err := r.pool.Exec(ctx, `UPDATE jobs SET locked_until=now()+$3*interval '1 second',updated_at=now() WHERE id=$1 AND locked_by=$2 AND status='RUNNING'`, jobID, workerID, int(ttl.Seconds()))
	return tag.RowsAffected() == 1, err
}

// CreateAttempt creates a RUNNING attempt for compatibility with manual claims.
func (r *PostgresRepository) CreateAttempt(ctx context.Context, jobID uuid.UUID, workerID string, attemptNumber int) (Attempt, error) {
	const query = `INSERT INTO job_attempts (id, job_id, tenant_id, worker_id, attempt_number, status, started_at)
SELECT $1,$2,tenant_id,$3,$4,'RUNNING',now() FROM jobs WHERE id=$2
RETURNING id, job_id, worker_id, attempt_number, status, started_at, completed_at, failed_at, error_message, execution_duration_ms, created_at, result, error_code, retryable`
	return scanAttempt(r.pool.QueryRow(ctx, query, uuid.New(), jobID, workerID, attemptNumber))
}

// CompleteAttempt marks a standalone attempt successful.
func (r *PostgresRepository) CompleteAttempt(ctx context.Context, attemptID uuid.UUID, duration time.Duration) error {
	_, err := r.pool.Exec(ctx, `UPDATE job_attempts SET status='SUCCEEDED', completed_at=now(), execution_duration_ms=$2 WHERE id=$1`, attemptID, duration.Milliseconds())
	return err
}

// FailAttempt marks a standalone attempt failed with a diagnostic message.
func (r *PostgresRepository) FailAttempt(ctx context.Context, attemptID uuid.UUID, duration time.Duration, message string) error {
	_, err := r.pool.Exec(ctx, `UPDATE job_attempts SET status='FAILED', failed_at=now(), error_message=$2, execution_duration_ms=$3 WHERE id=$1`, attemptID, message, duration.Milliseconds())
	return err
}

// MarkSucceeded performs an unconditional compatibility update.
func (r *PostgresRepository) MarkSucceeded(ctx context.Context, jobID uuid.UUID) error {
	_, err := r.pool.Exec(ctx, `UPDATE jobs SET status='SUCCEEDED', completed_at=now(), locked_by=NULL, locked_until=NULL, updated_at=now() WHERE id=$1`, jobID)
	return err
}

// MarkSucceededByWorker succeeds a RUNNING job only for its owner.
func (r *PostgresRepository) MarkSucceededByWorker(ctx context.Context, jobID uuid.UUID, workerID string) error {
	tag, err := r.pool.Exec(ctx, `UPDATE jobs SET status='SUCCEEDED', completed_at=now(), locked_by=NULL, locked_until=NULL, updated_at=now() WHERE id=$1 AND locked_by=$2 AND status='RUNNING'`, jobID, workerID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return appErrors.ErrConflict
	}
	return nil
}

// MarkFailedForRetry performs an unconditional compatibility retry update.
func (r *PostgresRepository) MarkFailedForRetry(ctx context.Context, j Job, message string, nextRunAt time.Time) error {
	_, err := r.pool.Exec(ctx, `UPDATE jobs SET status='RETRY_SCHEDULED', retry_count=retry_count+1, run_at=$2, last_error=$3, failed_at=now(), locked_by=NULL, locked_until=NULL, updated_at=now() WHERE id=$1`, j.ID, nextRunAt, message)
	return err
}

// MarkFailedForRetryByWorker schedules retry only for the current owner.
func (r *PostgresRepository) MarkFailedForRetryByWorker(ctx context.Context, j Job, workerID string, message string, nextRetryCount int, nextRunAt time.Time) error {
	tag, err := r.pool.Exec(ctx, `UPDATE jobs SET status='RETRY_SCHEDULED', retry_count=$2, run_at=$3, last_error=$4, failed_at=now(), locked_by=NULL, locked_until=NULL, updated_at=now() WHERE id=$1 AND locked_by=$5 AND status='RUNNING'`, j.ID, nextRetryCount, nextRunAt, message, workerID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return appErrors.ErrConflict
	}
	return nil
}

// MoveToDeadLetter performs an unconditional compatibility DLQ transaction.
func (r *PostgresRepository) MoveToDeadLetter(ctx context.Context, j Job, message string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `UPDATE jobs SET status='DEAD_LETTERED', last_error=$2, failed_at=now(), locked_by=NULL, locked_until=NULL, updated_at=now() WHERE id=$1`, j.ID, message); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO dead_letter_jobs (id, original_job_id, name, job_type, payload, final_error, retry_count, failed_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,now())`, uuid.New(), j.ID, j.Name, j.JobType, j.Payload, message, j.RetryCount); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// MoveToDeadLetterByWorker dead-letters a job only for its current owner.
func (r *PostgresRepository) MoveToDeadLetterByWorker(ctx context.Context, j Job, workerID string, message string, finalRetryCount int) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, `UPDATE jobs SET status='DEAD_LETTERED', retry_count=$2, last_error=$3, failed_at=now(), locked_by=NULL, locked_until=NULL, updated_at=now() WHERE id=$1 AND locked_by=$4 AND status='RUNNING'`, j.ID, finalRetryCount, message, workerID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return appErrors.ErrConflict
	}
	if _, err := tx.Exec(ctx, `INSERT INTO dead_letter_jobs (id, original_job_id, name, job_type, payload, final_error, retry_count, failed_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,now())`, uuid.New(), j.ID, j.Name, j.JobType, j.Payload, message, finalRetryCount); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ReleaseClaim clears ownership and returns a job to the supplied claimable state.
func (r *PostgresRepository) ReleaseClaim(ctx context.Context, jobID uuid.UUID, workerID string, status Status) error {
	tag, err := r.pool.Exec(ctx, `UPDATE jobs SET status=$3, locked_by=NULL, locked_until=NULL, updated_at=now() WHERE id=$1 AND locked_by=$2 AND status='RUNNING'`, jobID, workerID, status)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return appErrors.ErrConflict
	}
	return nil
}

// TransitionJob applies an operator transition using tenant and current-state guards.
func (r *PostgresRepository) TransitionJob(ctx context.Context, tenantID, id uuid.UUID, from []Status, to Status) error {
	statuses := make([]string, 0, len(from))
	for _, status := range from {
		statuses = append(statuses, string(status))
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// The current-state predicate closes the race between service validation and
	// this write if another operator or worker transitions the job first.
	query := `UPDATE jobs SET status=$3, updated_at=now(), cancelled_at=CASE WHEN $3='CANCELLED' THEN now() ELSE cancelled_at END, locked_by=NULL, locked_until=NULL WHERE tenant_id=$1 AND id=$2 AND status = ANY($4)`
	tag, err := tx.Exec(ctx, query, tenantID, id, to, statuses)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return appErrors.ErrInvalidTransition
	}
	if to == StatusCancelled {
		if _, err := tx.Exec(ctx, `UPDATE job_attempts SET status='CANCELLED',failed_at=now(),error_message='cancelled by operator',error_code='CANCELLED',retryable=false WHERE job_id=$1 AND status='RUNNING'`, id); err != nil {
			return err
		}
	}
	j := Job{ID: id, TenantID: tenantID}
	if err := insertJobEvent(ctx, tx, j, "job."+strings.ToLower(string(to)), "scheduler-api", map[string]any{"status": to}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// RecoverExpiredRunningJobs finalizes expired attempts and schedules retry or DLQ.
func (r *PostgresRepository) RecoverExpiredRunningJobs(ctx context.Context, reason string) (int64, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `SELECT id, name, job_type, payload, status, priority, run_at, locked_until, locked_by, retry_count, max_retries, retry_backoff_seconds, timeout_seconds, last_error, created_by, created_at, updated_at, completed_at, failed_at, cancelled_at, tenant_id, idempotency_key, request_hash FROM jobs WHERE status='RUNNING' AND locked_until < now() FOR UPDATE SKIP LOCKED`)
	if err != nil {
		return 0, err
	}
	items, err := scanJobs(rows)
	rows.Close()
	if err != nil {
		return 0, err
	}
	if len(items) == 0 {
		return 0, tx.Commit(ctx)
	}
	for _, j := range items {
		if _, err := tx.Exec(ctx, `UPDATE job_attempts SET status='FAILED',failed_at=now(),error_message=$2,error_code='LEASE_EXPIRED',retryable=true WHERE job_id=$1 AND status='RUNNING'`, j.ID, reason); err != nil {
			return 0, err
		}
		decision := scheduler.DecideFailure(time.Now().UTC(), j.RetryCount, j.MaxRetries, j.RetryBackoffSeconds)
		if decision.Status == scheduler.StatusDeadLettered {
			if _, err := tx.Exec(ctx, `UPDATE jobs SET status='DEAD_LETTERED',retry_count=$2,locked_by=NULL,locked_until=NULL,last_error=$3,failed_at=now(),updated_at=now() WHERE id=$1 AND status='RUNNING'`, j.ID, decision.NextRetryCount, reason); err != nil {
				return 0, err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO dead_letter_jobs(id,original_job_id,tenant_id,name,job_type,payload,final_error,retry_count,failed_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,now())`, uuid.New(), j.ID, j.TenantID, j.Name, j.JobType, j.Payload, reason, decision.NextRetryCount); err != nil {
				return 0, err
			}
			if err := insertJobEvent(ctx, tx, j, "job.dead_lettered", "recovery", map[string]any{"reason": reason, "retry_count": decision.NextRetryCount}); err != nil {
				return 0, err
			}
		} else {
			if _, err := tx.Exec(ctx, `UPDATE jobs SET status='RETRY_SCHEDULED',retry_count=$2,run_at=$3,locked_by=NULL,locked_until=NULL,last_error=$4,failed_at=now(),updated_at=now() WHERE id=$1 AND status='RUNNING'`, j.ID, decision.NextRetryCount, decision.NextRunAt, reason); err != nil {
				return 0, err
			}
			if err := insertJobEvent(ctx, tx, j, "job.retry_scheduled", "recovery", map[string]any{"reason": reason, "retry_count": decision.NextRetryCount, "next_run_at": decision.NextRunAt}); err != nil {
				return 0, err
			}
		}
	}
	return int64(len(items)), tx.Commit(ctx)
}

// ListDeadLetters returns tenant-scoped terminal failures in newest-first order.
func (r *PostgresRepository) ListDeadLetters(ctx context.Context, tenantID uuid.UUID, page, pageSize int) ([]DeadLetterJob, error) {
	page = normalizePage(page)
	pageSize = normalizePageSize(pageSize)
	rows, err := r.pool.Query(ctx, `SELECT id, original_job_id, name, job_type, payload, final_error, retry_count, failed_at, created_at, tenant_id, requeued_at, requeued_job_id FROM dead_letter_jobs WHERE tenant_id=$1 ORDER BY created_at DESC, id DESC LIMIT $2 OFFSET $3`, tenantID, pageSize, (page-1)*pageSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []DeadLetterJob
	for rows.Next() {
		var item DeadLetterJob
		if err := rows.Scan(&item.ID, &item.OriginalJobID, &item.Name, &item.JobType, &item.Payload, &item.FinalError, &item.RetryCount, &item.FailedAt, &item.CreatedAt, &item.TenantID, &item.RequeuedAt, &item.RequeuedJobID); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// RequeueDeadLetter creates a fresh linked job while retaining the DLQ audit row.
func (r *PostgresRepository) RequeueDeadLetter(ctx context.Context, tenantID, deadLetterID uuid.UUID, runAt time.Time) (Job, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return Job{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var dl DeadLetterJob
	err = tx.QueryRow(ctx, `SELECT id, original_job_id, name, job_type, payload, final_error, retry_count, failed_at, created_at, tenant_id, requeued_at, requeued_job_id FROM dead_letter_jobs WHERE tenant_id=$1 AND id=$2 AND requeued_at IS NULL FOR UPDATE`, tenantID, deadLetterID).
		Scan(&dl.ID, &dl.OriginalJobID, &dl.Name, &dl.JobType, &dl.Payload, &dl.FinalError, &dl.RetryCount, &dl.FailedAt, &dl.CreatedAt, &dl.TenantID, &dl.RequeuedAt, &dl.RequeuedJobID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, appErrors.ErrNotFound
	}
	if err != nil {
		return Job{}, err
	}

	newJob := Job{ID: uuid.New(), TenantID: tenantID, Name: dl.Name, JobType: dl.JobType, Payload: dl.Payload, Status: initialStatus(runAt, time.Now().UTC()), Priority: 5, RunAt: runAt, MaxRetries: 3, RetryBackoffSeconds: 30, TimeoutSeconds: 300}
	inserted, err := scanJob(tx.QueryRow(ctx, `INSERT INTO jobs (id, tenant_id, name, job_type, payload, status, priority, run_at, max_retries, retry_backoff_seconds, timeout_seconds, source_dead_letter_id)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
RETURNING id, name, job_type, payload, status, priority, run_at, locked_until, locked_by,
    retry_count, max_retries, retry_backoff_seconds, timeout_seconds, last_error, created_by,
    created_at, updated_at, completed_at, failed_at, cancelled_at, tenant_id, idempotency_key, request_hash`, newJob.ID, newJob.TenantID, newJob.Name, newJob.JobType, newJob.Payload, newJob.Status, newJob.Priority, newJob.RunAt, newJob.MaxRetries, newJob.RetryBackoffSeconds, newJob.TimeoutSeconds, deadLetterID))
	if err != nil {
		return Job{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE dead_letter_jobs SET requeued_at=now(),requeued_job_id=$2 WHERE id=$1`, deadLetterID, inserted.ID); err != nil {
		return Job{}, err
	}
	if err := insertJobEvent(ctx, tx, inserted, "job.requeued", "scheduler-api", map[string]any{"source_dead_letter_id": deadLetterID}); err != nil {
		return Job{}, err
	}
	return inserted, tx.Commit(ctx)
}

func scanJobs(rows pgx.Rows) ([]Job, error) {
	var jobs []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanJob(row rowScanner) (Job, error) {
	var j Job
	err := row.Scan(&j.ID, &j.Name, &j.JobType, &j.Payload, &j.Status, &j.Priority, &j.RunAt, &j.LockedUntil, &j.LockedBy, &j.RetryCount, &j.MaxRetries, &j.RetryBackoffSeconds, &j.TimeoutSeconds, &j.LastError, &j.CreatedBy, &j.CreatedAt, &j.UpdatedAt, &j.CompletedAt, &j.FailedAt, &j.CancelledAt, &j.TenantID, &j.IdempotencyKey, &j.RequestHash)
	return j, err
}

func scanAttempt(row rowScanner) (Attempt, error) {
	var a Attempt
	err := row.Scan(&a.ID, &a.JobID, &a.WorkerID, &a.AttemptNumber, &a.Status, &a.StartedAt, &a.CompletedAt, &a.FailedAt, &a.ErrorMessage, &a.ExecutionDurationMS, &a.CreatedAt, &a.Result, &a.ErrorCode, &a.Retryable)
	return a, err
}

func (r *PostgresRepository) getByIdempotencyKey(ctx context.Context, tenantID uuid.UUID, key string) (Job, error) {
	const query = `SELECT id, name, job_type, payload, status, priority, run_at, locked_until, locked_by, retry_count, max_retries, retry_backoff_seconds, timeout_seconds, last_error, created_by, created_at, updated_at, completed_at, failed_at, cancelled_at, tenant_id, idempotency_key, request_hash FROM jobs WHERE tenant_id=$1 AND idempotency_key=$2`
	return scanJob(r.pool.QueryRow(ctx, query, tenantID, key))
}

type eventExecutor interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func insertJobEvent(ctx context.Context, exec eventExecutor, j Job, eventType, source string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = exec.Exec(ctx, `INSERT INTO job_events(id, job_id, tenant_id, event_type, source, payload, schema_version) VALUES($1,$2,$3,$4,$5,$6,1)`, uuid.New(), j.ID, j.TenantID, eventType, source, body)
	return err
}

func nullableJSON(value json.RawMessage) any {
	if len(value) == 0 {
		return nil
	}
	return value
}

func normalizePage(page int) int {
	if page < 1 {
		return 1
	}
	return page
}

func normalizePageSize(pageSize int) int {
	if pageSize < 1 {
		return 20
	}
	if pageSize > 100 {
		return 100
	}
	return pageSize
}

func repositoryError(action string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", action, err)
}
