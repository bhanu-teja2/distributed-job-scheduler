package job

import (
	"context"
	"errors"
	"fmt"
	"time"

	appErrors "github.com/bhanuteja/distributed-job-scheduler/internal/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository interface {
	Create(ctx context.Context, job Job) (Job, error)
	List(ctx context.Context, filter ListFilter) (Page, error)
	GetByID(ctx context.Context, id uuid.UUID) (Job, error)
	ListAttempts(ctx context.Context, jobID uuid.UUID) ([]Attempt, error)
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
	TransitionJob(ctx context.Context, id uuid.UUID, from []Status, to Status) error
	ListDeadLetters(ctx context.Context, page, pageSize int) ([]DeadLetterJob, error)
	RequeueDeadLetter(ctx context.Context, deadLetterID uuid.UUID, runAt time.Time) (Job, error)
	RecoverExpiredRunningJobs(ctx context.Context, reason string) (int64, error)
}

type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

func (r *PostgresRepository) Create(ctx context.Context, j Job) (Job, error) {
	const query = `
INSERT INTO jobs (
    id, name, job_type, payload, status, priority, run_at, retry_count, max_retries,
    retry_backoff_seconds, timeout_seconds, created_by
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
RETURNING id, name, job_type, payload, status, priority, run_at, locked_until, locked_by,
    retry_count, max_retries, retry_backoff_seconds, timeout_seconds, last_error, created_by,
    created_at, updated_at, completed_at, failed_at, cancelled_at`
	return scanJob(r.pool.QueryRow(ctx, query, j.ID, j.Name, j.JobType, j.Payload, j.Status, j.Priority, j.RunAt, j.RetryCount, j.MaxRetries, j.RetryBackoffSeconds, j.TimeoutSeconds, j.CreatedBy))
}

func (r *PostgresRepository) List(ctx context.Context, filter ListFilter) (Page, error) {
	page := normalizePage(filter.Page)
	pageSize := normalizePageSize(filter.PageSize)
	offset := (page - 1) * pageSize

	where := "WHERE ($1 = '' OR status = $1) AND ($2 = '' OR job_type = $2)"
	countQuery := "SELECT count(*) FROM jobs " + where
	var total int64
	if err := r.pool.QueryRow(ctx, countQuery, string(filter.Status), filter.JobType).Scan(&total); err != nil {
		return Page{}, err
	}

	query := `SELECT id, name, job_type, payload, status, priority, run_at, locked_until, locked_by,
    retry_count, max_retries, retry_backoff_seconds, timeout_seconds, last_error, created_by,
    created_at, updated_at, completed_at, failed_at, cancelled_at
FROM jobs ` + where + ` ORDER BY created_at DESC LIMIT $3 OFFSET $4`
	rows, err := r.pool.Query(ctx, query, string(filter.Status), filter.JobType, pageSize, offset)
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

func (r *PostgresRepository) GetByID(ctx context.Context, id uuid.UUID) (Job, error) {
	const query = `SELECT id, name, job_type, payload, status, priority, run_at, locked_until, locked_by,
    retry_count, max_retries, retry_backoff_seconds, timeout_seconds, last_error, created_by,
    created_at, updated_at, completed_at, failed_at, cancelled_at FROM jobs WHERE id=$1`
	j, err := scanJob(r.pool.QueryRow(ctx, query, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, appErrors.ErrNotFound
	}
	return j, err
}

func (r *PostgresRepository) ListAttempts(ctx context.Context, jobID uuid.UUID) ([]Attempt, error) {
	const query = `SELECT id, job_id, worker_id, attempt_number, status, started_at, completed_at,
    failed_at, error_message, execution_duration_ms, created_at FROM job_attempts WHERE job_id=$1 ORDER BY attempt_number ASC`
	rows, err := r.pool.Query(ctx, query, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var attempts []Attempt
	for rows.Next() {
		var a Attempt
		if err := rows.Scan(&a.ID, &a.JobID, &a.WorkerID, &a.AttemptNumber, &a.Status, &a.StartedAt, &a.CompletedAt, &a.FailedAt, &a.ErrorMessage, &a.ExecutionDurationMS, &a.CreatedAt); err != nil {
			return nil, err
		}
		attempts = append(attempts, a)
	}
	return attempts, rows.Err()
}

func (r *PostgresRepository) ClaimDueJobs(ctx context.Context, workerID string, batchSize int, lockTTL time.Duration) ([]Job, error) {
	const query = `
WITH due_jobs AS (
    SELECT id
    FROM jobs
    WHERE status IN ('PENDING', 'SCHEDULED', 'RETRY_SCHEDULED')
      AND run_at <= now()
      AND (locked_until IS NULL OR locked_until < now())
    ORDER BY priority DESC, run_at ASC, created_at ASC
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
UPDATE jobs
SET status = 'RUNNING',
    locked_by = $2,
    locked_until = now() + ($3::text || ' seconds')::interval,
    updated_at = now()
WHERE id IN (SELECT id FROM due_jobs)
RETURNING id, name, job_type, payload, status, priority, run_at, locked_until, locked_by,
    retry_count, max_retries, retry_backoff_seconds, timeout_seconds, last_error, created_by,
    created_at, updated_at, completed_at, failed_at, cancelled_at`
	rows, err := r.pool.Query(ctx, query, batchSize, workerID, int(lockTTL.Seconds()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanJobs(rows)
}

func (r *PostgresRepository) CreateAttempt(ctx context.Context, jobID uuid.UUID, workerID string, attemptNumber int) (Attempt, error) {
	const query = `INSERT INTO job_attempts (id, job_id, worker_id, attempt_number, status, started_at)
VALUES ($1,$2,$3,$4,'RUNNING',now())
RETURNING id, job_id, worker_id, attempt_number, status, started_at, completed_at, failed_at, error_message, execution_duration_ms, created_at`
	return scanAttempt(r.pool.QueryRow(ctx, query, uuid.New(), jobID, workerID, attemptNumber))
}

func (r *PostgresRepository) CompleteAttempt(ctx context.Context, attemptID uuid.UUID, duration time.Duration) error {
	_, err := r.pool.Exec(ctx, `UPDATE job_attempts SET status='SUCCEEDED', completed_at=now(), execution_duration_ms=$2 WHERE id=$1`, attemptID, duration.Milliseconds())
	return err
}

func (r *PostgresRepository) FailAttempt(ctx context.Context, attemptID uuid.UUID, duration time.Duration, message string) error {
	_, err := r.pool.Exec(ctx, `UPDATE job_attempts SET status='FAILED', failed_at=now(), error_message=$2, execution_duration_ms=$3 WHERE id=$1`, attemptID, message, duration.Milliseconds())
	return err
}

func (r *PostgresRepository) MarkSucceeded(ctx context.Context, jobID uuid.UUID) error {
	_, err := r.pool.Exec(ctx, `UPDATE jobs SET status='SUCCEEDED', completed_at=now(), locked_by=NULL, locked_until=NULL, updated_at=now() WHERE id=$1`, jobID)
	return err
}

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

func (r *PostgresRepository) MarkFailedForRetry(ctx context.Context, j Job, message string, nextRunAt time.Time) error {
	_, err := r.pool.Exec(ctx, `UPDATE jobs SET status='RETRY_SCHEDULED', retry_count=retry_count+1, run_at=$2, last_error=$3, failed_at=now(), locked_by=NULL, locked_until=NULL, updated_at=now() WHERE id=$1`, j.ID, nextRunAt, message)
	return err
}

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

func (r *PostgresRepository) TransitionJob(ctx context.Context, id uuid.UUID, from []Status, to Status) error {
	statuses := make([]string, 0, len(from))
	for _, status := range from {
		statuses = append(statuses, string(status))
	}
	query := `UPDATE jobs SET status=$2, updated_at=now(), cancelled_at=CASE WHEN $2='CANCELLED' THEN now() ELSE cancelled_at END, locked_by=NULL, locked_until=NULL WHERE id=$1 AND status = ANY($3)`
	tag, err := r.pool.Exec(ctx, query, id, to, statuses)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return appErrors.ErrInvalidTransition
	}
	return nil
}

func (r *PostgresRepository) RecoverExpiredRunningJobs(ctx context.Context, reason string) (int64, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `SELECT id FROM jobs WHERE status='RUNNING' AND locked_until < now()`)
	if err != nil {
		return 0, err
	}
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, tx.Commit(ctx)
	}
	if _, err := tx.Exec(ctx, `UPDATE job_attempts SET status='FAILED', failed_at=now(), error_message=$2 WHERE job_id = ANY($1) AND status='RUNNING'`, ids, reason); err != nil {
		return 0, err
	}
	tag, err := tx.Exec(ctx, `UPDATE jobs SET status='RETRY_SCHEDULED', locked_by=NULL, locked_until=NULL, last_error=$2, updated_at=now() WHERE id = ANY($1) AND status='RUNNING'`, ids, reason)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), tx.Commit(ctx)
}

func (r *PostgresRepository) ListDeadLetters(ctx context.Context, page, pageSize int) ([]DeadLetterJob, error) {
	page = normalizePage(page)
	pageSize = normalizePageSize(pageSize)
	rows, err := r.pool.Query(ctx, `SELECT id, original_job_id, name, job_type, payload, final_error, retry_count, failed_at, created_at FROM dead_letter_jobs ORDER BY created_at DESC LIMIT $1 OFFSET $2`, pageSize, (page-1)*pageSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []DeadLetterJob
	for rows.Next() {
		var item DeadLetterJob
		if err := rows.Scan(&item.ID, &item.OriginalJobID, &item.Name, &item.JobType, &item.Payload, &item.FinalError, &item.RetryCount, &item.FailedAt, &item.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *PostgresRepository) RequeueDeadLetter(ctx context.Context, deadLetterID uuid.UUID, runAt time.Time) (Job, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return Job{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var dl DeadLetterJob
	err = tx.QueryRow(ctx, `SELECT id, original_job_id, name, job_type, payload, final_error, retry_count, failed_at, created_at FROM dead_letter_jobs WHERE id=$1`, deadLetterID).
		Scan(&dl.ID, &dl.OriginalJobID, &dl.Name, &dl.JobType, &dl.Payload, &dl.FinalError, &dl.RetryCount, &dl.FailedAt, &dl.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, appErrors.ErrNotFound
	}
	if err != nil {
		return Job{}, err
	}

	newJob := Job{ID: uuid.New(), Name: dl.Name, JobType: dl.JobType, Payload: dl.Payload, Status: initialStatus(runAt, time.Now().UTC()), Priority: 5, RunAt: runAt, MaxRetries: 3, RetryBackoffSeconds: 30, TimeoutSeconds: 300}
	inserted, err := scanJob(tx.QueryRow(ctx, `INSERT INTO jobs (id, name, job_type, payload, status, priority, run_at, max_retries, retry_backoff_seconds, timeout_seconds)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
RETURNING id, name, job_type, payload, status, priority, run_at, locked_until, locked_by,
    retry_count, max_retries, retry_backoff_seconds, timeout_seconds, last_error, created_by,
    created_at, updated_at, completed_at, failed_at, cancelled_at`, newJob.ID, newJob.Name, newJob.JobType, newJob.Payload, newJob.Status, newJob.Priority, newJob.RunAt, newJob.MaxRetries, newJob.RetryBackoffSeconds, newJob.TimeoutSeconds))
	if err != nil {
		return Job{}, err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM dead_letter_jobs WHERE id=$1`, deadLetterID); err != nil {
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
	err := row.Scan(&j.ID, &j.Name, &j.JobType, &j.Payload, &j.Status, &j.Priority, &j.RunAt, &j.LockedUntil, &j.LockedBy, &j.RetryCount, &j.MaxRetries, &j.RetryBackoffSeconds, &j.TimeoutSeconds, &j.LastError, &j.CreatedBy, &j.CreatedAt, &j.UpdatedAt, &j.CompletedAt, &j.FailedAt, &j.CancelledAt)
	return j, err
}

func scanAttempt(row rowScanner) (Attempt, error) {
	var a Attempt
	err := row.Scan(&a.ID, &a.JobID, &a.WorkerID, &a.AttemptNumber, &a.Status, &a.StartedAt, &a.CompletedAt, &a.FailedAt, &a.ErrorMessage, &a.ExecutionDurationMS, &a.CreatedAt)
	return a, err
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
