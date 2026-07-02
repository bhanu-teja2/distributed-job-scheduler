package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bhanuteja/distributed-job-scheduler/internal/job"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

func TestProcessJobMarksSucceededWhenExecutorSucceeds(t *testing.T) {
	repo := &workerRepo{}
	executor := executorFunc(func(ctx context.Context, j job.Job) error { return nil })
	service := NewService(repo, executor, zap.NewNop(), "worker-1", 1, 1, time.Second, time.Minute)
	j := newWorkerTestJob()

	if err := service.ProcessJob(context.Background(), j); err != nil {
		t.Fatalf("ProcessJob returned error: %v", err)
	}
	if !repo.completedAttempt {
		t.Fatal("expected attempt to be completed")
	}
	if repo.succeededJobID != j.ID {
		t.Fatalf("expected job %s marked succeeded, got %s", j.ID, repo.succeededJobID)
	}
}

func TestProcessJobReleasesClaimWhenLockAcquireFails(t *testing.T) {
	repo := &workerRepo{}
	executed := false
	executor := executorFunc(func(ctx context.Context, j job.Job) error {
		executed = true
		return nil
	})
	service := NewService(repo, executor, zap.NewNop(), "worker-1", 1, 1, time.Second, time.Minute)
	service.lockManager = lockFunc(func(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
		return false, nil
	})
	j := newWorkerTestJob()

	if err := service.ProcessJob(context.Background(), j); err != nil {
		t.Fatalf("ProcessJob returned error: %v", err)
	}
	if executed {
		t.Fatal("executor should not run when lock acquisition fails")
	}
	if repo.releasedJobID != j.ID {
		t.Fatalf("expected job %s released, got %s", j.ID, repo.releasedJobID)
	}
}

func TestProcessJobSchedulesRetryWhenExecutorFailsBelowMaxRetries(t *testing.T) {
	repo := &workerRepo{}
	executor := executorFunc(func(ctx context.Context, j job.Job) error { return errors.New("boom") })
	service := NewService(repo, executor, zap.NewNop(), "worker-1", 1, 1, time.Second, time.Minute)
	j := newWorkerTestJob()
	j.RetryCount = 0
	j.MaxRetries = 3

	if err := service.ProcessJob(context.Background(), j); err != nil {
		t.Fatalf("ProcessJob returned error: %v", err)
	}
	if !repo.failedAttempt {
		t.Fatal("expected attempt to be failed")
	}
	if repo.retryJobID != j.ID {
		t.Fatalf("expected job %s scheduled for retry, got %s", j.ID, repo.retryJobID)
	}
	if repo.deadLetterJobID != uuid.Nil {
		t.Fatalf("did not expect dead letter, got %s", repo.deadLetterJobID)
	}
}

func TestProcessJobMovesToDeadLetterWhenRetriesExhausted(t *testing.T) {
	repo := &workerRepo{}
	executor := executorFunc(func(ctx context.Context, j job.Job) error { return errors.New("boom") })
	service := NewService(repo, executor, zap.NewNop(), "worker-1", 1, 1, time.Second, time.Minute)
	j := newWorkerTestJob()
	j.RetryCount = 2
	j.MaxRetries = 3

	if err := service.ProcessJob(context.Background(), j); err != nil {
		t.Fatalf("ProcessJob returned error: %v", err)
	}
	if repo.deadLetterJobID != j.ID {
		t.Fatalf("expected job %s moved to dead letter, got %s", j.ID, repo.deadLetterJobID)
	}
}

type executorFunc func(ctx context.Context, j job.Job) error

func (f executorFunc) Execute(ctx context.Context, j job.Job) error {
	return f(ctx, j)
}

type lockFunc func(ctx context.Context, key, owner string, ttl time.Duration) (bool, error)

func (f lockFunc) Acquire(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	return f(ctx, key, owner, ttl)
}

func (f lockFunc) Release(ctx context.Context, key, owner string) (bool, error) {
	return true, nil
}

type workerRepo struct {
	job.Repository
	completedAttempt bool
	failedAttempt    bool
	succeededJobID   uuid.UUID
	retryJobID       uuid.UUID
	deadLetterJobID  uuid.UUID
	releasedJobID    uuid.UUID
}

func (w *workerRepo) CreateAttempt(ctx context.Context, jobID uuid.UUID, workerID string, attemptNumber int) (job.Attempt, error) {
	return job.Attempt{ID: uuid.New(), JobID: jobID, WorkerID: workerID, AttemptNumber: attemptNumber}, nil
}

func (w *workerRepo) CompleteAttempt(ctx context.Context, attemptID uuid.UUID, duration time.Duration) error {
	w.completedAttempt = true
	return nil
}

func (w *workerRepo) FailAttempt(ctx context.Context, attemptID uuid.UUID, duration time.Duration, message string) error {
	w.failedAttempt = true
	return nil
}

func (w *workerRepo) MarkSucceeded(ctx context.Context, jobID uuid.UUID) error {
	w.succeededJobID = jobID
	return nil
}

func (w *workerRepo) MarkSucceededByWorker(ctx context.Context, jobID uuid.UUID, workerID string) error {
	w.succeededJobID = jobID
	return nil
}

func (w *workerRepo) MarkFailedForRetry(ctx context.Context, j job.Job, message string, nextRunAt time.Time) error {
	w.retryJobID = j.ID
	return nil
}

func (w *workerRepo) MarkFailedForRetryByWorker(ctx context.Context, j job.Job, workerID string, message string, nextRetryCount int, nextRunAt time.Time) error {
	w.retryJobID = j.ID
	return nil
}

func (w *workerRepo) MoveToDeadLetter(ctx context.Context, j job.Job, message string) error {
	w.deadLetterJobID = j.ID
	return nil
}

func (w *workerRepo) MoveToDeadLetterByWorker(ctx context.Context, j job.Job, workerID string, message string, finalRetryCount int) error {
	w.deadLetterJobID = j.ID
	return nil
}

func (w *workerRepo) ReleaseClaim(ctx context.Context, jobID uuid.UUID, workerID string, status job.Status) error {
	w.releasedJobID = jobID
	return nil
}

func newWorkerTestJob() job.Job {
	return job.Job{
		ID:                  uuid.New(),
		Name:                "test",
		JobType:             "SEND_EMAIL",
		Payload:             []byte(`{"to":"user@example.com","subject":"Welcome"}`),
		Status:              job.StatusRunning,
		Priority:            5,
		RunAt:               time.Now().UTC(),
		MaxRetries:          3,
		RetryBackoffSeconds: 30,
		TimeoutSeconds:      1,
	}
}
