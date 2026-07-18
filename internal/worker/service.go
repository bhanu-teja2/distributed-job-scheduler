package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	appErrors "github.com/bhanuteja/distributed-job-scheduler/internal/errors"
	"github.com/bhanuteja/distributed-job-scheduler/internal/job"
	"github.com/bhanuteja/distributed-job-scheduler/internal/observability"
	"github.com/bhanuteja/distributed-job-scheduler/internal/scheduler"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// ExecutorInterface is the execution boundary used by the worker service.
type ExecutorInterface interface {
	Execute(ctx context.Context, j job.Job) (json.RawMessage, error)
}

// LockManager is the secondary distributed lease contract.
type LockManager interface {
	Acquire(ctx context.Context, key, owner string, ttl time.Duration) (bool, error)
	Release(ctx context.Context, key, owner string) (bool, error)
}

type renewableLockManager interface {
	Extend(context.Context, string, string, time.Duration) (bool, error)
}

// Service polls, claims, and executes jobs with bounded local concurrency.
type Service struct {
	repo              job.Repository
	executor          ExecutorInterface
	log               *zap.Logger
	workerID          string
	concurrency       int
	batchSize         int
	pollInterval      time.Duration
	lockTTL           time.Duration
	lockManager       LockManager
	metrics           observability.Recorder
	registry          Registry
	heartbeatTTL      time.Duration
	heartbeatInterval time.Duration
}

// NewService creates a worker and normalizes unsafe concurrency and batch values.
func NewService(repo job.Repository, executor ExecutorInterface, log *zap.Logger, workerID string, concurrency, batchSize int, pollInterval, lockTTL time.Duration) *Service {
	if workerID == "" {
		workerID = "worker-" + uuid.NewString()
	}
	if concurrency < 1 {
		concurrency = 1
	}
	if batchSize < 1 {
		batchSize = 1
	}
	return &Service{repo: repo, executor: executor, log: log, workerID: workerID, concurrency: concurrency, batchSize: batchSize, pollInterval: pollInterval, lockTTL: lockTTL, lockManager: noopLockManager{}, metrics: observability.NoopRecorder{}, registry: NoopRegistry{}, heartbeatTTL: 30 * time.Second, heartbeatInterval: 10 * time.Second}
}

// WithLockManager configures the secondary distributed lease implementation.
func (s *Service) WithLockManager(lockManager LockManager) *Service {
	if lockManager != nil {
		s.lockManager = lockManager
	}
	return s
}

// WithMetrics configures lifecycle and worker metrics.
func (s *Service) WithMetrics(metrics observability.Recorder) *Service {
	if metrics != nil {
		s.metrics = metrics
	}
	return s
}

// WithRegistry configures worker heartbeats and their expiry timings.
func (s *Service) WithRegistry(registry Registry, ttl, interval time.Duration) *Service {
	if registry != nil {
		s.registry = registry
	}
	if ttl > 0 {
		s.heartbeatTTL = ttl
	}
	if interval > 0 {
		s.heartbeatInterval = interval
	}
	return s
}

// Run starts worker slots, recovery, heartbeats, and the claim loop. It returns
// after cancellation and waits for all in-process job slots to stop.
func (s *Service) Run(ctx context.Context) error {
	s.log.Info("worker started", zap.String("worker_id", s.workerID), zap.Int("concurrency", s.concurrency))
	jobs := make(chan job.Job)
	var wg sync.WaitGroup
	for i := 0; i < s.concurrency; i++ {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			s.consume(ctx, jobs, slot)
		}(i)
	}
	go s.heartbeatLoop(ctx)

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()
	defer func() {
		close(jobs)
		wg.Wait()
		s.log.Info("worker stopped", zap.String("worker_id", s.workerID))
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			// Recovery precedes new claims so expired RUNNING attempts consume their
			// retry budget before another worker executes the job again.
			if recovered, err := s.repo.RecoverExpiredRunningJobs(ctx, "worker lease expired before completion"); err != nil {
				s.log.Error("failed to recover expired jobs", zap.Error(err))
			} else if recovered > 0 {
				s.log.Warn("recovered expired jobs", zap.Int64("count", recovered))
			}
			claimed, err := s.repo.ClaimDueJobs(ctx, s.workerID, s.batchSize, s.lockTTL)
			if err != nil {
				s.log.Error("failed to claim due jobs", zap.Error(err))
				continue
			}
			s.metrics.WorkerClaimed(len(claimed))
			for _, item := range claimed {
				select {
				case <-ctx.Done():
					return nil
				case jobs <- item:
				}
			}
		}
	}
}

func (s *Service) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(s.heartbeatInterval)
	defer ticker.Stop()
	for {
		if err := s.registry.Heartbeat(ctx, s.workerID, s.heartbeatTTL); err != nil {
			s.log.Warn("worker heartbeat failed", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Service) consume(ctx context.Context, jobs <-chan job.Job, slot int) {
	for {
		select {
		case <-ctx.Done():
			return
		case j, ok := <-jobs:
			if !ok {
				return
			}
			if err := s.ProcessJob(ctx, j); err != nil {
				s.log.Error("job processing failed", zap.String("job_id", j.ID.String()), zap.Int("worker_slot", slot), zap.Error(err))
			}
		}
	}
}

// ProcessJob obtains the secondary lease, executes one attempt, and atomically
// finalizes success, retry, or dead-letter state.
func (s *Service) ProcessJob(ctx context.Context, j job.Job) error {
	lockKey := "lock:job:" + j.ID.String()
	leaseTTL := s.lockTTL
	if leaseTTL <= 0 {
		leaseTTL = time.Duration(j.TimeoutSeconds+60) * time.Second
	}
	// PostgreSQL has already claimed the job. Redis is defense in depth for
	// external side effects and is released only by the owner value.
	acquired, err := s.lockManager.Acquire(ctx, lockKey, s.workerID, leaseTTL)
	if err != nil {
		return fmt.Errorf("acquire redis lock: %w", err)
	}
	if !acquired {
		if err := s.repo.ReleaseClaim(ctx, j.ID, s.workerID, job.StatusRetryScheduled); err != nil {
			return fmt.Errorf("release claim after lock miss: %w", err)
		}
		s.log.Warn("skipped job because redis lock is owned elsewhere", zap.String("job_id", j.ID.String()))
		return nil
	}
	defer func() {
		if _, err := s.lockManager.Release(context.Background(), lockKey, s.workerID); err != nil {
			s.log.Warn("failed to release redis lock", zap.String("job_id", j.ID.String()), zap.Error(err))
		}
	}()

	if j.ActiveAttemptID == uuid.Nil {
		attempt, err := s.repo.CreateAttempt(ctx, j.ID, s.workerID, j.RetryCount+1)
		if err != nil {
			return fmt.Errorf("create attempt: %w", err)
		}
		j.ActiveAttemptID = attempt.ID
		j.AttemptNumber = j.RetryCount + 1
	}

	started := time.Now()
	timeout := time.Duration(j.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 300 * time.Second
	}
	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	leaseDone := make(chan struct{})
	go s.renewLease(execCtx, cancel, leaseDone, lockKey, j.ID, leaseTTL)

	result, err := s.executor.Execute(execCtx, j)
	cancel()
	<-leaseDone
	duration := time.Since(started)
	if err == nil {
		if successErr := s.repo.CompleteExecution(ctx, j, s.workerID, duration, result); successErr != nil {
			return fmt.Errorf("mark succeeded: %w", successErr)
		}
		s.metrics.JobCompleted(j.JobType, duration)
		return nil
	}

	outcome := ClassifyError(err)
	s.metrics.JobFailed(j.JobType)

	decision := scheduler.DecideFailure(time.Now().UTC(), j.RetryCount, j.MaxRetries, j.RetryBackoffSeconds)
	if delay := retryAfter(err); delay > 0 {
		decision.NextRunAt = time.Now().UTC().Add(delay)
	}
	nextStatus := job.StatusRetryScheduled
	if decision.Status == scheduler.StatusDeadLettered || !outcome.Retryable {
		nextStatus = job.StatusDeadLettered
	}
	if failErr := s.repo.FailExecution(ctx, j, s.workerID, duration, outcome, nextStatus, decision.NextRetryCount, decision.NextRunAt); failErr != nil {
		// An operator cancellation can win the ownership-checked finalization race;
		// that conflict is expected once the handler context has been cancelled.
		if errors.Is(err, context.Canceled) && errors.Is(failErr, appErrors.ErrConflict) {
			return nil
		}
		return fmt.Errorf("finalize failed execution: %w", failErr)
	}
	if nextStatus == job.StatusDeadLettered {
		s.metrics.JobDeadLettered(j.JobType)
	}
	return nil
}

func (s *Service) renewLease(ctx context.Context, cancel context.CancelFunc, done chan<- struct{}, lockKey string, jobID uuid.UUID, ttl time.Duration) {
	defer close(done)
	interval := ttl / 3
	if interval < time.Second {
		interval = time.Second
	}
	if interval > 5*time.Second {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Losing either authoritative DB ownership or the secondary Redis lease
			// cancels execution before this worker can commit an outcome.
			ok, err := s.repo.ExtendLease(ctx, jobID, s.workerID, ttl)
			if err != nil || !ok {
				cancel()
				return
			}
			if lockManager, ok := s.lockManager.(renewableLockManager); ok {
				extended, err := lockManager.Extend(ctx, lockKey, s.workerID, ttl)
				if err != nil || !extended {
					cancel()
					return
				}
			}
		}
	}
}

type noopLockManager struct{}

func (noopLockManager) Acquire(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	return true, nil
}

func (noopLockManager) Release(ctx context.Context, key, owner string) (bool, error) {
	return true, nil
}
