package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/bhanuteja/distributed-job-scheduler/internal/events"
	"github.com/bhanuteja/distributed-job-scheduler/internal/job"
	"github.com/bhanuteja/distributed-job-scheduler/internal/observability"
	"github.com/bhanuteja/distributed-job-scheduler/internal/scheduler"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type ExecutorInterface interface {
	Execute(ctx context.Context, j job.Job) error
}

type LockManager interface {
	Acquire(ctx context.Context, key, owner string, ttl time.Duration) (bool, error)
	Release(ctx context.Context, key, owner string) (bool, error)
}

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
	publisher         events.Publisher
	metrics           observability.Recorder
	registry          Registry
	heartbeatTTL      time.Duration
	heartbeatInterval time.Duration
}

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
	return &Service{repo: repo, executor: executor, log: log, workerID: workerID, concurrency: concurrency, batchSize: batchSize, pollInterval: pollInterval, lockTTL: lockTTL, lockManager: noopLockManager{}, publisher: events.NoopPublisher{}, metrics: observability.NoopRecorder{}, registry: NoopRegistry{}, heartbeatTTL: 30 * time.Second, heartbeatInterval: 10 * time.Second}
}

func (s *Service) WithLockManager(lockManager LockManager) *Service {
	if lockManager != nil {
		s.lockManager = lockManager
	}
	return s
}

func (s *Service) WithPublisher(publisher events.Publisher) *Service {
	if publisher != nil {
		s.publisher = publisher
	}
	return s
}

func (s *Service) WithMetrics(metrics observability.Recorder) *Service {
	if metrics != nil {
		s.metrics = metrics
	}
	return s
}

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

func (s *Service) ProcessJob(ctx context.Context, j job.Job) error {
	lockKey := "lock:job:" + j.ID.String()
	acquired, err := s.lockManager.Acquire(ctx, lockKey, s.workerID, time.Duration(j.TimeoutSeconds+60)*time.Second)
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

	attemptNumber := j.RetryCount + 1
	attempt, err := s.repo.CreateAttempt(ctx, j.ID, s.workerID, attemptNumber)
	if err != nil {
		return fmt.Errorf("create attempt: %w", err)
	}
	_ = s.publisher.Publish(ctx, events.New(events.JobStarted, "worker", "job", j.ID.String(), map[string]any{"worker_id": s.workerID, "attempt_number": attemptNumber}))

	started := time.Now()
	timeout := time.Duration(j.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 300 * time.Second
	}
	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	err = s.executor.Execute(execCtx, j)
	duration := time.Since(started)
	if err == nil {
		if completeErr := s.repo.CompleteAttempt(ctx, attempt.ID, duration); completeErr != nil {
			return fmt.Errorf("complete attempt: %w", completeErr)
		}
		if successErr := s.repo.MarkSucceededByWorker(ctx, j.ID, s.workerID); successErr != nil {
			return fmt.Errorf("mark succeeded: %w", successErr)
		}
		s.metrics.JobCompleted(j.JobType, duration)
		_ = s.publisher.Publish(ctx, events.New(events.JobCompleted, "worker", "job", j.ID.String(), map[string]any{"worker_id": s.workerID, "execution_duration_ms": duration.Milliseconds()}))
		return nil
	}

	message := err.Error()
	if failErr := s.repo.FailAttempt(ctx, attempt.ID, duration, message); failErr != nil {
		return fmt.Errorf("fail attempt: %w", failErr)
	}
	s.metrics.JobFailed(j.JobType)
	_ = s.publisher.Publish(ctx, events.New(events.JobFailed, "worker", "job", j.ID.String(), map[string]any{"worker_id": s.workerID, "attempt_number": attemptNumber, "error": message}))

	decision := scheduler.DecideFailure(time.Now().UTC(), j.RetryCount, j.MaxRetries, j.RetryBackoffSeconds)
	if decision.Status == scheduler.StatusDeadLettered {
		if dlqErr := s.repo.MoveToDeadLetterByWorker(ctx, j, s.workerID, message, decision.NextRetryCount); dlqErr != nil {
			return fmt.Errorf("move to dead letter: %w", dlqErr)
		}
		s.metrics.JobDeadLettered(j.JobType)
		_ = s.publisher.Publish(ctx, events.New(events.JobDeadLettered, "worker", "job", j.ID.String(), map[string]any{"worker_id": s.workerID, "retry_count": decision.NextRetryCount, "final_error": message}))
		return nil
	}
	if retryErr := s.repo.MarkFailedForRetryByWorker(ctx, j, s.workerID, message, decision.NextRetryCount, decision.NextRunAt); retryErr != nil {
		return fmt.Errorf("schedule retry: %w", retryErr)
	}
	_ = s.publisher.Publish(ctx, events.New(events.JobRetryScheduled, "worker", "job", j.ID.String(), map[string]any{"worker_id": s.workerID, "retry_count": decision.NextRetryCount, "next_run_at": decision.NextRunAt}))
	return nil
}

type noopLockManager struct{}

func (noopLockManager) Acquire(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	return true, nil
}

func (noopLockManager) Release(ctx context.Context, key, owner string) (bool, error) {
	return true, nil
}
