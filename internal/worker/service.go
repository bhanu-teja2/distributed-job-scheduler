package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/bhanuteja/distributed-job-scheduler/internal/job"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type ExecutorInterface interface {
	Execute(ctx context.Context, j job.Job) error
}

type Service struct {
	repo         job.Repository
	executor     ExecutorInterface
	log          *zap.Logger
	workerID     string
	concurrency  int
	batchSize    int
	pollInterval time.Duration
	lockTTL      time.Duration
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
	return &Service{repo: repo, executor: executor, log: log, workerID: workerID, concurrency: concurrency, batchSize: batchSize, pollInterval: pollInterval, lockTTL: lockTTL}
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
	attemptNumber := j.RetryCount + 1
	attempt, err := s.repo.CreateAttempt(ctx, j.ID, s.workerID, attemptNumber)
	if err != nil {
		return fmt.Errorf("create attempt: %w", err)
	}

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
		if successErr := s.repo.MarkSucceeded(ctx, j.ID); successErr != nil {
			return fmt.Errorf("mark succeeded: %w", successErr)
		}
		return nil
	}

	message := err.Error()
	if failErr := s.repo.FailAttempt(ctx, attempt.ID, duration, message); failErr != nil {
		return fmt.Errorf("fail attempt: %w", failErr)
	}
	if job.ShouldDeadLetter(j.RetryCount+1, j.MaxRetries) {
		if dlqErr := s.repo.MoveToDeadLetter(ctx, j, message); dlqErr != nil {
			return fmt.Errorf("move to dead letter: %w", dlqErr)
		}
		return nil
	}
	nextRunAt := job.NextRetryAt(time.Now().UTC(), j.RetryBackoffSeconds, j.RetryCount)
	if retryErr := s.repo.MarkFailedForRetry(ctx, j, message, nextRunAt); retryErr != nil {
		return fmt.Errorf("schedule retry: %w", retryErr)
	}
	return nil
}
