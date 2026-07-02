package job

import (
	"context"
	"fmt"
	"math"
	"time"

	appErrors "github.com/bhanuteja/distributed-job-scheduler/internal/errors"
	"github.com/bhanuteja/distributed-job-scheduler/internal/events"
	"github.com/bhanuteja/distributed-job-scheduler/internal/observability"
	"github.com/bhanuteja/distributed-job-scheduler/internal/scheduler"
	"github.com/google/uuid"
)

type Service struct {
	repo              Repository
	now               func() time.Time
	defaultRetries    int
	defaultBackoffSec int
	defaultTimeoutSec int
	publisher         events.Publisher
	metrics           observability.Recorder
}

func NewService(repo Repository, defaultRetries, defaultBackoffSec, defaultTimeoutSec int) *Service {
	return &Service{
		repo:              repo,
		now:               func() time.Time { return time.Now().UTC() },
		defaultRetries:    defaultRetries,
		defaultBackoffSec: defaultBackoffSec,
		defaultTimeoutSec: defaultTimeoutSec,
		publisher:         events.NoopPublisher{},
		metrics:           observability.NoopRecorder{},
	}
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

func (s *Service) Create(ctx context.Context, req CreateRequest) (CreateResponse, error) {
	req = s.withDefaults(req)
	if err := validateCreate(req); err != nil {
		return CreateResponse{}, fmt.Errorf("%w: %s", appErrors.ErrInvalidInput, err.Error())
	}

	var createdBy *string
	if req.CreatedBy != "" {
		createdBy = &req.CreatedBy
	}
	j := Job{
		ID:                  uuid.New(),
		Name:                req.Name,
		JobType:             req.JobType,
		Payload:             req.Payload,
		Status:              initialStatus(req.RunAt, s.now()),
		Priority:            req.Priority,
		RunAt:               req.RunAt.UTC(),
		MaxRetries:          req.MaxRetries,
		RetryBackoffSeconds: req.RetryBackoffSeconds,
		TimeoutSeconds:      req.TimeoutSeconds,
		CreatedBy:           createdBy,
	}

	inserted, err := s.repo.Create(ctx, j)
	if err != nil {
		return CreateResponse{}, err
	}
	s.metrics.JobCreated(inserted.JobType)
	_ = s.publisher.Publish(ctx, events.New(events.JobCreated, "scheduler-api", "job", inserted.ID.String(), map[string]any{"job_id": inserted.ID.String(), "job_type": inserted.JobType, "status": inserted.Status, "run_at": inserted.RunAt, "priority": inserted.Priority}))
	return CreateResponse{JobID: inserted.ID.String(), Status: inserted.Status}, nil
}

func (s *Service) List(ctx context.Context, filter ListFilter) (Page, error) {
	if filter.Status != "" && !isKnownStatus(filter.Status) {
		return Page{}, fmt.Errorf("%w: unsupported status filter", appErrors.ErrInvalidInput)
	}
	if filter.JobType != "" && !isSupportedJobType(filter.JobType) {
		return Page{}, fmt.Errorf("%w: unsupported job_type filter", appErrors.ErrInvalidInput)
	}
	return s.repo.List(ctx, filter)
}

func (s *Service) Get(ctx context.Context, id uuid.UUID) (Job, error) {
	return s.repo.GetByID(ctx, id)
}

func (s *Service) Attempts(ctx context.Context, id uuid.UUID) ([]Attempt, error) {
	return s.repo.ListAttempts(ctx, id)
}

func (s *Service) ListDeadLetters(ctx context.Context, page, pageSize int) ([]DeadLetterJob, error) {
	return s.repo.ListDeadLetters(ctx, page, pageSize)
}

func (s *Service) RequeueDeadLetter(ctx context.Context, deadLetterID uuid.UUID) (Job, error) {
	return s.repo.RequeueDeadLetter(ctx, deadLetterID, s.now())
}

func (s *Service) Cancel(ctx context.Context, id uuid.UUID) error {
	if err := s.transition(ctx, id, StatusCancelled, []Status{StatusPending, StatusScheduled, StatusRetryScheduled, StatusRunning}); err != nil {
		return err
	}
	_ = s.publisher.Publish(ctx, events.New(events.JobCancelled, "scheduler-api", "job", id.String(), map[string]any{"job_id": id.String()}))
	return nil
}

func (s *Service) Pause(ctx context.Context, id uuid.UUID) error {
	return s.transition(ctx, id, StatusPaused, []Status{StatusPending, StatusScheduled})
}

func (s *Service) Resume(ctx context.Context, id uuid.UUID) error {
	return s.transition(ctx, id, StatusScheduled, []Status{StatusPaused})
}

func (s *Service) Retry(ctx context.Context, id uuid.UUID) error {
	return s.transition(ctx, id, StatusRetryScheduled, []Status{StatusFailed, StatusDeadLettered})
}

func (s *Service) transition(ctx context.Context, id uuid.UUID, to Status, allowedFrom []Status) error {
	current, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return err
	}
	allowed := false
	for _, from := range allowedFrom {
		if current.Status == from {
			allowed = true
			break
		}
	}
	if !allowed || !scheduler.CanTransition(string(current.Status), string(to)) {
		return fmt.Errorf("%w: cannot transition %s to %s", appErrors.ErrInvalidTransition, current.Status, to)
	}
	return s.repo.TransitionJob(ctx, id, allowedFrom, to)
}

func (s *Service) withDefaults(req CreateRequest) CreateRequest {
	if req.Priority == 0 {
		req.Priority = 5
	}
	if req.MaxRetries == 0 {
		req.MaxRetries = s.defaultRetries
	}
	if req.RetryBackoffSeconds == 0 {
		req.RetryBackoffSeconds = s.defaultBackoffSec
	}
	if req.TimeoutSeconds == 0 {
		req.TimeoutSeconds = s.defaultTimeoutSec
	}
	return req
}

func NextRetryAt(now time.Time, baseBackoffSeconds int, retryCount int) time.Time {
	if baseBackoffSeconds <= 0 {
		baseBackoffSeconds = 30
	}
	if retryCount < 0 {
		retryCount = 0
	}
	multiplier := math.Pow(2, float64(retryCount))
	delay := time.Duration(float64(baseBackoffSeconds)*multiplier) * time.Second
	return now.Add(delay)
}

func ShouldDeadLetter(retryCount, maxRetries int) bool {
	return retryCount >= maxRetries
}

func isKnownStatus(status Status) bool {
	switch status {
	case StatusPending, StatusScheduled, StatusRunning, StatusSucceeded, StatusFailed, StatusRetryScheduled, StatusDeadLettered, StatusCancelled, StatusPaused:
		return true
	default:
		return false
	}
}

func isSupportedJobType(jobType string) bool {
	_, ok := supportedJobTypes[jobType]
	return ok
}
