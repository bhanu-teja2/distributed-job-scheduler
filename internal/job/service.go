package job

import (
	"context"
	"fmt"
	"math"
	"time"

	appErrors "github.com/bhanuteja/distributed-job-scheduler/internal/errors"
	"github.com/google/uuid"
)

type Service struct {
	repo              Repository
	now               func() time.Time
	defaultRetries    int
	defaultBackoffSec int
	defaultTimeoutSec int
}

func NewService(repo Repository, defaultRetries, defaultBackoffSec, defaultTimeoutSec int) *Service {
	return &Service{
		repo:              repo,
		now:               func() time.Time { return time.Now().UTC() },
		defaultRetries:    defaultRetries,
		defaultBackoffSec: defaultBackoffSec,
		defaultTimeoutSec: defaultTimeoutSec,
	}
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
	return CreateResponse{JobID: inserted.ID.String(), Status: inserted.Status}, nil
}

func (s *Service) List(ctx context.Context, filter ListFilter) (Page, error) {
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
