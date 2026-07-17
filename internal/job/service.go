package job

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/bhanuteja/distributed-job-scheduler/internal/auth"
	appErrors "github.com/bhanuteja/distributed-job-scheduler/internal/errors"
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
	metrics           observability.Recorder
}

func NewService(repo Repository, defaultRetries, defaultBackoffSec, defaultTimeoutSec int) *Service {
	return &Service{
		repo:              repo,
		now:               func() time.Time { return time.Now().UTC() },
		defaultRetries:    defaultRetries,
		defaultBackoffSec: defaultBackoffSec,
		defaultTimeoutSec: defaultTimeoutSec,
		metrics:           observability.NoopRecorder{},
	}
}

func (s *Service) WithMetrics(metrics observability.Recorder) *Service {
	if metrics != nil {
		s.metrics = metrics
	}
	return s
}

func (s *Service) Create(ctx context.Context, req CreateRequest) (CreateResponse, error) {
	req = s.withDefaults(req)
	req.IdempotencyKey = strings.TrimSpace(req.IdempotencyKey)
	if len(req.IdempotencyKey) > 255 {
		return CreateResponse{}, fmt.Errorf("%w: Idempotency-Key exceeds 255 characters", appErrors.ErrInvalidInput)
	}
	if err := validateCreate(req); err != nil {
		return CreateResponse{}, fmt.Errorf("%w: %s", appErrors.ErrInvalidInput, err.Error())
	}

	principal := auth.PrincipalOrDefault(ctx)
	createdBy := principal.ClientName
	requestBytes, _ := json.Marshal(struct {
		Name                string          `json:"name"`
		JobType             string          `json:"job_type"`
		Payload             json.RawMessage `json:"payload"`
		RunAt               time.Time       `json:"run_at"`
		Priority            int             `json:"priority"`
		MaxRetries          int             `json:"max_retries"`
		RetryBackoffSeconds int             `json:"retry_backoff_seconds"`
		TimeoutSeconds      int             `json:"timeout_seconds"`
	}{req.Name, req.JobType, req.Payload, req.RunAt.UTC(), req.Priority, req.MaxRetries, req.RetryBackoffSeconds, req.TimeoutSeconds})
	sum := sha256.Sum256(requestBytes)
	requestHash := hex.EncodeToString(sum[:])
	var idempotencyKey *string
	if req.IdempotencyKey != "" {
		idempotencyKey = &req.IdempotencyKey
	}
	j := Job{
		ID:                  uuid.New(),
		TenantID:            principal.TenantID,
		Name:                req.Name,
		JobType:             req.JobType,
		Payload:             req.Payload,
		Status:              initialStatus(req.RunAt, s.now()),
		Priority:            req.Priority,
		RunAt:               req.RunAt.UTC(),
		MaxRetries:          req.MaxRetries,
		RetryBackoffSeconds: req.RetryBackoffSeconds,
		TimeoutSeconds:      req.TimeoutSeconds,
		CreatedBy:           &createdBy,
		IdempotencyKey:      idempotencyKey,
		RequestHash:         &requestHash,
	}

	inserted, err := s.repo.Create(ctx, j)
	if err != nil {
		return CreateResponse{}, err
	}
	s.metrics.JobCreated(inserted.JobType)
	replayed := inserted.ID != j.ID
	return CreateResponse{JobID: inserted.ID.String(), Status: inserted.Status, Replayed: replayed}, nil
}

func (s *Service) List(ctx context.Context, filter ListFilter) (Page, error) {
	if filter.Status != "" && !isKnownStatus(filter.Status) {
		return Page{}, fmt.Errorf("%w: unsupported status filter", appErrors.ErrInvalidInput)
	}
	if filter.JobType != "" && !isSupportedJobType(filter.JobType) {
		return Page{}, fmt.Errorf("%w: unsupported job_type filter", appErrors.ErrInvalidInput)
	}
	if filter.Sort == "" {
		filter.Sort = "created_at"
	}
	if filter.Order == "" {
		filter.Order = "desc"
	}
	if filter.Sort != "created_at" && filter.Sort != "run_at" && filter.Sort != "priority" {
		return Page{}, fmt.Errorf("%w: unsupported sort", appErrors.ErrInvalidInput)
	}
	if filter.Order != "asc" && filter.Order != "desc" {
		return Page{}, fmt.Errorf("%w: unsupported order", appErrors.ErrInvalidInput)
	}
	if filter.CreatedAfter != nil && filter.CreatedBefore != nil && filter.CreatedAfter.After(*filter.CreatedBefore) {
		return Page{}, fmt.Errorf("%w: created_after must not be after created_before", appErrors.ErrInvalidInput)
	}
	return s.repo.List(ctx, auth.PrincipalOrDefault(ctx).TenantID, filter)
}

func (s *Service) Get(ctx context.Context, id uuid.UUID) (Job, error) {
	return s.repo.GetByID(ctx, auth.PrincipalOrDefault(ctx).TenantID, id)
}

func (s *Service) Attempts(ctx context.Context, id uuid.UUID) ([]Attempt, error) {
	return s.repo.ListAttempts(ctx, auth.PrincipalOrDefault(ctx).TenantID, id)

}

func (s *Service) Events(ctx context.Context, id uuid.UUID) ([]Event, error) {
	return s.repo.ListEvents(ctx, auth.PrincipalOrDefault(ctx).TenantID, id)
}

func (s *Service) ListDeadLetters(ctx context.Context, page, pageSize int) ([]DeadLetterJob, error) {
	return s.repo.ListDeadLetters(ctx, auth.PrincipalOrDefault(ctx).TenantID, page, pageSize)
}

func (s *Service) RequeueDeadLetter(ctx context.Context, deadLetterID uuid.UUID) (Job, error) {
	return s.repo.RequeueDeadLetter(ctx, auth.PrincipalOrDefault(ctx).TenantID, deadLetterID, s.now())
}

func (s *Service) Cancel(ctx context.Context, id uuid.UUID) error {
	if err := s.transition(ctx, id, StatusCancelled, []Status{StatusPending, StatusScheduled, StatusRetryScheduled, StatusRunning}); err != nil {
		return err
	}
	return nil
}

func (s *Service) Pause(ctx context.Context, id uuid.UUID) error {
	return s.transition(ctx, id, StatusPaused, []Status{StatusPending, StatusScheduled})
}

func (s *Service) Resume(ctx context.Context, id uuid.UUID) error {
	return s.transition(ctx, id, StatusScheduled, []Status{StatusPaused})
}

func (s *Service) Retry(ctx context.Context, id uuid.UUID) error {
	return s.transition(ctx, id, StatusRetryScheduled, []Status{StatusFailed})
}

func (s *Service) transition(ctx context.Context, id uuid.UUID, to Status, allowedFrom []Status) error {
	tenantID := auth.PrincipalOrDefault(ctx).TenantID
	current, err := s.repo.GetByID(ctx, tenantID, id)
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
	return s.repo.TransitionJob(ctx, tenantID, id, allowedFrom, to)
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
