package job

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestServiceCreateAppliesDefaultsAndScheduledStatus(t *testing.T) {
	repo := &fakeRepo{}
	service := NewService(repo, 3, 30, 300)
	fixedNow := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return fixedNow }

	resp, err := service.Create(context.Background(), CreateRequest{
		Name:    "send email",
		JobType: "SEND_EMAIL",
		Payload: json.RawMessage(`{"to":"user@example.com","subject":"Welcome"}`),
		RunAt:   fixedNow.Add(time.Minute),
	})

	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if resp.Status != StatusScheduled {
		t.Fatalf("expected status %s, got %s", StatusScheduled, resp.Status)
	}
	if repo.created.Priority != 5 {
		t.Fatalf("expected default priority 5, got %d", repo.created.Priority)
	}
	if repo.created.MaxRetries != 3 || repo.created.RetryBackoffSeconds != 30 || repo.created.TimeoutSeconds != 300 {
		t.Fatalf("defaults not applied: %+v", repo.created)
	}
}

func TestServiceCreateRejectsUnsupportedJobType(t *testing.T) {
	service := NewService(&fakeRepo{}, 3, 30, 300)
	_, err := service.Create(context.Background(), CreateRequest{
		Name:    "bad job",
		JobType: "UNKNOWN",
		Payload: json.RawMessage(`{}`),
		RunAt:   time.Now(),
	})
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func TestNextRetryAtUsesExponentialBackoff(t *testing.T) {
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	got := NextRetryAt(now, 30, 2)
	want := now.Add(120 * time.Second)
	if !got.Equal(want) {
		t.Fatalf("expected %s, got %s", want, got)
	}
}

type fakeRepo struct {
	Repository
	created Job
}

func (f *fakeRepo) Create(ctx context.Context, j Job) (Job, error) {
	f.created = j
	j.CreatedAt = time.Now()
	j.UpdatedAt = j.CreatedAt
	return j, nil
}

func (f *fakeRepo) List(ctx context.Context, filter ListFilter) (Page, error) {
	return Page{}, nil
}

func (f *fakeRepo) GetByID(ctx context.Context, id uuid.UUID) (Job, error) {
	return Job{}, nil
}
