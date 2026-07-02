package scheduler

import (
	"testing"
	"time"
)

func TestCanTransitionAllowsExpectedJobLifecycle(t *testing.T) {
	tests := []struct {
		name string
		from string
		to   string
	}{
		{"pending to running", StatusPending, StatusRunning},
		{"scheduled to running", StatusScheduled, StatusRunning},
		{"running to succeeded", StatusRunning, StatusSucceeded},
		{"running to retry scheduled", StatusRunning, StatusRetryScheduled},
		{"running to dead lettered", StatusRunning, StatusDeadLettered},
		{"pending to cancelled", StatusPending, StatusCancelled},
		{"paused to scheduled", StatusPaused, StatusScheduled},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !CanTransition(tt.from, tt.to) {
				t.Fatalf("expected transition %s -> %s to be allowed", tt.from, tt.to)
			}
		})
	}
}

func TestCanTransitionRejectsInvalidJobLifecycle(t *testing.T) {
	tests := []struct {
		name string
		from string
		to   string
	}{
		{"succeeded to running", StatusSucceeded, StatusRunning},
		{"cancelled to running", StatusCancelled, StatusRunning},
		{"running to paused", StatusRunning, StatusPaused},
		{"dead lettered to succeeded", StatusDeadLettered, StatusSucceeded},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if CanTransition(tt.from, tt.to) {
				t.Fatalf("expected transition %s -> %s to be rejected", tt.from, tt.to)
			}
		})
	}
}

func TestRetryDecisionSchedulesNextAttemptBeforeMaxRetries(t *testing.T) {
	now := time.Date(2026, 7, 2, 10, 0, 0, 0, time.UTC)
	decision := DecideFailure(now, 1, 3, 30)

	if decision.Status != StatusRetryScheduled {
		t.Fatalf("expected retry scheduled, got %s", decision.Status)
	}
	if decision.NextRetryCount != 2 {
		t.Fatalf("expected next retry count 2, got %d", decision.NextRetryCount)
	}
	want := now.Add(60 * time.Second)
	if !decision.NextRunAt.Equal(want) {
		t.Fatalf("expected next run at %s, got %s", want, decision.NextRunAt)
	}
}

func TestRetryDecisionDeadLettersWhenMaxRetriesExhausted(t *testing.T) {
	now := time.Date(2026, 7, 2, 10, 0, 0, 0, time.UTC)
	decision := DecideFailure(now, 2, 3, 30)

	if decision.Status != StatusDeadLettered {
		t.Fatalf("expected dead lettered, got %s", decision.Status)
	}
	if !decision.NextRunAt.IsZero() {
		t.Fatalf("expected no retry time, got %s", decision.NextRunAt)
	}
}
