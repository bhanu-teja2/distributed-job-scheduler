package scheduler

import (
	"time"
)

const (
	// Status constants mirror the persisted job lifecycle without importing the
	// job package, keeping this rules package dependency-free.
	StatusPending        = "PENDING"
	StatusScheduled      = "SCHEDULED"
	StatusRunning        = "RUNNING"
	StatusSucceeded      = "SUCCEEDED"
	StatusFailed         = "FAILED"
	StatusRetryScheduled = "RETRY_SCHEDULED"
	StatusDeadLettered   = "DEAD_LETTERED"
	StatusCancelled      = "CANCELLED"
	StatusPaused         = "PAUSED"
)

var transitions = map[string]map[string]struct{}{
	StatusPending: {
		StatusRunning:   {},
		StatusCancelled: {},
		StatusPaused:    {},
	},
	StatusScheduled: {
		StatusRunning:   {},
		StatusCancelled: {},
		StatusPaused:    {},
	},
	StatusRunning: {
		StatusSucceeded:      {},
		StatusFailed:         {},
		StatusRetryScheduled: {},
		StatusDeadLettered:   {},
		StatusCancelled:      {},
	},
	StatusFailed: {
		StatusRetryScheduled: {},
		StatusDeadLettered:   {},
	},
	StatusRetryScheduled: {
		StatusRunning:      {},
		StatusCancelled:    {},
		StatusDeadLettered: {},
	},
	StatusPaused: {
		StatusScheduled: {},
	},
	StatusDeadLettered: {
		StatusRetryScheduled: {},
	},
}

// FailureDecision describes the next persisted state after an execution error.
type FailureDecision struct {
	Status         string
	NextRetryCount int
	NextRunAt      time.Time
}

// CanTransition reports whether the central lifecycle state machine permits a move.
func CanTransition(from, to string) bool {
	next, ok := transitions[from]
	if !ok {
		return false
	}
	_, ok = next[to]
	return ok
}

// DecideFailure increments the retry count exactly once and either schedules
// the next attempt or exhausts the job into the dead-letter state.
func DecideFailure(now time.Time, retryCount, maxRetries, retryBackoffSeconds int) FailureDecision {
	nextRetryCount := retryCount + 1
	if nextRetryCount > maxRetries {
		return FailureDecision{Status: StatusDeadLettered, NextRetryCount: nextRetryCount}
	}
	return FailureDecision{
		Status:         StatusRetryScheduled,
		NextRetryCount: nextRetryCount,
		NextRunAt:      NextRetryAt(now, retryBackoffSeconds, nextRetryCount),
	}
}

// NextRetryAt calculates uncapped exponential backoff for an attempt number.
func NextRetryAt(now time.Time, baseBackoffSeconds int, attemptNumber int) time.Time {
	if baseBackoffSeconds <= 0 {
		baseBackoffSeconds = 30
	}
	if attemptNumber < 1 {
		attemptNumber = 1
	}
	delay := time.Duration(baseBackoffSeconds) * time.Second
	for i := 1; i < attemptNumber; i++ {
		delay *= 2
	}
	return now.Add(delay)
}
