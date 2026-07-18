package observability

import (
	"time"
)

// Recorder is the metrics boundary used by domain and worker services.
type Recorder interface {
	JobCreated(jobType string)
	JobCompleted(jobType string, duration time.Duration)
	JobFailed(jobType string)
	JobDeadLettered(jobType string)
	WorkerClaimed(count int)
	SetActiveWorkers(count int)
}

// NoopRecorder disables metrics while preserving the Recorder contract.
type NoopRecorder struct{}

// JobCreated satisfies Recorder without recording a metric.
func (NoopRecorder) JobCreated(jobType string) {}

// JobCompleted satisfies Recorder without recording a metric.
func (NoopRecorder) JobCompleted(jobType string, duration time.Duration) {}

// JobFailed satisfies Recorder without recording a metric.
func (NoopRecorder) JobFailed(jobType string) {}

// JobDeadLettered satisfies Recorder without recording a metric.
func (NoopRecorder) JobDeadLettered(jobType string) {}

// WorkerClaimed satisfies Recorder without recording a metric.
func (NoopRecorder) WorkerClaimed(count int) {}

// SetActiveWorkers satisfies Recorder without recording a metric.
func (NoopRecorder) SetActiveWorkers(count int) {}
