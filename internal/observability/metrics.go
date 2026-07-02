package observability

import (
	"time"
)

type Recorder interface {
	JobCreated(jobType string)
	JobCompleted(jobType string, duration time.Duration)
	JobFailed(jobType string)
	JobDeadLettered(jobType string)
	WorkerClaimed(count int)
	SetActiveWorkers(count int)
}

type NoopRecorder struct{}

func (NoopRecorder) JobCreated(jobType string)                           {}
func (NoopRecorder) JobCompleted(jobType string, duration time.Duration) {}
func (NoopRecorder) JobFailed(jobType string)                            {}
func (NoopRecorder) JobDeadLettered(jobType string)                      {}
func (NoopRecorder) WorkerClaimed(count int)                             {}
func (NoopRecorder) SetActiveWorkers(count int)                          {}
