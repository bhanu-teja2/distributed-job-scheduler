package observability

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type PrometheusRecorder struct {
	jobsCreated       *prometheus.CounterVec
	jobsCompleted     *prometheus.CounterVec
	jobsFailed        *prometheus.CounterVec
	jobsDeadLettered  *prometheus.CounterVec
	executionDuration *prometheus.HistogramVec
	workerClaimed     prometheus.Counter
	activeWorkers     prometheus.Gauge
}

func NewPrometheusRecorder() *PrometheusRecorder {
	return &PrometheusRecorder{
		jobsCreated: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "jobs_created_total",
			Help: "Total jobs created.",
		}, []string{"job_type"}),
		jobsCompleted: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "jobs_completed_total",
			Help: "Total jobs completed.",
		}, []string{"job_type"}),
		jobsFailed: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "jobs_failed_total",
			Help: "Total failed job attempts.",
		}, []string{"job_type"}),
		jobsDeadLettered: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "jobs_dead_lettered_total",
			Help: "Total jobs moved to the dead-letter queue.",
		}, []string{"job_type"}),
		executionDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "job_execution_duration_seconds",
			Help:    "Job execution duration.",
			Buckets: prometheus.DefBuckets,
		}, []string{"job_type"}),
		workerClaimed: promauto.NewCounter(prometheus.CounterOpts{
			Name: "worker_claimed_jobs_total",
			Help: "Total jobs claimed by workers.",
		}),
		activeWorkers: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "active_workers",
			Help: "Current number of active workers.",
		}),
	}
}

func (r *PrometheusRecorder) JobCreated(jobType string) {
	r.jobsCreated.WithLabelValues(jobType).Inc()
}

func (r *PrometheusRecorder) JobCompleted(jobType string, duration time.Duration) {
	r.jobsCompleted.WithLabelValues(jobType).Inc()
	r.executionDuration.WithLabelValues(jobType).Observe(duration.Seconds())
}

func (r *PrometheusRecorder) JobFailed(jobType string) {
	r.jobsFailed.WithLabelValues(jobType).Inc()
}

func (r *PrometheusRecorder) JobDeadLettered(jobType string) {
	r.jobsDeadLettered.WithLabelValues(jobType).Inc()
}

func (r *PrometheusRecorder) WorkerClaimed(count int) {
	r.workerClaimed.Add(float64(count))
}

func (r *PrometheusRecorder) SetActiveWorkers(count int) {
	r.activeWorkers.Set(float64(count))
}

func Handler() http.Handler {
	return promhttp.Handler()
}
