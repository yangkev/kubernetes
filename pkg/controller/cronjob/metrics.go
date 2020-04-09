package cronjob

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	cronjobSubsystem = "cronjob_controller"
	cronNameKey      = "cronjob"
	namespaceKey     = "namespace"
	skipReasonKey    = "reason"
)

const (
	skipReasonConcurrencyPolicy = "concurrencyPolicy"
	skipReasonMissedDeadline    = "missedDeadline"
	skipReasonError             = "error"
)

var schedulingDecisionInvoke = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Subsystem: cronjobSubsystem,
		Name:      "scheduling_decision_invoke",
		Help:      "Counter that increments when the cronjob controller decides to invoke a CronJob",
	},
	[]string{namespaceKey, cronNameKey})

var schedulingDecisionSkip = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Subsystem: cronjobSubsystem,
		Name:      "scheduling_decision_skip",
		Help:      "Counter that increments when the cronjob controller decides to skip a CronJob invocation",
	},
	[]string{namespaceKey, cronNameKey, skipReasonKey})

var jobSucceeded = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Subsystem: cronjobSubsystem,
		Name:      "job_succeeded",
		Help:      "Counter that increments when the cronjob controller detects a child Job has completed with success",
	}, []string{namespaceKey, cronNameKey})

var jobFailed = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Subsystem: cronjobSubsystem,
		Name:      "job_failed",
		Help:      "Counter that increments when the cronjob controller detects a child Job has completed with failure",
	}, []string{namespaceKey, cronNameKey})

var syncOneWallTimeGauge = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Subsystem: cronjobSubsystem,
		Name:      "sync_one_wall_time_gauge_seconds",
		Help:      "Gauge that observes time in seconds it takes the syncOne function to run. Long wall times indicate potential kube client rate limiting",
	})

var syncOneWallTimeHistogram = prometheus.NewHistogram(
	prometheus.HistogramOpts{
		Subsystem: cronjobSubsystem,
		Name:      "sync_one_wall_time_histogram_seconds",
		Help:      "Histogram that observes time in seconds it takes the syncOne function to run. Long wall times indicate potential kube client rate limiting",
		// Buckets range from 1 microsecond to 1 second by factors of 10
		Buckets: prometheus.ExponentialBuckets(1e-6, 10, 7),
	})

var syncAllWallTimeGauge = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Subsystem: cronjobSubsystem,
		Name:      "sync_all_wall_time_gauge_seconds",
		Help:      "Gauge that observes time in seconds it takes the syncAll function to run. Long wall times indicate potential kube client rate limiting",
	})

var syncAllWallTimeHistogram = prometheus.NewHistogram(
	prometheus.HistogramOpts{
		Subsystem: cronjobSubsystem,
		Name:      "sync_all_wall_time_histogram_seconds",
		Help:      "Histogram that observes time in seconds it takes the syncAll function to run. Long wall times indicate potential kube client rate limiting",
		// Buckets at 1, 3, 5, 7,...61 seconds
		Buckets: prometheus.LinearBuckets(1, 2, 30),
	})

func observeSyncAllWallTime(d time.Duration) {
	syncAllWallTimeGauge.Set(d.Seconds())
	syncAllWallTimeHistogram.Observe(d.Seconds())
}

func observeSyncOneWallTime(d time.Duration) {
	syncOneWallTimeGauge.Set(d.Seconds())
	syncOneWallTimeHistogram.Observe(d.Seconds())
}

var registerOnce sync.Once

func registerMetrics() {
	registerOnce.Do(func() {
		prometheus.MustRegister(schedulingDecisionInvoke)
		prometheus.MustRegister(schedulingDecisionSkip)
		prometheus.MustRegister(jobSucceeded)
		prometheus.MustRegister(jobFailed)
		prometheus.MustRegister(syncOneWallTimeGauge)
		prometheus.MustRegister(syncOneWallTimeHistogram)
		prometheus.MustRegister(syncAllWallTimeGauge)
		prometheus.MustRegister(syncAllWallTimeHistogram)
	})
}
