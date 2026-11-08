// Package observability exports aggregate service health without caller or chat data.
package observability

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	namespace = "harness"
	completed = "completed"
	canceled  = "canceled"
	failed    = "failed"
)

var durationBuckets = []float64{0.005, 0.025, 0.1, 0.5, 1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600}

type Metrics struct {
	registry *prometheus.Registry
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
	inFlight *prometheus.GaugeVec
	admitted prometheus.Counter
	runs     *prometheus.HistogramVec
}

// New owns an independent registry. snapshot reads only aggregate run counts.
func New(snapshot func() (active, queued, held int)) *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: namespace, Name: "http_requests_total", Help: "Completed HTTP handlers by registered route, method and response status; streaming errors are counted separately in run outcomes."}, []string{"route", "method", "code"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: namespace, Name: "http_request_duration_seconds", Help: "Time until an HTTP handler returns, including the lifetime of streaming responses.", Buckets: durationBuckets}, []string{"route", "method"}),
		inFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: namespace, Name: "http_requests_in_flight", Help: "HTTP handlers currently serving a registered application route."}, []string{"route"}),
		admitted: prometheus.NewCounter(prometheus.CounterOpts{Namespace: namespace, Name: "runs_admitted_total", Help: "Unique admitted turns, including turns waiting in a chat's queue."}),
		runs:     prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: namespace, Name: "run_duration_seconds", Help: "Admitted turns ending by outcome, timed from admission through queueing, generation and persistence.", Buckets: durationBuckets}, []string{"outcome"}),
	}
	m.registry.MustRegister(m.requests, m.duration, m.inFlight, m.admitted, m.runs, collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	for _, outcome := range []string{completed, canceled, failed} {
		m.runs.WithLabelValues(outcome)
	}
	m.registry.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: namespace, Name: "runs_active", Help: "Turns occupying a chat's running slot, including startup and persistence."}, func() float64 { active, _, _ := snapshot(); return float64(active) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: namespace, Name: "runs_queued", Help: "Turns waiting behind an active turn."}, func() float64 { _, queued, _ := snapshot(); return float64(queued) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: namespace, Name: "runs_retained", Help: "Finished device-chat replies retained for clients to collect."}, func() float64 { _, _, held := snapshot(); return float64(held) }),
	)
	return m
}

// Instrument uses the registered pattern, never a caller-supplied URL or header.
// promhttp preserves the response writer's streaming interfaces.
func (m *Metrics) Instrument(pattern string, next http.HandlerFunc) http.HandlerFunc {
	labels := prometheus.Labels{"route": pattern}
	return promhttp.InstrumentHandlerInFlight(m.inFlight.With(labels),
		promhttp.InstrumentHandlerCounter(m.requests.MustCurryWith(labels),
			promhttp.InstrumentHandlerDuration(m.duration.MustCurryWith(labels), next))).ServeHTTP
}

// ServeHTTP is reached through the shim's authenticated /metrics endpoint.
func (m *Metrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}).ServeHTTP(w, r)
}

func (m *Metrics) AdmitRun() {
	if m != nil {
		m.admitted.Inc()
	}
}

func (m *Metrics) FinishRun(admittedAt time.Time, wasCanceled, wasFailed bool) {
	if m == nil {
		return
	}
	outcome := completed
	if wasCanceled {
		outcome = canceled
	} else if wasFailed {
		outcome = failed
	}
	m.runs.WithLabelValues(outcome).Observe(time.Since(admittedAt).Seconds())
}
