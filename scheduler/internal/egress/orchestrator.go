// Package orchestrator writes syslog drain bindings to adapters.
package egress

import (
	"log"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"code.cloudfoundry.org/scalable-syslog/internal/metricemitter"
	"code.cloudfoundry.org/scalable-syslog/scheduler/internal/ingress"
)

type BindingReader interface {
	FetchBindings() (appBindings ingress.Bindings, invalid int, err error)
}

type AdapterService interface {
	CreateDelta(actual ingress.Bindings, expected ingress.Bindings)
	DeleteDelta(actual ingress.Bindings, expected ingress.Bindings)
	List() ingress.Bindings
}

var (
	// metric-documentation-v2: DrainCount will keep track of the number of
	// syslog drains that have been registered with an adapter.
	drainCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "scalablesyslog", // deployment name
			Subsystem: "scheduler",      // job name
			Name:      "drain_count",
			Help:      "Number of drains registered",
		},
	)

	// metric-documentation-v2: blacklistedOrInvalidUrlCount will keep track
	// of the number of syslog drains that are blacklisted or invalid
	blacklistUrlCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "scalablesyslog", // deployment name
			Subsystem: "scheduler",      // job name
			Name:      "blacklistedOrInvalidUrlCount",
			Help:      "Number of blacklisted or invalid urls",
		},
	)
)

func init() {
	prometheus.MustRegister(drainCount)
	prometheus.MustRegister(blacklistUrlCount)
}

// Orchestrator manages writes to a number of adapters.
type Orchestrator struct {
	reader     BindingReader
	service    AdapterService
	drainGauge *metricemitter.GaugeMetric
}

type MetricEmitter interface {
	NewGaugeMetric(name, unit string, opts ...metricemitter.MetricOption) *metricemitter.GaugeMetric
}

// NewOrchestrator creates a new orchestrator.
func NewOrchestrator(
	r BindingReader,
	s AdapterService,
	m MetricEmitter,
) *Orchestrator {
	return &Orchestrator{
		reader:  r,
		service: s,
		drainGauge: m.NewGaugeMetric(
			"drains",
			"count",
			metricemitter.WithVersion(2, 0),
		),
	}
}

// Run starts the orchestrator.
func (o *Orchestrator) Run(interval time.Duration) {
	for range time.Tick(interval) {
		expected, blacklisted, err := o.reader.FetchBindings()
		if err != nil {
			log.Printf("fetch bindings failed with error: %s", err)
			continue
		}
		drainCount.Set(float64(len(expected)))
		blacklistUrlCount.Set(float64(blacklisted))

		o.drainGauge.Set(int64(len(expected)))

		actual := o.service.List()

		o.service.DeleteDelta(actual, expected)
		o.service.CreateDelta(actual, expected)
	}
}
