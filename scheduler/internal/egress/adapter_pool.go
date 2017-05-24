package egress

import (
	"log"

	"github.com/prometheus/client_golang/prometheus"

	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"

	"google.golang.org/grpc"
)

type AdapterPool []v1.AdapterClient

var (
	// metric-documentation-v2: DrainCount will keep track of the number of
	// syslog drains that have been registered with an adapter.
	adapterCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "scalablesyslog", // deployment name
			Subsystem: "scheduler",      // job name
			Name:      "adapter_count",
			Help:      "Number of adpaters registered",
		},
	)
)

func init() {
	prometheus.MustRegister(adapterCount)
}

func NewAdapterPool(addrs []string, opts ...grpc.DialOption) AdapterPool {
	var pool AdapterPool

	for _, addr := range addrs {
		conn, err := grpc.Dial(addr, opts...)
		if err != nil {
			log.Printf("error dialing adapter: %v", err)
			continue
		}

		c := v1.NewAdapterClient(conn)

		pool = append(pool, c)
	}

	adapterCount.Set(float64(len(pool)))

	return pool
}

func (a AdapterPool) Subset(index, count int) AdapterPool {
	var pool AdapterPool

	if len(a) < count {
		return a
	}

	if index+count >= len(a) {
		missing := (index + count) - len(a)

		pool = a[index:]
		return append(pool, a[0:missing]...)
	}

	return a[index : index+count]
}
