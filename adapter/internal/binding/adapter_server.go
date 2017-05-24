package binding

import (
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"

	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
)

var (
	// metric-documentation-v2: DrainCount will keep track of the number of
	// syslog drains that have been registered with an adapter.
	drainCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "scalablesyslog", // deployment name
			Subsystem: "adapter",        // job name
			Name:      "drain_count",
			Help:      "Number of drains registered",
		},
	)
)

// BindingStore manages the bindings and respective subscriptions
type BindingStore interface {
	Add(binding *v1.Binding)
	Delete(binding *v1.Binding)
	List() (bindings []*v1.Binding)
}

func init() {
	prometheus.MustRegister(drainCount)
}

// AdapterServer implements the v1.AdapterServer interface.
type AdapterServer struct {
	store BindingStore
}

// New returns a new AdapterServer.
func NewAdapterServer(store BindingStore) *AdapterServer {
	return &AdapterServer{
		store: store,
	}
}

// ListBindings returns a list of bindings from the binding manager
func (c *AdapterServer) ListBindings(ctx context.Context, req *v1.ListBindingsRequest) (*v1.ListBindingsResponse, error) {
	return &v1.ListBindingsResponse{Bindings: c.store.List()}, nil
}

// CreateBinding adds a new binding to the binding manager.
func (c *AdapterServer) CreateBinding(ctx context.Context, req *v1.CreateBindingRequest) (*v1.CreateBindingResponse, error) {
	c.store.Add(req.Binding)

	drainCount.Set(float64(len(c.store.List())))
	// c.health.SetCounter(map[string]int{"drainCount": len(c.store.List())})

	return &v1.CreateBindingResponse{}, nil
}

// DeleteBinding removes a binding from the binding manager.
func (c *AdapterServer) DeleteBinding(ctx context.Context, req *v1.DeleteBindingRequest) (*v1.DeleteBindingResponse, error) {
	c.store.Delete(req.Binding)
	drainCount.Set(float64(len(c.store.List())))
	// c.health.SetCounter(map[string]int{"drainCount": len(c.store.List())})

	return &v1.DeleteBindingResponse{}, nil
}
