package app_test

import (
	"sync"

	"golang.org/x/net/context"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
)

func NewTestAdapterServer() *testAdapterServer {
	return &testAdapterServer{
		ActualCreateBindingRequest: make(chan *v1.CreateBindingRequest, 10),
		ActualDeleteBindingRequest: make(chan *v1.DeleteBindingRequest, 10),
	}
}

type testAdapterServer struct {
	ActualCreateBindingRequest chan *v1.CreateBindingRequest
	ActualDeleteBindingRequest chan *v1.DeleteBindingRequest
	mu                         sync.Mutex
	Bindings                   []*v1.Binding
}

func (t *testAdapterServer) ListBindings(context.Context, *v1.ListBindingsRequest) (*v1.ListBindingsResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return &v1.ListBindingsResponse{Bindings: t.Bindings}, nil
}

func (t *testAdapterServer) CreateBinding(c context.Context, r *v1.CreateBindingRequest) (*v1.CreateBindingResponse, error) {
	t.mu.Lock()
	t.Bindings = append(t.Bindings, r.Binding)
	t.mu.Unlock()

	t.ActualCreateBindingRequest <- r

	return new(v1.CreateBindingResponse), nil
}

func (t *testAdapterServer) DeleteBinding(c context.Context, r *v1.DeleteBindingRequest) (*v1.DeleteBindingResponse, error) {
	t.mu.Lock()
	oldBindings := t.Bindings
	t.Bindings = nil
	for _, b := range oldBindings {
		if b.AppId == r.Binding.AppId && b.Drain == r.Binding.Drain {
			continue
		}
		t.Bindings = append(t.Bindings, b)
	}
	t.mu.Unlock()

	t.ActualDeleteBindingRequest <- r

	return new(v1.DeleteBindingResponse), nil
}
