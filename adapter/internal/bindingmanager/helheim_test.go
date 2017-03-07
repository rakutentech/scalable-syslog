// This file was generated by github.com/nelsam/hel.  Do not
// edit this code by hand unless you *really* know what you're
// doing.  Expect any changes made manually to be overwritten
// the next time hel regenerates this file.

package bindingmanager_test

import v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"

type mockSubscriber struct {
	StartCalled chan bool
	StartInput  struct {
		Binding chan *v1.Binding
	}
	StartOutput struct {
		StopFunc chan func()
	}
}

func newMockSubscriber() *mockSubscriber {
	m := &mockSubscriber{}
	m.StartCalled = make(chan bool, 100)
	m.StartInput.Binding = make(chan *v1.Binding, 100)
	m.StartOutput.StopFunc = make(chan func(), 100)
	return m
}
func (m *mockSubscriber) Start(binding *v1.Binding) (stopFunc func()) {
	m.StartCalled <- true
	m.StartInput.Binding <- binding
	return <-m.StartOutput.StopFunc
}
