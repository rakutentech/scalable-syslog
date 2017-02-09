package controller_test

import (
	"context"

	"github.com/cloudfoundry-incubator/scalable-syslog/adapter/internal/controller"
	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Controller", func() {
	It("returns a list of known drains", func() {
		s := controller.New()

		resp, err := s.Drains(context.Background(), new(v1.DrainsRequest))
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.Drains).To(HaveLen(0))
	})
})