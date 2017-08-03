package egress_test

import (
	"errors"
	"math/rand"
	"time"

	"code.cloudfoundry.org/go-loggregator/pulseemitter"
	v2 "code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/scalable-syslog/adapter/internal/egress"
	"golang.org/x/net/context"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Retry Writer", func() {
	It("calls through to a syslog writer", func() {
		writeCloser := &spyWriteCloser{}
		r := buildRetryWriter(writeCloser, 1)
		env := &v2.Envelope{}

		r.Write(env)

		Expect(writeCloser.writeCalled).To(BeTrue())
		Expect(writeCloser.writeEnvelope).To(Equal(env))
	})

	It("retries writes if the delegation to syslog writer fails", func() {
		writeCloser := &spyWriteCloser{
			returnErrCount: 1,
			writeErr:       errors.New("write error"),
		}
		r := buildRetryWriter(writeCloser, 3)
		env := &v2.Envelope{}

		r.Write(env)

		Eventually(func() int {
			return writeCloser.writeAttempts
		}).Should(Equal(2))
	})

	It("returns an error when there are no more retries", func() {
		writeCloser := &spyWriteCloser{
			returnErrCount: 3,
			writeErr:       errors.New("write error"),
		}
		r := buildRetryWriter(writeCloser, 2)
		env := &v2.Envelope{}

		err := r.Write(env)

		Expect(err).To(HaveOccurred())
	})

	// It returns an error when the retry fails
	// It bails out on try when the binding is deleted

	// It("closes a connection", func() {
	// })

	Describe("ExponentialDuration", func() {
		var backoffTests = []struct {
			attempt  uint
			expected time.Duration
		}{
			{0, 1000},
			{1, 1000},
			{2, 2000},
			{3, 4000},
			{4, 8000},
			{5, 16000},
			{11, 1024000},    //1.024s
			{12, 2048000},    //2.048s
			{20, 524288000},  //8m and a bit
			{21, 1048576000}, //17m28.576s
			{22, 2097152000}, //34m57.152s
		}

		It("backs off exponentially with different random seeds starting at 1ms", func() {
			rand.Seed(1)
			for _, bt := range backoffTests {
				delta := int(bt.expected / 10)

				for i := 0; i < 10; i++ {
					backoff := egress.ExponentialDuration(bt.attempt)

					Expect(bt.expected.Seconds()).To(BeNumerically("~", backoff.Seconds(), delta))
				}
			}
		})
	})
})

type spyWriteCloser struct {
	writeCalled   bool
	writeEnvelope *v2.Envelope
	writeAttempts int

	returnErrCount int
	writeErr       error
}

func (s *spyWriteCloser) Write(env *v2.Envelope) error {
	var err error
	if s.writeAttempts < s.returnErrCount {
		err = s.writeErr
	}
	s.writeAttempts += 1
	s.writeCalled = true
	s.writeEnvelope = env

	return err
}

func (*spyWriteCloser) Close() error {
	return nil
}

func noDelay(uint) time.Duration {
	return 0
}

func buildRetryWriter(w *spyWriteCloser, maxRetries uint) egress.WriteCloser {
	constructor := egress.NewRetryWriter(func(
		ctx context.Context,
		binding *egress.URLBinding,
		dialTimeout time.Duration,
		ioTimeout time.Duration,
		skipCertVerify bool,
		egressMetric *pulseemitter.CounterMetric,
	) egress.WriteCloser {
		return w
	}, egress.RetryDuration(noDelay), maxRetries)

	return constructor(nil, nil, 0, 0, false, nil)
}
