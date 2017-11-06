package egress

import (
	"crypto/tls"
	"fmt"
	"net/url"
	"time"

	"code.cloudfoundry.org/go-loggregator/pulseemitter"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"

	"github.com/Shopify/sarama"
)

// Kafka DSN: kafka[-tls]://[username[:password]@]hostname[:port]/topic[?opts]

type KafkaWriter struct {
	hostname     string
	appID        string
	url          *url.URL
	client       *sarama.AsyncProducer
	egressMetric pulseemitter.CounterMetric
	doneCh       chan struct{}
}

type msgMetadata struct {
	retries int
	env     *loggregator_v2.Envelope
}

func NewKafkaWriter(
	binding *URLBinding,
	dialTimeout time.Duration,
	ioTimeout time.Duration,
	skipCertVerify bool,
	egressMetric pulseemitter.CounterMetric,
) WriteCloser {

	config := sarama.NewConfig()
	config.Net.DialTimeout = dialTimeout
	config.Net.ReadTimeout = ioTimeout
	config.Net.WriteTimeout = ioTimeout
	config.Net.KeepAlive = 30 * time.Second

	if binding.URL.Scheme() == "kafka-tls" {
		config.Net.TLS.Config = *tls.Config{InsecureSkipVerify: skipCertVerify}
		config.Net.TLS.Enable = true
	}

	if binding.URL.User != nil {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = binding.URL.User.Username()
		config.Net.SASL.Password = binding.URL.User.Password()
	}

	config.Producer.Flush.Bytes = 500000
	config.Producer.Flush.Messages = 1000
	config.Producer.Flush.Frequency = 1 * time.Second
	config.Producer.Compression = sarama.CompressionSnappy
	config.ClientID = "Cloud Foundry"

	client := sarama.NewAsyncProducer(binding.URL.Host, config)

	w := &KafkaWriter{
		appID:        binding.AppID,
		hostname:     binding.Hostname,
		topic:        binding.URL.Path,
		client:       client,
		egressMetric: egressMetric,
		doneCh:       make(chan struct{}),
	}

	go w.resultConsumer()

	return w
}

func (w *KafkaWriter) Write(env *loggregator_v2.Envelope) error {
	msgs := generateRFC5424Messages(env, w.hostname, w.appID)

	for _, msg := range msgs {
		b, err := msg.MarshalBinary()
		if err != nil {
			return err
		}

		w.client.Input() <- &sarama.ProducerMessage{
			Topic: w.topic,
			Value: ByteEncoder(b),
			Metadata: msgMetadata{
				retries: 2,
				env:     env,
			},
		}
	}

	return nil
}

func (w *KafkaWriter) resultConsumer() {
	for {
		select {
		case e, ok := <-w.client.Errors():
			if !ok {
				continue
			}
			md := e.Msg.Metadata.(msgMetadata)
			if md.retries > 0 {
				md.retries -= 1
				e.Msg.Metadata = md
				select {
				case w.client.Input() <- e.Msg:
					continue
				default:
					// the input channel is full: drop the message
				}
			}
			w.emitLGRLog(md.env, e.Error())
			w.egressFailures.Increment(1)
		case _, ok := <-w.client.Successes():
			if !ok {
				continue
			}
			w.egressMetric.Increment(1)
		case <-w.doneCh:
			return
		}
	}
}

func (w *KafkaWriter) emitLGRLog(e *loggregator_v2.Envelope, err error) {
	w.logClient.EmitLog(
		fmt.Sprintf("Kafka Drain: Error when writing to %s, error: %s", w.binding.URL.Host, err),
		loggregator.WithAppInfo(
			w.appID,
			"LGR",
			e.GetTags()["source_instance"],
		),
	)
}

func (w *KafkaWriter) Close() error {
	w.Close()
	close(w.doneCh)
	return nil
}
