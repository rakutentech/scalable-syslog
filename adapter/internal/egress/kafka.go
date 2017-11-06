package egress

import (
	"crypto/tls"
	"fmt"
	"net/url"
	"time"

	loggregator "code.cloudfoundry.org/go-loggregator"
	"code.cloudfoundry.org/go-loggregator/pulseemitter"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"

	"github.com/Shopify/sarama"
)

// Kafka DSN: kafka[-tls]://[username[:password]@]hostname[:port]/topic[?opts]
// where opts are key=value pairs:
// key: "compression.type", values: "none", "lz4", "snappy", "gzip"
// key: "client.id", values: any valid client ID
// key: "broker[]", values: valid hostname:port of additional broker
//                          (can be repeated for additional brokers)

type KafkaWriter struct {
	hostname      string
	appID         string
	topic         string
	url           *url.URL
	client        sarama.AsyncProducer
	egressMetric  pulseemitter.CounterMetric
	droppedMetric pulseemitter.CounterMetric // FIXME
	retries       int
	logClient     LogClient
	doneCh        chan struct{}
}

type msgMetadata struct {
	retries int
	env     *loggregator_v2.Envelope
}

func KafkaRetryWriterConstructor(
	maxRetries int,
	logClient LogClient,
) WriterConstructor {
	return WriterConstructor(func(
		binding *URLBinding,
		dialTimeout time.Duration,
		ioTimeout time.Duration,
		skipCertVerify bool,
		egressMetric pulseemitter.CounterMetric,
	) WriteCloser {
		return newKafkaWriter(
			binding,
			dialTimeout,
			ioTimeout,
			skipCertVerify,
			egressMetric,
			maxRetries,
			logClient,
		)
	})
}

func newKafkaWriter(
	binding *URLBinding,
	dialTimeout time.Duration,
	ioTimeout time.Duration,
	skipCertVerify bool,
	egressMetric pulseemitter.CounterMetric,
	maxRetries int,
	logClient LogClient,
) *KafkaWriter {

	config := sarama.NewConfig()
	config.Net.DialTimeout = dialTimeout
	config.Net.ReadTimeout = ioTimeout
	config.Net.WriteTimeout = ioTimeout
	config.Net.KeepAlive = 30 * time.Second

	if binding.URL.Scheme == "kafka-tls" {
		config.Net.TLS.Config = &tls.Config{InsecureSkipVerify: skipCertVerify}
		config.Net.TLS.Enable = true
	}

	if binding.URL.User != nil {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = binding.URL.User.Username()
		config.Net.SASL.Password, _ = binding.URL.User.Password()
	}

	config.Producer.Flush.Bytes = 500000
	config.Producer.Flush.Messages = 1000
	config.Producer.Flush.Frequency = 1 * time.Second
	switch binding.URL.Query().Get("compression.type") {
	case "none":
		config.Producer.Compression = sarama.CompressionNone
	default:
		config.Producer.Compression = sarama.CompressionSnappy
	case "lz4":
		config.Producer.Compression = sarama.CompressionLZ4
	case "gzip":
		config.Producer.Compression = sarama.CompressionGZIP
	}

	config.ClientID = "Cloud Foundry"
	if clientID := binding.URL.Query().Get("client.id"); clientID != "" {
		config.ClientID = clientID
	}

	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true

	brokers := append([]string{binding.URL.Host}, binding.URL.Query()["broker[]"]...)

	client, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return nil
	}

	w := &KafkaWriter{
		appID:        binding.AppID,
		hostname:     binding.Hostname,
		topic:        binding.URL.Path,
		client:       client,
		egressMetric: egressMetric,
		retries:      maxRetries,
		logClient:    logClient,
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
			Value: sarama.ByteEncoder(b),
			Metadata: msgMetadata{
				retries: w.retries,
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
			w.droppedMetric.Increment(1)
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

func (w *KafkaWriter) emitLGRLog(e *loggregator_v2.Envelope, err string) {
	w.logClient.EmitLog(
		fmt.Sprintf("Kafka Drain: Error when writing to %s, error: %s", w.url.Host, err),
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
