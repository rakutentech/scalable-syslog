package egress

import (
	"net/url"
	"time"

	"code.cloudfoundry.org/go-loggregator/pulseemitter"

	"github.com/Shopify/sarama"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("KafkaWriter", func() {
	Context("configuration", func() {
		It("parses the broker list", func() {
			cases := []struct {
				url        string
				expBrokers []string
			}{
				{
					url:        "kafka://broker.local/topic",
					expBrokers: []string{"broker.local"},
				},
				{
					url:        "kafka-tls://broker.local:4000/topic?bootstrap.servers=broker2.local",
					expBrokers: []string{"broker.local:4000", "broker2.local"},
				},
				{
					url:        "kafka://broker.local/topic?bootstrap.servers=broker2.local:4000,broker3.local",
					expBrokers: []string{"broker.local", "broker2.local:4000", "broker3.local"},
				},
			}

			for _, c := range cases {
				brokers, _ := parse(c.url)
				Expect(brokers).To(Equal(c.expBrokers))
			}
		})

		It("parses client.id", func() {
			{
				_, config := parse("kafka://broker.local/topic?client.id=myclient")
				Expect(config.ClientID).To(Equal("myclient"))
			}
			{
				_, config := parse("kafka://broker.local/topic?client.id=myclient2&client.id=myclient")
				Expect(config.ClientID).To(Equal("myclient2"))
			}
		})

		It("parses compression.type", func() {
			CompressionDefault := sarama.CompressionSnappy
			Context("no compression.type", func() {
				_, config := parse("kafka://broker.local/topic")
				Expect(config.Producer.Compression).To(Equal(CompressionDefault))
			})
			{
				_, config := parse("kafka://broker.local/topic?compression.type=snappy")
				Expect(config.Producer.Compression).To(Equal(sarama.CompressionSnappy))
			}
			{
				_, config := parse("kafka://broker.local/topic?compression.type=none")
				Expect(config.Producer.Compression).To(Equal(sarama.CompressionNone))
			}
			{
				_, config := parse("kafka://broker.local/topic?compression.type=gzip")
				Expect(config.Producer.Compression).To(Equal(sarama.CompressionGZIP))
			}
			{
				_, config := parse("kafka://broker.local/topic?compression.type=lz4")
				Expect(config.Producer.Compression).To(Equal(sarama.CompressionLZ4))
			}
			{
				_, config := parse("kafka://broker.local/topic?compression.type=WTF")
				Expect(config.Producer.Compression).To(Equal(CompressionDefault))
			}
			{
				_, config := parse("kafka://broker.local/topic?compression.type=gzip&compression.type=none")
				Expect(config.Producer.Compression).To(Equal(sarama.CompressionGZIP))
			}
			{
				_, config := parse("kafka://broker.local/topic?compression.type=WTF&compression.type=none")
				Expect(config.Producer.Compression).To(Equal(CompressionDefault))
			}
		})

		It("parses max.message.bytes", func() {
			{
				_, config := parse("kafka://broker.local/topic")
				Expect(config.Producer.MaxMessageBytes).To(Equal(1000000))
				Expect(config.Producer.Flush.Bytes).To(Equal(config.Producer.MaxMessageBytes / 2))
			}
			{
				_, config := parse("kafka://broker.local/topic?max.message.bytes=1")
				Expect(config.Producer.MaxMessageBytes).To(Equal(1000000))
				Expect(config.Producer.Flush.Bytes).To(Equal(config.Producer.MaxMessageBytes / 2))
			}
			{
				_, config := parse("kafka://broker.local/topic?max.message.bytes=100000000")
				Expect(config.Producer.MaxMessageBytes).To(Equal(1000000))
				Expect(config.Producer.Flush.Bytes).To(Equal(config.Producer.MaxMessageBytes / 2))
			}
			{
				_, config := parse("kafka://broker.local/topic?max.message.bytes=WTF")
				Expect(config.Producer.MaxMessageBytes).To(Equal(1000000))
				Expect(config.Producer.Flush.Bytes).To(Equal(config.Producer.MaxMessageBytes / 2))
			}
			{
				_, config := parse("kafka://broker.local/topic?max.message.bytes=200000")
				Expect(config.Producer.MaxMessageBytes).To(Equal(200000))
				Expect(config.Producer.Flush.Bytes).To(Equal(config.Producer.MaxMessageBytes / 2))
			}
			{
				_, config := parse("kafka://broker.local/topic?max.message.bytes=2000000")
				Expect(config.Producer.MaxMessageBytes).To(Equal(2000000))
				Expect(config.Producer.Flush.Bytes).To(Equal(config.Producer.MaxMessageBytes / 2))
			}
		})

		It("parses the user credentials", func() {
			{
				_, config := parse("kafka://broker.local/topic")
				Expect(config.Net.SASL.Enable).To(BeFalse())
			}
			{
				_, config := parse("kafka://username@broker.local/topic")
				Expect(config.Net.SASL.Enable).To(BeTrue())
				Expect(config.Net.SASL.User).To(Equal("username"))
				Expect(config.Net.SASL.Password).To(BeEmpty())
			}
			{
				_, config := parse("kafka://username:password@broker.local/topic")
				Expect(config.Net.SASL.Enable).To(BeTrue())
				Expect(config.Net.SASL.User).To(Equal("username"))
				Expect(config.Net.SASL.Password).To(Equal("password"))
			}
		})

		It("parses the schema", func() {
			{
				_, config := parse("kafka://broker.local/topic")
				Expect(config.Net.TLS.Enable).To(BeFalse())
			}
			{
				_, config := parse("kafka-tls://broker.local/topic")
				Expect(config.Net.TLS.Enable).To(BeTrue())
			}
		})
	})

	Context("kafka interactions", func() {
		var lc *adapter.spyLogClient
		var cnt pulseemitter.CounterMetric
		var idleT time.Duration
		var ioT time.Duration
		var b *URLBinding

		BeforeEach(func() {
			lc = &adapter.spyLogClient()
			cnt = pulseemitter.CounterMetric{}
			idleT, ioT = 10*time.Second, 20*time.Second
			kw = KafkaRetryWriterConstructor(2, lc)(b, idleT, ioT, true, cnt)
		})

	})
})

func parse(u string) ([]string, *sarama.Config) {
	pu, _ := url.Parse(u)
	binding := &URLBinding{URL: pu, AppID: "AppID", Hostname: "Hostname"}
	return parseConnString(binding, 10*time.Second, 20*time.Second, false)
}
