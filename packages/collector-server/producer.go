package collectorserver

import (
	"encoding/json"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/Krunis/load-metrics-collector/packages/common"
	pb "github.com/Krunis/load-metrics-collector/packages/grpcapi"
)

func NewSaramaProducer(brokerList []string) (*common.SaramaAsyncProducer, error) {
	config := sarama.NewConfig()

	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Idempotent = true
	config.Net.MaxOpenRequests = 1

	config.Producer.Retry.Max = 10
	config.Producer.Retry.Backoff = 100 * time.Millisecond

	config.Producer.Return.Successes = false
	config.Producer.Return.Errors = true

	config.Producer.Partitioner = sarama.NewHashPartitioner

	config.Producer.Compression = sarama.CompressionSnappy

	config.Producer.Flush.Bytes = 100000 // 100 KB
	config.Producer.Flush.Messages = 1000
	config.Producer.Flush.Frequency = 20 * time.Millisecond

	config.Producer.Timeout = 30 * time.Second
	config.Net.DialTimeout = 30 * time.Second
	config.Net.ReadTimeout = 30 * time.Second
	config.Net.WriteTimeout = 30 * time.Second

	producer, err := sarama.NewAsyncProducer(brokerList, config)
	if err != nil {
		return nil, err
	}

	return &common.SaramaAsyncProducer{
		AsyncProducer: producer,
		Config:        config}, nil
}

func (c *CollectorServer) FromChToKafka() {
	var metric *pb.MetricRequest

	defer log.Println("FromChToKafka stopped")

	defer c.wg.Done()

	c.wg.Go(func() {
		for {
			select {
			case err := <-c.saramaProducer.Errors():
				if err != nil {
					log.Printf("Error while sending to Kafka: %s", err)
				}
			case <-c.lifecycle.Ctx.Done():
				return
			}
		}
	})

	for {
		select {
		case metric = <-c.metricCh:
			log.Printf("Из канала %v/10000", len(c.metricCh))

			valueJSON, _ := json.Marshal(common.MetricForAggr{
				Service:       metric.GetService(),
				Metric:        metric.GetMetric(),
				Value:         metric.GetValue(),
				TimestampUnix: metric.GetTimestamp(),
			})

			c.saramaProducer.SendMsg(
				"raw-metrics",
				[]byte(metric.GetService()+":"+metric.GetMetric()),
				valueJSON,
			)

			log.Println(time.Now().UnixMilli())

			// log.Printf("Sent in Kafka:\nkey: %s\nvalue: %v", metric.GetService()+":"+metric.GetMetric(), common.MetricForAggr{
			// 	Service:       metric.GetService(),
			// 	Metric:        metric.GetMetric(),
			// 	Value:         metric.GetValue(),
			// 	TimestampUnix: metric.GetTimestamp(),
			// })
		case <-c.lifecycle.Ctx.Done():
			return
		}
	}
}
