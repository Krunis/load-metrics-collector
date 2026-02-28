package worker

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/Krunis/load-metrics-collector/packages/common"
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

func (w *Worker) FromChToKafka() {
	defer w.wg.Done()

	w.wg.Go(func() {
		for {
			select {
			case err := <-w.saramaProducer.Errors():
				if err != nil {
					log.Printf("Error while sending to Kafka: %s", err)
				}
			case <-w.lifecycle.Ctx.Done():
				return
			}
		}
	})

	for {
		select {
		case snapshot := <-w.SnapshotCh:
			for key, value := range snapshot {

				aggrMetric := &AggregatedMetric{
					Service: key.Service,
					Metric: key.Metric,
					Bucket: key.Bucket,
					Count: value.Count,
					Min: value.Min,
					Max: value.Max,
					Avg: value.Sum / float32(value.Count),
					P95: value.P95,
				}

				metricJSON, _ := json.Marshal(aggrMetric)

				w.saramaProducer.SendMsg(
					"aggregated-metrics",
					[]byte(fmt.Sprintf("%s:%s:%s", aggrMetric.Service, aggrMetric.Metric, aggrMetric.Bucket)),
					metricJSON,
				)
			}
		case <-w.lifecycle.Ctx.Done():

		}
	}
}
