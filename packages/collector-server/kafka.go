package collectorserver

import (
	"time"

	"github.com/IBM/sarama"
)

type SaramaProducer struct {
	asyncProducer sarama.AsyncProducer
	config       *sarama.Config
}

func NewSaramaProducer(brokerList []string) (*SaramaProducer, error) {
	config := sarama.NewConfig()

	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Idempotent = true
	config.Net.MaxOpenRequests = 1

	config.Producer.Retry.Max = 10
	config.Producer.Retry.Backoff = 100 * time.Millisecond

	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	config.Producer.Partitioner = sarama.NewHashPartitioner

	config.Producer.Compression = sarama.CompressionSnappy

	config.Producer.Flush.Bytes = 100000 // 100 KB
	config.Producer.Flush.Messages = 1000
	config.Producer.Flush.Frequency = 50 * time.Millisecond

	config.Producer.Timeout = 30 * time.Second
	config.Net.DialTimeout = 30 * time.Second
	config.Net.ReadTimeout = 30 * time.Second
	config.Net.WriteTimeout = 30 * time.Second

	producer, err := sarama.NewAsyncProducer(brokerList, config)
	if err != nil {
		return nil, err
	}

	return &SaramaProducer{
		asyncProducer: producer,
		config:       config}, nil
}

func connectToKafka() {
	sarama.AsyncProducer.Input() 
}