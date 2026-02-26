package common

import (
	"context"
	"github.com/IBM/sarama"
)

type Lifecycle struct {
	Ctx    context.Context
	Cancel context.CancelFunc
}

type SaramaAsyncProducer struct {
	AsyncProducer sarama.AsyncProducer
	Config        *sarama.Config
}

type MetricForAggr struct {
	Service, Metric string
	Value float32
	TimestampUnix          int64
}

type SaramaConsumer struct {
	ConsumerGroup sarama.ConsumerGroup
	Config        *sarama.Config

	Handler func(msg *sarama.ConsumerMessage) error
}

func (ap *SaramaAsyncProducer) SendMsg(topic string, key, value []byte) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	ap.AsyncProducer.Input() <- msg
}

func (ap *SaramaAsyncProducer) Errors() <-chan *sarama.ProducerError {
	return ap.AsyncProducer.Errors()
}

func (ap *SaramaAsyncProducer) Close() error {
	return ap.AsyncProducer.Close()
}
