package common

import (
	"context"
	"time"

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

type SaramaConsumer struct{
	ConsumerGroup sarama.ConsumerGroup
	Config *sarama.Config

	Handler func(msg *sarama.ConsumerMessage) error
}

func (ap *SaramaAsyncProducer) SendMsg(topic, key, value string, timestamp time.Time) {
	msg := &sarama.ProducerMessage{
				Topic:     topic,
				Key:       sarama.ByteEncoder([]byte(key)),
				Value:     sarama.ByteEncoder([]byte(value)),
				Timestamp: timestamp,
			}

			ap.AsyncProducer.Input() <- msg
}

func (ap *SaramaAsyncProducer) Errors() <-chan *sarama.ProducerError{
	return ap.AsyncProducer.Errors()
}

func (ap *SaramaAsyncProducer) Close() error{
	return ap.AsyncProducer.Close()
}