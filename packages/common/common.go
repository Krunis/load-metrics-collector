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

type SaramaConsumer struct{
	ConsumerGroup sarama.ConsumerGroup
	Config *sarama.Config

	consumerData ConsumerData
}

type ConsumerData struct{
	Topics []string

	Handler func(msg *sarama.ConsumerMessage) error
}