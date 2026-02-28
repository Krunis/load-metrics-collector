package common

import (
	"github.com/IBM/sarama"
)

type Producer interface {
	SendMsg(topic string, key, value []byte)
	Errors() <-chan *sarama.ProducerError
	Close() error
}