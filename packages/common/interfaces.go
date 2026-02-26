package common

import (
	"github.com/IBM/sarama"
)

type Producer interface {
	SendMsg(string, []byte, []byte)
	Errors() <-chan *sarama.ProducerError
	Close() error
}