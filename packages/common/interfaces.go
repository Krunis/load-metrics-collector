package common

import (
	"time"

	"github.com/IBM/sarama"
)

type Producer interface {
	SendMsg(string, string, string, time.Time)
	Errors() <-chan *sarama.ProducerError
	Close() error
}