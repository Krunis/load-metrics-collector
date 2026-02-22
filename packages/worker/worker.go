package worker

import (
	"context"
	"sync"

	"github.com/IBM/sarama"
	"github.com/Krunis/load-metrics-collector/packages/common"
)

type Worker struct {
	kafkaAddress string
	saramaProducer *common.SaramaAsyncProducer
	saramaConsumer *common.SaramaConsumer

	stopOnce sync.Once

	lifecycle common.Lifecycle
}

func NewWorker(kafkaAddress string) *Worker{
	ctx, cancel := context.WithCancel(context.Background())

	return &Worker{
		kafkaAddress: kafkaAddress,
		lifecycle: common.Lifecycle{Ctx: ctx, Cancel: cancel},
	}
}

func (w *Worker) Start() error{
	var err error

	topics := []string{"???"}

	w.saramaConsumer, err = NewSaramaConsumer([]string{w.kafkaAddress}, topics, func(msg *sarama.ConsumerMessage) error {return nil}, "A")
	if err != nil{
		return err
	}
}