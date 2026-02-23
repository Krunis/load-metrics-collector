package worker

import (
	"context"
	"sync"

	"github.com/IBM/sarama"
	"github.com/Krunis/load-metrics-collector/packages/common"
)

type Worker struct {
	kafkaAddress   string
	saramaProducer *common.SaramaAsyncProducer
	saramaConsumer *common.SaramaConsumer

	stopOnce sync.Once

	lifecycle common.Lifecycle
}

func NewWorker(kafkaAddress string) *Worker {
	ctx, cancel := context.WithCancel(context.Background())

	return &Worker{
		kafkaAddress: kafkaAddress,
		lifecycle:    common.Lifecycle{Ctx: ctx, Cancel: cancel},
	}
}

func (w *Worker) Start(topics []string) error {
	var err error

	w.saramaConsumer, err = NewSaramaConsumer([]string{w.kafkaAddress}, "A", func(msg *sarama.ConsumerMessage) {return })
	if err != nil {
		return err
	}

	if err := w.startConsuming(topics); err != nil{
		return err
	}

	return nil
}

func (w *Worker) startConsuming(topics []string) error {
	for {
		select {
		case <-w.lifecycle.Ctx.Done():
			return nil
		default:
			ctx, cancel := context.WithCancel(w.lifecycle.Ctx)
			defer cancel()

			if err := w.saramaConsumer.ConsumerGroup.Consume(ctx, topics, w); err != nil {
				return err
			}
		}
	}

}

