package worker

import (
	"context"
	"log"
	"sync"

	"github.com/IBM/sarama"
	"github.com/Krunis/load-metrics-collector/packages/common"
)

type Worker struct {
	kafkaAddress   string
	saramaProducer common.Producer
	saramaConsumer *common.SaramaConsumer

	AccMap map[AggrKey]*Accumulator

	BatchCh chan []*sarama.ConsumerMessage

	stopOnce sync.Once

	wg sync.WaitGroup

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

	w.saramaConsumer, err = NewSaramaConsumer([]string{w.kafkaAddress}, "A")
	if err != nil {
		return err
	}

	if err := w.startConsuming(topics); err != nil {
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

func (w *Worker) pollingBatchCh() error{
	defer w.wg.Done()

	for{
		select {
		case batch := <- w.BatchCh:
			result, err := w.FindP95(batch)
			if err != nil{
				log.Printf("Error while p95 %s: %s", batch[0].Key, err)
				return nil
			}

			//mutex
			w.AccMap[AggrKey(batch[0].Key)].P95 = result
			
		case <-w.lifecycle.Ctx.Done():
			return nil
		}}
	}


