package worker

import (
	"context"
	"log"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/Krunis/load-metrics-collector/packages/common"
)

type Worker struct {
	kafkaAddress   string
	saramaProducer common.Producer
	saramaConsumer *common.SaramaConsumer

	AccMap      map[AggrKey]*Accumulator
	AccMapMutex sync.Mutex

	BatchCh chan []*sarama.ConsumerMessage

	SnapshotCh chan map[AggrKey]*Accumulator

	stopOnce sync.Once

	wg sync.WaitGroup

	lifecycle common.Lifecycle
}

func NewWorker(kafkaAddress string) *Worker {
	ctx, cancel := context.WithCancel(context.Background())

	return &Worker{
		kafkaAddress: kafkaAddress,
		BatchCh: make(chan []*sarama.ConsumerMessage, 100),
		SnapshotCh: make(chan map[AggrKey]*Accumulator, 20),
		lifecycle:    common.Lifecycle{Ctx: ctx, Cancel: cancel},
	}
}

func (w *Worker) Start(topics []string) error {
	var err error

	w.saramaConsumer, err = NewSaramaConsumer([]string{w.kafkaAddress}, "A")
	if err != nil {
		return err
	}

	w.wg.Go(w.FromChToKafka)

	w.wg.Go(w.flushCycle)

	w.wg.Go(w.aggrCycle)

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

func (w *Worker) aggrCycle() {
	defer w.wg.Done()

	for {
		select {
		case batch := <-w.BatchCh:

			//mutex
			w.AggregateBatch(batch)

		case <-w.lifecycle.Ctx.Done():
			return
		}
	}
}

func (w *Worker) flushCycle() {
	defer w.wg.Done()

	timer := time.NewTimer(time.Second * 1)
	defer timer.Stop()

	select {
	case <-timer.C:
		now := time.Now().Unix()

		w.AccMapMutex.Lock()
		snapshot := make(map[AggrKey]*Accumulator)

		for key, value := range w.AccMap {
			if key.Bucket < now{
				snapshot[key] = value
				snapshot[key].findP95()
				delete(w.AccMap, key)
			}
		}

		w.AccMapMutex.Unlock()

		w.SnapshotCh <- snapshot

		timer.Reset(time.Second * 1)
	case <-w.lifecycle.Ctx.Done():
		return
	}
}

func (a *Accumulator) findP95() {
	vals := a.Values

	sort.Slice(vals, func(i, j int) bool {
		return vals[i] < vals[j]
	})
	idx := int(math.Ceil(0.95 * float64(len(vals)))) - 1

	a.P95 = vals[idx]
}
