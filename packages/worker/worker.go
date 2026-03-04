package worker

import (
	"context"
	"fmt"
	"log"
	"strings"
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
		AccMap:       map[AggrKey]*Accumulator{},
		BatchCh:      make(chan []*sarama.ConsumerMessage, 100),
		SnapshotCh:   make(chan map[AggrKey]*Accumulator, 20),
		lifecycle:    common.Lifecycle{Ctx: ctx, Cancel: cancel},
	}
}

func (w *Worker) Start(topics []string) error {
	var err error

	w.saramaProducer, err = NewSaramaProducer([]string{w.kafkaAddress})
	if err != nil {
		return err
	}

	w.saramaConsumer, err = NewSaramaConsumer([]string{w.kafkaAddress}, "A")
	if err != nil {
		return err
	}

	w.wg.Go(w.fromChToKafka) //kafka<- <-SnapshotCh

	w.wg.Go(w.flushCycle) //SnapshotCh <-AccMap

	w.wg.Go(w.aggrCycle) //AccMap <-BatchCh

	if err := w.startConsuming(topics); err != nil { // BatchCh<- <-kafka
		log.Printf("Error while consuming from Kafka: %s", err)
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

	defer log.Println("aggrCycle stopped")

	for {
		select {
		case batch := <-w.BatchCh:
			log.Printf("From BatchCh: %v/100", len(w.BatchCh))

			w.AggregateBatch(batch)
		case <-w.lifecycle.Ctx.Done():
			return
		}
	}
}

func (w *Worker) flushCycle() {
	defer w.wg.Done()

	defer log.Println("flushCycle stopped")

	timer := time.NewTimer(time.Second * 1)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			now := time.Now().Unix()

			w.AccMapMutex.Lock()
			snapshot := make(map[AggrKey]*Accumulator)

			for key, value := range w.AccMap {
				if key.Bucket < now {
					snapshot[key] = value
					snapshot[key].findP95()
					delete(w.AccMap, key)
				}
			}

			w.AccMapMutex.Unlock()

			if len(snapshot) > 0 {
				w.SnapshotCh <- snapshot

				log.Printf("In SnapshotCh: %v/20", len(w.SnapshotCh))
			}

			timer.Reset(time.Second * 1)
		case <-w.lifecycle.Ctx.Done():
			return
		}
	}
}

func (w *Worker) Stop() error {
	w.lifecycle.Cancel()

	w.wg.Wait()

	errs := []string{}

	if w.saramaConsumer.ConsumerGroup != nil {
		if err := w.saramaConsumer.ConsumerGroup.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}

	if w.saramaProducer != nil {
		if err := w.saramaProducer.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}

	return fmt.Errorf("%s", strings.Join(errs, " "))
}
