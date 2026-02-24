package worker

import (
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/Krunis/load-metrics-collector/packages/common"
)

func (w *Worker) Setup(session sarama.ConsumerGroupSession) error {
	log.Println("Setup")
	return nil
}

func (w *Worker) Cleanup(session sarama.ConsumerGroupSession) error {
	log.Println("Cleanup")
	return nil
}

func (w *Worker) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	const batchSize = 1000
	const maxWait = 50 * time.Millisecond

	batch := make([]*sarama.ConsumerMessage, 0, batchSize)

	timer := time.NewTimer(maxWait)
	defer timer.Stop()

	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				if len(batch) > 0 {
					w.BatchCh <- append([]*sarama.ConsumerMessage(nil), batch...)

					for _, m := range batch {
						session.MarkMessage(m, "")
					}
				}
				return nil
			}

			batch = append(batch, msg)

			if len(batch) >= batchSize {
				w.BatchCh <- append([]*sarama.ConsumerMessage(nil), batch...)

				for _, m := range batch {
					session.MarkMessage(m, "")
				}

				batch = batch[:0]

				timer.Reset(maxWait)
			}
		case <-timer.C:
			if len(batch) > 0 {
				w.BatchCh <- append([]*sarama.ConsumerMessage(nil), batch...)

				for _, m := range batch {
					session.MarkMessage(m, "")
				}

				batch = batch[:0]
			}

			timer.Reset(maxWait)
		case <-session.Context().Done():
			return session.Context().Err()
		}
	}
}

func NewSaramaConsumer(brokers []string, groupID string, handler func(msg *sarama.ConsumerMessage)) (*common.SaramaConsumer, error) {
	config := sarama.NewConfig()

	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, err
	}

	return &common.SaramaConsumer{
		ConsumerGroup: consumerGroup,
		Config:        config,
	}, nil
}
