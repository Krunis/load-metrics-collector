package batcher

import (
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/Krunis/load-metrics-collector/packages/common"
)

func NewSaramaConsumer(brokers []string, groupID string) (*common.SaramaConsumer, error) {
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

func (b *Batcher) Setup(session sarama.ConsumerGroupSession) error {
	log.Println("Setup")
	return nil
}

func (b *Batcher) Cleanup(session sarama.ConsumerGroupSession) error {
	log.Println("Cleanup")
	return nil
}

func (b *Batcher) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	const batchSize = 1000
	const maxWait = 50 * time.Millisecond

	batch := make([]*sarama.ConsumerMessage, 0, batchSize)

	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				
				}
				return nil
			

			
		case <-session.Context().Done():
			return session.Context().Err()
		}
	}
}