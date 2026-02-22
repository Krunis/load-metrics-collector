package worker

import (
	"github.com/IBM/sarama"
	"github.com/Krunis/load-metrics-collector/packages/common"
)

func NewSaramaConsumer(brokers, topics []string, handler func(msg *sarama.ConsumerMessage) error, groupID string) (*common.SaramaConsumer, error){
	config := sarama.NewConfig()

	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil{
		return nil, err
	}

	return &common.SaramaConsumer{
		ConsumerGroup: consumerGroup,
		Config: config,
	}, nil
}