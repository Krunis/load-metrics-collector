package batcher

import (
	"context"
	"log"

	"github.com/Krunis/load-metrics-collector/packages/common"
	"github.com/jackc/pgx/v4/pgxpool"
)

type Batcher struct {
	dbPool *pgxpool.Pool

	kafkaAddress string
	saramaConsumer *common.SaramaConsumer

	lifecycle common.Lifecycle
}

func NewBatcher(kafkaAddress string) *Batcher{
	ctx, cancel := context.WithCancel(context.Background())

	return &Batcher{
		kafkaAddress: kafkaAddress,
		lifecycle: common.Lifecycle{
			Ctx: ctx,
			Cancel: cancel,
		},

	}
}

func (b *Batcher) Start(topics []string, dbConnectionString string) error {
	var err error

	b.dbPool, err = common.ConnectToDB(b.lifecycle.Ctx, dbConnectionString)
	if err != nil{
		return err
	}

	b.saramaConsumer, err = NewSaramaConsumer([]string{b.kafkaAddress}, "B")
	if err != nil{
		return err
	}

	if err := b.startConsuming(topics); err != nil {
		log.Printf("Error while consuming from Kafka: %s", err)
	}

	return nil
}

func (b *Batcher) startConsuming(topics []string) error {
	for {
		select {
		case <-b.lifecycle.Ctx.Done():
			return nil
		default:
			ctx, cancel := context.WithCancel(b.lifecycle.Ctx)
			defer cancel()

			if err := b.saramaConsumer.ConsumerGroup.Consume(ctx, topics, b); err != nil {
				return err
			}
		}
	}

}