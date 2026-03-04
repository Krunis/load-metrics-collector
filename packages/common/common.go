package common

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/jackc/pgx/v4/pgxpool"
)

type Lifecycle struct {
	Ctx    context.Context
	Cancel context.CancelFunc
}

type SaramaAsyncProducer struct {
	AsyncProducer sarama.AsyncProducer
	Config        *sarama.Config
}

func (ap *SaramaAsyncProducer) SendMsg(topic string, key, value []byte) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	select {
        case ap.AsyncProducer.Input() <- msg:
            // OK
        case <-time.After(50 * time.Millisecond):
            log.Printf("⚠️ Таймаут отправки в Sarama, сообщение потеряно")
        }
    }

func (ap *SaramaAsyncProducer) Errors() <-chan *sarama.ProducerError {
	return ap.AsyncProducer.Errors()
}

func (ap *SaramaAsyncProducer) Close() error {
	return ap.AsyncProducer.Close()
}

type MetricForAggr struct {
	Service, Metric string
	Value float32
	TimestampUnix          int64
}

type SaramaConsumer struct {
	ConsumerGroup sarama.ConsumerGroup
	Config        *sarama.Config

	Handler func(msg *sarama.ConsumerMessage) error
}



func GetDBConnectionString() string{
	var missingEnvVars []string

	checkEnvVar := func(envVar, envVarName string) {
		if envVar == "" {
			missingEnvVars = append(missingEnvVars, envVarName)
		}
	}

	dbName := os.Getenv("POSTGRES_DB")
	checkEnvVar(dbName, "POSTGRES_DB")

	dbUser := os.Getenv("POSTGRES_USER")
	checkEnvVar(dbUser, "POSTGRES_USER")

	dbHost := os.Getenv("POSTGRES_HOST")
	if dbHost == "" {
		dbHost = "localhost"
	}

	dbPort := os.Getenv("POSTGRES_PORT")
	checkEnvVar(dbPort, "POSTGRES_PORT")

	dbPassword := os.Getenv("POSTGRES_PASSWORD")
	checkEnvVar(dbPassword, "POSTGRES_PASSWORD")

	if len(missingEnvVars) > 0 {
		log.Fatalf("Required environment variables are not set: %s",
			strings.Join(missingEnvVars, ","))
	}

	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", dbUser, dbPassword, dbHost, dbPort, dbName)
}

func ConnectToDB(ctx context.Context, dbConnectionString string) (*pgxpool.Pool, error) {
	var err error

	timer := time.NewTimer(time.Second * 26)
	defer timer.Stop()

	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			dbPool, err := pgxpool.Connect(ctx, dbConnectionString)
			if err == nil {
				log.Println("Connected to DB")
				return dbPool, nil
			}

			log.Printf("Failed to connect to DB: %s. Retrying...\n", err)

		case <-timer.C:
			return nil, fmt.Errorf("db connection timeout (25s): %v", err)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

