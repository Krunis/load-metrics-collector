package collectorserver

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"github.com/IBM/sarama"
	"github.com/Krunis/load-metrics-collector/packages/common"
	pb "github.com/Krunis/load-metrics-collector/packages/grpcapi"
	"google.golang.org/grpc"
)

type CollectorServer struct {
	pb.UnimplementedServiceCollectorServerServer

	address    string
	lis        net.Listener
	grpcServer *grpc.Server

	kafkaAddress   string
	saramaProducer *common.SaramaAsyncProducer

	metricCh chan *pb.MetricRequest

	stopOnce sync.Once

	wg sync.WaitGroup

	lifecycle common.Lifecycle
}

func NewCollectorServer(port, kafkaAddress string) *CollectorServer {
	ctx, cancel := context.WithCancel(context.Background())

	return &CollectorServer{
		address:      port,
		kafkaAddress: kafkaAddress,
		metricCh:     make(chan *pb.MetricRequest, 1000),
		lifecycle:    common.Lifecycle{Ctx: ctx, Cancel: cancel},
	}
}

func (c *CollectorServer) Start() error {
	var err error

	c.saramaProducer, err = NewSaramaProducer([]string{c.kafkaAddress})
	if err != nil {
		return err
	}

	go c.fromChToKafka()

	c.lis, err = net.Listen("tcp", c.address)
	if err != nil {
		return err
	}

	c.grpcServer = grpc.NewServer()

	pb.RegisterServiceCollectorServerServer(c.grpcServer, c)

	errCh := make(chan error, 1)

	if err = c.Run(); err != nil {
		errCh <- err
	}

	log.Printf("Error while running: %s", <-errCh)

	return nil
}

func (c *CollectorServer) Run() error {
	if err := c.grpcServer.Serve(c.lis); err != nil {
		return err
	}

	return nil
}

func (c *CollectorServer) SendMetric(stream grpc.ClientStreamingServer[pb.MetricRequest, pb.MetricResponse]) error {
	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return stream.SendAndClose(&pb.MetricResponse{Acknowledged: true})
			}
		}

		c.metricCh <- msg
	}
}

func (c *CollectorServer) fromChToKafka() {
	var metric *pb.MetricRequest
	var msg *sarama.ProducerMessage

	c.wg.Add(1)
	go func() {
		for {
			select {
			case err := <-c.saramaProducer.AsyncProducer.Errors():
				if err != nil {
					log.Printf("Error while sending to Kafka: %s", err)
				}
			case <-c.lifecycle.Ctx.Done():
				c.wg.Done()
				return
			}
		}
	}()

	c.wg.Add(1)
	for {
		select {
		case metric = <-c.metricCh:

			msg = &sarama.ProducerMessage{
				Topic:     fmt.Sprintf("%s-raw-metrics", metric.GetService()),
				Key:       sarama.ByteEncoder([]byte(metric.GetMetric())),
				Value:     sarama.ByteEncoder([]byte(metric.GetValue())),
				Timestamp: metric.GetTimestamp().AsTime(),
			}

			c.saramaProducer.AsyncProducer.Input() <- msg
		case <-c.lifecycle.Ctx.Done():
			c.wg.Done()
			return
		}
	}
}
