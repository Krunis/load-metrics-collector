package collectorserver

import (
	"context"
	"io"
	"log"
	"net"
	"sync"

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
	saramaProducer common.Producer

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
		metricCh:     make(chan *pb.MetricRequest, 10000),
		lifecycle:    common.Lifecycle{Ctx: ctx, Cancel: cancel},
	}
}

func (c *CollectorServer) Start() error {
	var err error

	c.saramaProducer, err = NewSaramaProducer([]string{c.kafkaAddress})
	if err != nil {
		return err
	}

	c.wg.Go(c.FromChToKafka)

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
			
			log.Printf("Ошибка получения сообщения: %v", err)
            return err
		}

		log.Printf("Received message: %s %s %v %d", msg.GetMetric(), msg.GetService(), msg.GetValue(), msg.GetTimestamp())

		c.metricCh <- msg

		log.Printf("Длина канала: %v", len(c.metricCh))
		
	}
}

func (c *CollectorServer) Stop() error {
	c.lifecycle.Cancel()

	c.wg.Wait()

	if c.lis != nil {
		c.lis.Close()
	}

	if c.grpcServer != nil {
		c.grpcServer.Stop()
	}

	if c.saramaProducer != nil {
		if err := c.saramaProducer.Close(); err != nil {
			return err
		}
	}

	return nil
}
