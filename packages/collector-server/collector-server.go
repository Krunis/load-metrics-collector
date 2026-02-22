package collectorserver

import (
	"context"
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

	kafkaAddress string
	saramaProducer *common.SaramaAsyncProducer

	stopOnce sync.Once

	wg sync.WaitGroup

	lifecycle common.Lifecycle
}

func NewCollectorServer(port, kafkaAddress string) *CollectorServer {
	ctx, cancel := context.WithCancel(context.Background())

	return &CollectorServer{
		address:   port,
		kafkaAddress: kafkaAddress,
		lifecycle: common.Lifecycle{Ctx: ctx, Cancel: cancel},
	}
}

func (c *CollectorServer) Start() error{
	var err error

	c.saramaProducer, err = NewSaramaProducer([]string{c.kafkaAddress})
	if err != nil{
		return err
	}

	c.lis, err = net.Listen("tcp", c.address)
	if err != nil{
		return err
	}

	c.grpcServer = grpc.NewServer()

	pb.RegisterServiceCollectorServerServer(c.grpcServer, c)

	errCh := make(chan error, 1)

	if err = c.Run(); err != nil{
		errCh <- err
	}

	log.Printf("Error while running: %s", <-errCh)

	return nil
}

func (c *CollectorServer) Run() error{
	if err := c.grpcServer.Serve(c.lis); err != nil{
		return err
	}

	return nil
}

func (c *CollectorServer) SendMetric(stream grpc.BidiStreamingServer[pb.MetricRequest, pb.MetricResponse]) error{
	
}