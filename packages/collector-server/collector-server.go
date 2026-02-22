package collectorserver

import (
	"context"
	"net"
	"sync"

	pb "github.com/Krunis/load-metrics-collector/packages/grpcapi"
	"google.golang.org/grpc"
)

type Lifecycle struct {
	ctx    context.Context
	cancel context.CancelFunc
}

type CollectorServer struct {
	pb.UnimplementedServiceCollectorServerServer

	address    string
	lis        net.Listener
	grpcServer *grpc.Server

	kafkaAddress string

	stopOnce sync.Once

	wg sync.WaitGroup

	lifecycle Lifecycle
}

func NewCollectorServer(port, kafkaAddress string) *CollectorServer {
	ctx, cancel := context.WithCancel(context.Background())

	return &CollectorServer{
		address:   port,
		kafkaAddress: kafkaAddress,
		lifecycle: Lifecycle{ctx: ctx, cancel: cancel},
	}
}

func (c *CollectorServer) Start() error{
	if err := connectToKafka(); err != nil{
		
	}
}