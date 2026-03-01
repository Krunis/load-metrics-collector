package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	collectorserver "github.com/Krunis/load-metrics-collector/packages/collector-server"
)

func main() {
	coll := collectorserver.NewCollectorServer(":8082", "kafka" + ":9092")

	errCh := make(chan error, 1)

 	errCh <- coll.Start()
		
	stopCh := make(chan os.Signal, 1)
	
	signal.Notify(stopCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stopCh)

	select{
	case err := <-errCh:
		if err != nil{
			log.Printf("Error while starting: %s", err)
		}
	case <-stopCh:
		log.Println("Received OS signal")
	}

	log.Println("Stopping...")
	coll.Stop()
}