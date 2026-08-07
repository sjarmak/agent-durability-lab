package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/temporalio-labs/agent-durability-lab/experiments/external-effects/internal/lab"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "external-effect Worker: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var address string
	var namespace string
	var taskQueue string
	var workerID string
	flag.StringVar(&address, "address", "", "Temporal frontend address")
	flag.StringVar(&namespace, "namespace", "default", "Temporal namespace")
	flag.StringVar(&taskQueue, "task-queue", "", "Temporal task queue")
	flag.StringVar(&workerID, "worker-id", "", "stable experiment Worker ID")
	flag.Parse()
	if address == "" || namespace == "" || taskQueue == "" || workerID == "" {
		return errors.New("address, namespace, task queue, and Worker ID are required")
	}
	temporalClient, err := client.Dial(client.Options{HostPort: address, Namespace: namespace})
	if err != nil {
		return fmt.Errorf("connect to Temporal: %w", err)
	}
	defer temporalClient.Close()
	temporalWorker := worker.New(temporalClient, taskQueue, worker.Options{})
	lab.RegisterWorker(temporalWorker, workerID)
	if err := temporalWorker.Run(worker.InterruptCh()); err != nil {
		return fmt.Errorf("run Temporal Worker: %w", err)
	}
	return nil
}
