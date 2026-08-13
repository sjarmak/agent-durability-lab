package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"

	"github.com/sjarmak/temporal_projects/experiments/large-artifact-durability/internal/lab"
	"github.com/sjarmak/temporal_projects/internal/failureinject"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
)

type workerConfig struct {
	address      string
	namespace    string
	taskQueue    string
	workerID     string
	barrierURL   string
	sessionID    string
	storeRoot    string
	externalRoot string
	mode         lab.Mode
	boundary     lab.Boundary
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "large-artifact Worker: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	config := parseFlags()
	if err := config.validate(); err != nil {
		return err
	}
	credential, err := failureinject.ReadCredentialFromEnvironment()
	if err != nil {
		return err
	}
	if !credential.IsSet() {
		return errors.New("failure-injection credential is required")
	}
	barrierClient := failureinject.NewAuthenticatedClient(config.barrierURL, credential)
	arrive := barrierClient.Arrive
	driverHook := lab.BoundaryHook(nil)
	if config.workerID == "worker-1" && config.boundary == lab.BoundaryExternalStorageStored {
		driverHook = func(ctx context.Context, boundary lab.Boundary, _ lab.StoreSnapshot) error {
			err := arrive(ctx, failureinject.Arrival{
				ID:    config.sessionID + "/" + string(boundary) + "/attempt-1",
				Point: string(boundary), SessionID: config.sessionID, Generation: 1,
				ActorID: config.workerID, PID: os.Getpid(),
			})
			if err != nil {
				return err
			}
			return errors.New("external-storage barrier unexpectedly released")
		}
	}
	driver, err := lab.NewFileStorageDriver(config.externalRoot, config.mode, driverHook)
	if err != nil {
		return err
	}
	temporalClient, err := client.Dial(client.Options{
		HostPort:  config.address,
		Namespace: config.namespace,
		ExternalStorage: converter.ExternalStorage{
			Drivers:              []converter.StorageDriver{driver},
			PayloadSizeThreshold: lab.ExternalStorageThreshold,
		},
	})
	if err != nil {
		return fmt.Errorf("connect to Temporal: %w", err)
	}
	defer temporalClient.Close()
	temporalWorker := worker.New(temporalClient, config.taskQueue, worker.Options{Identity: config.workerID})
	lab.RegisterWorker(temporalWorker, lab.Activities{WorkerID: config.workerID, Arrive: arrive}, lab.ExternalActivities{})
	if err := temporalWorker.Run(worker.InterruptCh()); err != nil {
		return fmt.Errorf("run Temporal Worker: %w", err)
	}
	return nil
}

func parseFlags() workerConfig {
	var config workerConfig
	var mode string
	var boundary string
	flag.StringVar(&config.address, "address", "", "Temporal frontend address")
	flag.StringVar(&config.namespace, "namespace", "default", "Temporal namespace")
	flag.StringVar(&config.taskQueue, "task-queue", "", "Temporal task queue")
	flag.StringVar(&config.workerID, "worker-id", "", "stable experiment Worker identity")
	flag.StringVar(&config.barrierURL, "barrier-url", "", "loopback failure-barrier URL")
	flag.StringVar(&config.sessionID, "session-id", "", "stable artifact session identity")
	flag.StringVar(&config.storeRoot, "store-root", "", "durable application artifact store")
	flag.StringVar(&config.externalRoot, "external-root", "", "durable SDK external payload store")
	flag.StringVar(&mode, "mode", "", "unsafe or protected")
	flag.StringVar(&boundary, "boundary", "", "exact failure boundary")
	flag.Parse()
	config.mode = lab.Mode(mode)
	config.boundary = lab.Boundary(boundary)
	return config
}

func (c workerConfig) validate() error {
	parsedBarrier, err := url.Parse(c.barrierURL)
	if err != nil || parsedBarrier.Scheme != "http" || parsedBarrier.Hostname() == "" {
		return errors.New("worker requires an HTTP loopback barrier URL")
	}
	ip := net.ParseIP(parsedBarrier.Hostname())
	if ip == nil || !ip.IsLoopback() {
		return errors.New("worker barrier must use a loopback IP address")
	}
	if c.address == "" || c.namespace == "" || c.taskQueue == "" || c.workerID == "" ||
		c.sessionID == "" || !filepath.IsAbs(c.storeRoot) || !filepath.IsAbs(c.externalRoot) ||
		!c.mode.Valid() || !c.boundary.Valid() {
		return errors.New("worker requires Temporal, identity, absolute store, mode, and boundary configuration")
	}
	return nil
}
