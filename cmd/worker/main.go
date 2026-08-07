package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/sjarmak/temporal_projects/internal/temporalagent"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "worker: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var config workerConfig
	flag.StringVar(&config.address, "address", "127.0.0.1:7233", "Temporal frontend address")
	flag.StringVar(&config.namespace, "namespace", "default", "Temporal namespace")
	flag.StringVar(&config.taskQueue, "task-queue", "", "Temporal task queue")
	flag.StringVar(&config.storePath, "store", "", "application work store path")
	flag.StringVar(&config.agentBinary, "agent-binary", "", "agent simulator binary")
	flag.StringVar(&config.barrierURL, "barrier-url", "", "failure-injection coordinator URL")
	flag.StringVar(&config.runDirectory, "run-dir", "", "agent logs and private launch requests")
	flag.StringVar(&config.workerID, "worker-id", "", "stable identity for this Worker process")
	flag.StringVar(&config.agentBuild, "agent-build", "", "agent session protocol/build identity")
	flag.Parse()
	if err := config.validate(); err != nil {
		return err
	}

	temporalClient, err := client.Dial(client.Options{
		HostPort: config.address, Namespace: config.namespace, Identity: config.workerID,
	})
	if err != nil {
		return fmt.Errorf("connect to Temporal: %w", err)
	}
	defer temporalClient.Close()

	temporalWorker := worker.New(temporalClient, config.taskQueue, worker.Options{})
	temporalWorker.RegisterWorkflowWithOptions(
		temporalagent.WorkerDeathWorkflow,
		workflow.RegisterOptions{Name: temporalagent.WorkflowName},
	)
	activities := temporalagent.Activities{
		StorePath: config.storePath, AgentBinary: config.agentBinary,
		BarrierURL: config.barrierURL, RunDirectory: config.runDirectory,
		WorkerID: config.workerID, AgentBuild: config.agentBuild,
	}
	temporalWorker.RegisterActivityWithOptions(
		activities.RunAgent,
		activity.RegisterOptions{Name: temporalagent.ActivityName},
	)
	temporalWorker.RegisterActivityWithOptions(
		activities.CancelAgent,
		activity.RegisterOptions{Name: temporalagent.CancelActivityName},
	)
	if err := temporalWorker.Run(worker.InterruptCh()); err != nil {
		return fmt.Errorf("run Temporal Worker: %w", err)
	}
	return nil
}

type workerConfig struct {
	address      string
	namespace    string
	taskQueue    string
	storePath    string
	agentBinary  string
	barrierURL   string
	runDirectory string
	workerID     string
	agentBuild   string
}

func (c workerConfig) validate() error {
	if c.address == "" || c.namespace == "" || c.taskQueue == "" || c.storePath == "" ||
		c.agentBinary == "" || c.barrierURL == "" || c.runDirectory == "" || c.workerID == "" || c.agentBuild == "" {
		return errors.New("all worker flags are required")
	}
	return nil
}
