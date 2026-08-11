package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sjarmak/temporal_projects/experiments/durable-vendor-sessions/claude-direct/internal/lab"
	"github.com/sjarmak/temporal_projects/internal/agentprocess"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

const defaultNamespace = "default"

type workerCLIConfig struct {
	TemporalAddress string
	Namespace       string
	TaskQueue       string
	ReadyFile       string
	Worker          lab.WorkerConfig
}

type readyRecord struct {
	WorkerID     string    `json:"worker_id"`
	PID          int       `json:"pid"`
	ProcessStart string    `json:"process_start"`
	ReadyAt      time.Time `json:"ready_at"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	config, err := parseConfig(os.Args[1:])
	if err == nil {
		err = run(ctx, config)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func parseConfig(args []string) (workerCLIConfig, error) {
	flags := flag.NewFlagSet("claude-direct-worker", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var config workerCLIConfig
	flags.StringVar(&config.TemporalAddress, "temporal-address", "", "Temporal frontend address")
	flags.StringVar(&config.Namespace, "namespace", defaultNamespace, "Temporal namespace")
	flags.StringVar(&config.TaskQueue, "task-queue", "", "Temporal task queue")
	flags.StringVar(&config.ReadyFile, "ready-file", "", "exclusive Worker readiness receipt")
	flags.StringVar(&config.Worker.WorkerID, "worker-id", "", "stable Worker identity")
	flags.StringVar(&config.Worker.Command.Binary, "claude-binary", "", "Claude CLI binary")
	flags.StringVar(&config.Worker.LauncherBinary, "launcher-binary", "", "pre-registration launcher binary")
	flags.Var((*faultBoundaryValue)(&config.Worker.FaultBoundary), "fault-boundary", "declared exact fault boundary")
	flags.StringVar(&config.Worker.Command.WorkDir, "fixture-dir", "", "isolated fixture workspace")
	flags.StringVar(&config.Worker.Command.Model, "model", "", "pinned Claude model alias or ID")
	flags.StringVar(&config.Worker.Command.MaxBudgetUSD, "max-budget-usd", "", "per-attempt spend ceiling")
	flags.IntVar(&config.Worker.Command.MaxTurns, "max-turns", 0, "per-attempt turn ceiling")
	flags.StringVar(&config.Worker.EffectBinary, "effect-binary", "", "controlled effect binary")
	flags.StringVar(&config.Worker.DestinationPath, "destination", "", "controlled destination database")
	flags.StringVar(&config.Worker.WorkspacePath, "workspace-effects", "", "workspace effect journal")
	flags.StringVar(&config.Worker.EffectPayload, "effect-payload", "controlled-edit", "controlled effect payload")
	flags.StringVar(&config.Worker.BarrierURL, "barrier-url", "", "exact barrier controller URL")
	flags.StringVar(&config.Worker.BarrierPoint, "barrier-point", "", "post-effect barrier point")
	flags.StringVar(&config.Worker.RunRoot, "run-root", "", "append-only attempt artifact root")
	flags.StringVar(&config.Worker.SupervisorURL, "supervisor-url", "", "loopback fenced turn supervisor URL")
	if err := flags.Parse(args); err != nil {
		return workerCLIConfig{}, fmt.Errorf("parse Worker flags: %w", err)
	}
	if flags.NArg() != 0 || config.TemporalAddress == "" || config.Namespace == "" ||
		config.TaskQueue == "" || config.ReadyFile == "" || !config.WorkerValid() {
		return workerCLIConfig{}, errors.New("all Claude direct Worker flags are required and positional arguments are not accepted")
	}
	return config, nil
}

func (c workerCLIConfig) WorkerValid() bool {
	return c.Worker.Command.Binary != "" && c.Worker.LauncherBinary != "" && c.Worker.FaultBoundary != "" &&
		c.Worker.Command.WorkDir != "" && c.Worker.Command.Model != "" &&
		c.Worker.Command.MaxBudgetUSD != "" && c.Worker.Command.MaxTurns > 0 && c.Worker.EffectBinary != "" &&
		c.Worker.DestinationPath != "" && c.Worker.WorkspacePath != "" && c.Worker.EffectPayload != "" &&
		c.Worker.BarrierURL != "" && c.Worker.BarrierPoint != "" && c.Worker.RunRoot != "" && c.Worker.WorkerID != ""
}

type faultBoundaryValue lab.FaultBoundary

func (v *faultBoundaryValue) String() string {
	return string(*v)
}

func (v *faultBoundaryValue) Set(value string) error {
	*v = faultBoundaryValue(value)
	return nil
}

func run(ctx context.Context, config workerCLIConfig) error {
	temporalClient, err := client.Dial(client.Options{
		HostPort: config.TemporalAddress, Namespace: config.Namespace, Identity: config.Worker.WorkerID,
	})
	if err != nil {
		return fmt.Errorf("connect to Temporal: %w", err)
	}
	defer temporalClient.Close()
	temporalWorker := worker.New(temporalClient, config.TaskQueue, worker.Options{Identity: config.Worker.WorkerID})
	if err := lab.RegisterWorker(temporalWorker, config.Worker); err != nil {
		return err
	}
	if err := temporalWorker.Start(); err != nil {
		return fmt.Errorf("start Temporal Worker: %w", err)
	}
	defer temporalWorker.Stop()
	if err := publishReady(config.ReadyFile, config.Worker.WorkerID); err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}

func publishReady(path, workerID string) error {
	pid := os.Getpid()
	startIdentity, err := agentprocess.ProcessStartIdentity(pid)
	if err != nil {
		return fmt.Errorf("identify Worker process: %w", err)
	}
	record := readyRecord{
		WorkerID: workerID, PID: pid, ProcessStart: startIdentity, ReadyAt: time.Now().UTC(),
	}
	encoded, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return fmt.Errorf("encode Worker readiness: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create Worker readiness: %w", err)
	}
	if _, err := file.Write(append(encoded, '\n')); err != nil {
		_ = file.Close()
		return fmt.Errorf("write Worker readiness: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync Worker readiness: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close Worker readiness: %w", err)
	}
	return nil
}
