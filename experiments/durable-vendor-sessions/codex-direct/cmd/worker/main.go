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

	"github.com/sjarmak/temporal_projects/experiments/durable-vendor-sessions/codex-direct/internal/lab"
	"github.com/sjarmak/temporal_projects/internal/agentprocess"
	"github.com/sjarmak/temporal_projects/internal/failureinject"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

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

type faultBoundaryValue lab.FaultBoundary

func (v *faultBoundaryValue) String() string { return string(*v) }
func (v *faultBoundaryValue) Set(value string) error {
	*v = faultBoundaryValue(value)
	return nil
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
	flags := flag.NewFlagSet("codex-direct-worker", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var config workerCLIConfig
	flags.StringVar(&config.TemporalAddress, "temporal-address", "", "Temporal frontend address")
	flags.StringVar(&config.Namespace, "namespace", "default", "Temporal namespace")
	flags.StringVar(&config.TaskQueue, "task-queue", "", "Temporal task queue")
	flags.StringVar(&config.ReadyFile, "ready-file", "", "exclusive Worker readiness receipt")
	flags.StringVar(&config.Worker.WorkerID, "worker-id", "", "stable Worker identity")
	flags.StringVar(&config.Worker.Command.Binary, "codex-binary", "", "pinned Codex CLI binary")
	flags.StringVar(&config.Worker.Command.CodexHome, "codex-home", "", "fixed authenticated Codex profile")
	flags.StringVar(&config.Worker.LauncherBinary, "launcher-binary", "", "pre-thread launcher binary")
	flags.StringVar(&config.Worker.EffectBinary, "effect-binary", "", "controlled effect binary")
	flags.StringVar(&config.Worker.Command.WorkDir, "fixture-dir", "", "isolated fixture workspace")
	flags.StringVar(&config.Worker.DestinationPath, "destination", "", "unsafe destination database")
	flags.StringVar(&config.Worker.WorkspacePath, "workspace-effects", "", "workspace effect journal")
	flags.StringVar(&config.Worker.RunRoot, "run-root", "", "append-only attempt root")
	flags.StringVar(&config.Worker.BarrierURL, "barrier-url", "", "exact barrier service")
	flags.StringVar(&config.Worker.BarrierDirectory, "barrier-directory", "", "sandbox-compatible exact effect barrier")
	flags.StringVar(&config.Worker.BarrierPoint, "barrier-point", "", "effect barrier point")
	flags.StringVar(&config.Worker.SupervisorURL, "supervisor-url", "", "fenced supervisor service")
	flags.StringVar(&config.Worker.Command.Model, "model", "", "pinned Codex model")
	flags.StringVar(&config.Worker.Command.ReasoningEffort, "reasoning-effort", "", "pinned reasoning effort")
	flags.StringVar(&config.Worker.Command.OutputSchema, "output-schema", "", "structured result schema")
	flags.StringVar(&config.Worker.Command.Sandbox, "sandbox", "workspace-write", "Codex sandbox")
	flags.StringVar(&config.Worker.EffectPayload, "effect-payload", "controlled-edit", "controlled payload")
	flags.Var((*faultBoundaryValue)(&config.Worker.FaultBoundary), "fault-boundary", "exact fault boundary")
	flags.BoolVar(&config.Worker.Hermetic, "hermetic", false, "use hermetic Codex registration gate")
	if err := flags.Parse(args); err != nil {
		return workerCLIConfig{}, err
	}
	credential, err := failureinject.ReadCredentialFromEnvironment()
	if err != nil {
		return workerCLIConfig{}, err
	}
	config.Worker.BarrierCredential = credential
	if flags.NArg() != 0 || config.TemporalAddress == "" || config.Namespace == "" || config.TaskQueue == "" ||
		config.ReadyFile == "" || !config.WorkerValid() {
		return workerCLIConfig{}, errors.New("all Codex Worker identities and paths are required")
	}
	return config, nil
}

func (c workerCLIConfig) WorkerValid() bool {
	return c.Worker.Command.Binary != "" && c.Worker.Command.CodexHome != "" &&
		c.Worker.Command.WorkDir != "" && c.Worker.Command.Model != "" &&
		c.Worker.Command.ReasoningEffort != "" && c.Worker.Command.OutputSchema != "" &&
		c.Worker.EffectBinary != "" && c.Worker.DestinationPath != "" && c.Worker.WorkspacePath != "" &&
		c.Worker.RunRoot != "" && c.Worker.BarrierURL != "" && c.Worker.BarrierPoint != "" &&
		c.Worker.WorkerID != "" && c.Worker.FaultBoundary != ""
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
	processStart, err := agentprocess.ProcessStartIdentity(pid)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	encodeErr := json.NewEncoder(file).Encode(readyRecord{
		WorkerID: workerID, PID: pid, ProcessStart: processStart, ReadyAt: time.Now().UTC(),
	})
	if encodeErr == nil {
		encodeErr = file.Sync()
	}
	return errors.Join(encodeErr, file.Close())
}
