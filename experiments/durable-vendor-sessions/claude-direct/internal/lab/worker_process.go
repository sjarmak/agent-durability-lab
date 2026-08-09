package lab

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/sjarmak/temporal_projects/internal/agentprocess"
)

type workerProcessConfig struct {
	Binary           string
	Directory        string
	TemporalAddress  string
	TaskQueue        string
	WorkerID         string
	ClaudeBinary     string
	LauncherBinary   string
	FaultBoundary    FaultBoundary
	EffectBinary     string
	FixtureDirectory string
	DestinationPath  string
	WorkspacePath    string
	BarrierURL       string
	BarrierPoint     string
	RunRoot          string
	Model            string
	MaxBudgetUSD     string
	MaxTurns         int
}

type managedWorker struct {
	command       *exec.Cmd
	logFile       *os.File
	startIdentity string
	waited        bool
}

func (w *managedWorker) processIdentity() string {
	if w == nil || w.command == nil || w.command.Process == nil {
		return ""
	}
	return fmt.Sprintf("pid:%d:start:%s", w.command.Process.Pid, w.startIdentity)
}

func startWorkerProcess(config workerProcessConfig) (*managedWorker, error) {
	workerDirectory := filepath.Join(config.Directory, "workers")
	if err := os.MkdirAll(workerDirectory, 0o750); err != nil {
		return nil, fmt.Errorf("create Worker artifact directory: %w", err)
	}
	logPath := filepath.Join(workerDirectory, config.WorkerID+".log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create Worker log: %w", err)
	}
	readyPath := filepath.Join(workerDirectory, config.WorkerID+".ready.json")
	command := workerCommand(config, readyPath)
	command.Stdout = logFile
	command.Stderr = logFile
	command.Env = os.Environ()
	if err := command.Start(); err != nil {
		_ = logFile.Close()
		return nil, fmt.Errorf("start %s: %w", config.WorkerID, err)
	}
	startIdentity, err := agentprocess.ProcessStartIdentity(command.Process.Pid)
	if err != nil {
		_ = command.Process.Kill()
		_, _ = command.Process.Wait()
		_ = logFile.Close()
		return nil, fmt.Errorf("identify %s: %w", config.WorkerID, err)
	}
	return &managedWorker{command: command, logFile: logFile, startIdentity: startIdentity}, nil
}

func workerCommand(config workerProcessConfig, readyPath string) *exec.Cmd {
	return exec.Command(
		config.Binary,
		"--temporal-address", config.TemporalAddress,
		"--namespace", "default",
		"--task-queue", config.TaskQueue,
		"--worker-id", config.WorkerID,
		"--claude-binary", config.ClaudeBinary,
		"--launcher-binary", config.LauncherBinary,
		"--fault-boundary", string(config.FaultBoundary),
		"--effect-binary", config.EffectBinary,
		"--fixture-dir", config.FixtureDirectory,
		"--destination", config.DestinationPath,
		"--workspace-effects", config.WorkspacePath,
		"--effect-payload", "controlled-edit",
		"--barrier-url", config.BarrierURL,
		"--barrier-point", config.BarrierPoint,
		"--run-root", config.RunRoot,
		"--ready-file", readyPath,
		"--model", config.Model,
		"--max-budget-usd", config.MaxBudgetUSD,
		"--max-turns", strconv.Itoa(config.MaxTurns),
	)
}

func (w *managedWorker) killAndWait() (string, time.Time, error) {
	if w == nil || w.waited {
		return "already-waited", time.Time{}, nil
	}
	currentIdentity, err := agentprocess.ProcessStartIdentity(w.command.Process.Pid)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("revalidate Worker process: %w", err)
	}
	if currentIdentity != w.startIdentity {
		return "", time.Time{}, errors.New("worker PID was reused before fault injection")
	}
	requestedAt := time.Now().UTC()
	if err := w.command.Process.Kill(); err != nil {
		return "", requestedAt, fmt.Errorf("kill Worker process: %w", err)
	}
	state, waitErr := w.command.Process.Wait()
	w.waited = true
	closeErr := w.logFile.Close()
	if waitErr != nil {
		return "", requestedAt, errors.Join(waitErr, closeErr)
	}
	waitStatus, ok := state.Sys().(syscall.WaitStatus)
	if !ok || !waitStatus.Signaled() || waitStatus.Signal() != syscall.SIGKILL {
		return state.String(), requestedAt, errors.Join(errors.New("worker did not exit from SIGKILL"), closeErr)
	}
	return state.String(), requestedAt, closeErr
}

func (w *managedWorker) stop(ctx context.Context) error {
	if w == nil || w.waited {
		return nil
	}
	if err := w.command.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("signal Worker stop: %w", err)
	}
	waited := make(chan error, 1)
	go func() { waited <- w.command.Wait() }()
	select {
	case <-ctx.Done():
		_ = w.command.Process.Kill()
		<-waited
		w.waited = true
		_ = w.logFile.Close()
		return fmt.Errorf("stop Worker: %w", ctx.Err())
	case err := <-waited:
		w.waited = true
		return errors.Join(err, w.logFile.Close())
	}
}
