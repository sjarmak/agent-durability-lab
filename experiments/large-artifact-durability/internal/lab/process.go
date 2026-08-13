package lab

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/sjarmak/temporal_projects/internal/failureinject"
)

type workerProcessConfig struct {
	Binary       string
	RunDirectory string
	Address      string
	TaskQueue    string
	WorkerID     string
	BarrierURL   string
	SessionID    string
	StoreRoot    string
	ExternalRoot string
	CoverageRoot string
	Mode         Mode
	Boundary     Boundary
	Credential   failureinject.Credential
}

type managedWorker struct {
	command *exec.Cmd
	logFile *os.File
	mu      sync.Mutex
	waited  bool
}

func startWorkerProcess(config workerProcessConfig) (*managedWorker, error) {
	logPath := filepath.Join(config.RunDirectory, "workers", config.WorkerID+".log")
	if err := os.MkdirAll(filepath.Dir(logPath), 0o750); err != nil {
		return nil, fmt.Errorf("create Worker log directory: %w", err)
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create Worker log: %w", err)
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		_ = logFile.Close()
		return nil, fmt.Errorf("create Worker credential pipe: %w", err)
	}
	if err := config.Credential.Write(writer); err != nil {
		_ = reader.Close()
		_ = writer.Close()
		_ = logFile.Close()
		return nil, err
	}
	if err := writer.Close(); err != nil {
		_ = reader.Close()
		_ = logFile.Close()
		return nil, fmt.Errorf("close Worker credential writer: %w", err)
	}
	command := exec.Command(config.Binary,
		"--address", config.Address,
		"--namespace", "default",
		"--task-queue", config.TaskQueue,
		"--worker-id", config.WorkerID,
		"--barrier-url", config.BarrierURL,
		"--session-id", config.SessionID,
		"--store-root", config.StoreRoot,
		"--external-root", config.ExternalRoot,
		"--mode", string(config.Mode),
		"--boundary", string(config.Boundary),
	)
	command.Stdout = logFile
	command.Stderr = logFile
	command.ExtraFiles = []*os.File{reader}
	command.Env = workerEnvironment(config)
	if err := command.Start(); err != nil {
		_ = reader.Close()
		_ = logFile.Close()
		return nil, fmt.Errorf("start %s: %w", config.WorkerID, err)
	}
	if err := reader.Close(); err != nil {
		_ = command.Process.Kill()
		_, _ = command.Process.Wait()
		_ = logFile.Close()
		return nil, fmt.Errorf("close parent Worker credential reader: %w", err)
	}
	return &managedWorker{command: command, logFile: logFile}, nil
}

func workerEnvironment(config workerProcessConfig) []string {
	environment := []string{
		"PATH=/usr/bin:/bin",
		"TZ=UTC",
		failureinject.CredentialFDEnvironment + "=3",
	}
	if config.CoverageRoot != "" {
		environment = append(environment, "GOCOVERDIR="+config.CoverageRoot)
	}
	return environment
}

func (w *managedWorker) PID() int {
	return w.command.Process.Pid
}

func (w *managedWorker) killAndWait() (string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.waited {
		return "already-waited", nil
	}
	if err := w.command.Process.Kill(); err != nil {
		return "", fmt.Errorf("kill Worker %d: %w", w.PID(), err)
	}
	state, waitErr := w.command.Process.Wait()
	w.waited = true
	closeErr := w.logFile.Close()
	if waitErr != nil {
		return state.String(), errors.Join(fmt.Errorf("wait for killed Worker: %w", waitErr), closeErr)
	}
	status, ok := state.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		return state.String(), errors.Join(fmt.Errorf("worker exit = %v, want SIGKILL", state), closeErr)
	}
	return state.String(), closeErr
}

func (w *managedWorker) stop(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.waited {
		return nil
	}
	if err := w.command.Process.Signal(os.Interrupt); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("interrupt Worker %d: %w", w.PID(), err)
	}
	waited := make(chan error, 1)
	go func() { waited <- w.command.Wait() }()
	select {
	case waitErr := <-waited:
		w.waited = true
		return errors.Join(waitErr, w.logFile.Close())
	case <-ctx.Done():
		killErr := w.command.Process.Kill()
		waitErr := <-waited
		w.waited = true
		return errors.Join(ctx.Err(), killErr, waitErr, w.logFile.Close())
	}
}
