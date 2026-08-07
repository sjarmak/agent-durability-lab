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
)

type managedWorker struct {
	command *exec.Cmd
	logFile *os.File
	mu      sync.Mutex
	waited  bool
}

func startWorkerProcess(
	workerBinary, runDirectory, address, taskQueue, workerID string,
) (*managedWorker, error) {
	logPath := filepath.Join(runDirectory, "workers", workerID+".log")
	if err := os.MkdirAll(filepath.Dir(logPath), 0o750); err != nil {
		return nil, fmt.Errorf("create Worker log directory: %w", err)
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create Worker log: %w", err)
	}
	command := exec.Command(
		workerBinary,
		"--address", address,
		"--namespace", "default",
		"--task-queue", taskQueue,
		"--worker-id", workerID,
	)
	command.Stdout = logFile
	command.Stderr = logFile
	if err := command.Start(); err != nil {
		_ = logFile.Close()
		return nil, fmt.Errorf("start %s: %w", workerID, err)
	}
	return &managedWorker{command: command, logFile: logFile}, nil
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
		return "", fmt.Errorf("kill process %d: %w", w.command.Process.Pid, err)
	}
	state, waitErr := w.command.Process.Wait()
	w.waited = true
	closeErr := w.logFile.Close()
	if waitErr != nil {
		return state.String(), errors.Join(fmt.Errorf("wait for killed process: %w", waitErr), closeErr)
	}
	waitStatus, ok := state.Sys().(syscall.WaitStatus)
	if !ok || !waitStatus.Signaled() || waitStatus.Signal() != syscall.SIGKILL {
		return state.String(), errors.Join(fmt.Errorf("process %d status %v; want SIGKILL", state.Pid(), state), closeErr)
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
		return fmt.Errorf("interrupt process %d: %w", w.command.Process.Pid, err)
	}
	waited := make(chan error, 1)
	go func() { waited <- w.command.Wait() }()
	select {
	case <-ctx.Done():
		_ = w.command.Process.Kill()
		<-waited
		w.waited = true
		_ = w.logFile.Close()
		return fmt.Errorf("stop process %d: %w", w.command.Process.Pid, ctx.Err())
	case err := <-waited:
		w.waited = true
		return errors.Join(err, w.logFile.Close())
	}
}
