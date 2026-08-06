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

type managedProcess struct {
	command *exec.Cmd
	logFile *os.File
	mu      sync.Mutex
	waited  bool
}

func startWorkerProcess(
	options Options,
	runDirectory, address, taskQueue, storePath, barrierURL, workerID string,
) (*managedProcess, error) {
	logPath := filepath.Join(runDirectory, "workers", workerID+".log")
	if err := os.MkdirAll(filepath.Dir(logPath), 0o750); err != nil {
		return nil, fmt.Errorf("create Worker log directory: %w", err)
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create Worker log: %w", err)
	}
	command := exec.Command(
		options.WorkerBinary,
		"--address", address,
		"--namespace", "default",
		"--task-queue", taskQueue,
		"--store", storePath,
		"--agent-binary", options.AgentBinary,
		"--barrier-url", barrierURL,
		"--run-dir", filepath.Join(runDirectory, "sessions"),
		"--worker-id", workerID,
		"--agent-build", agentProtocolBuild,
	)
	command.Stdout = logFile
	command.Stderr = logFile
	if err := command.Start(); err != nil {
		_ = logFile.Close()
		return nil, fmt.Errorf("start %s: %w", workerID, err)
	}
	return &managedProcess{command: command, logFile: logFile}, nil
}

func (p *managedProcess) killAndWait() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.waited {
		return nil
	}
	if err := p.command.Process.Kill(); err != nil {
		return fmt.Errorf("kill process %d: %w", p.command.Process.Pid, err)
	}
	state, err := p.command.Process.Wait()
	p.waited = true
	closeErr := p.logFile.Close()
	if err != nil {
		return errors.Join(fmt.Errorf("wait for killed process: %w", err), closeErr)
	}
	waitStatus, ok := state.Sys().(syscall.WaitStatus)
	if !ok || !waitStatus.Signaled() || waitStatus.Signal() != syscall.SIGKILL {
		return errors.Join(fmt.Errorf("process %d status %v; want SIGKILL", state.Pid(), state), closeErr)
	}
	return closeErr
}

func (p *managedProcess) stop(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.waited {
		return nil
	}
	if err := p.command.Process.Signal(os.Interrupt); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("interrupt process %d: %w", p.command.Process.Pid, err)
	}
	waited := make(chan error, 1)
	go func() {
		waited <- p.command.Wait()
	}()
	select {
	case <-ctx.Done():
		_ = p.command.Process.Kill()
		<-waited
		p.waited = true
		_ = p.logFile.Close()
		return fmt.Errorf("stop process %d: %w", p.command.Process.Pid, ctx.Err())
	case err := <-waited:
		p.waited = true
		return errors.Join(err, p.logFile.Close())
	}
}
