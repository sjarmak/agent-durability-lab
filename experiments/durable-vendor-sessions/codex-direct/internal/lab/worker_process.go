package lab

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/sjarmak/temporal_projects/internal/agentprocess"
	"github.com/sjarmak/temporal_projects/internal/failureinject"
)

const codexDirectCoverageDirectoryEnvironment = "CODEX_DIRECT_GOCOVERDIR"

type workerProcessConfig struct {
	Binary, Directory, TemporalAddress, TaskQueue, WorkerID   string
	CodexBinary, CodexHome, LauncherBinary, EffectBinary      string
	FixtureDirectory, DestinationPath, WorkspacePath, RunRoot string
	BarrierURL, BarrierPoint, SupervisorURL                   string
	BarrierCredential                                         failureinject.Credential
	BarrierDirectory                                          string
	FaultBoundary                                             FaultBoundary
	Model, ReasoningEffort, OutputSchema                      string
	Hermetic                                                  bool
}

type managedWorker struct {
	command       *exec.Cmd
	logFile       *os.File
	startIdentity string
	waited        bool
}

func startWorkerProcess(config workerProcessConfig) (*managedWorker, error) {
	environment, err := workerProcessEnvironment(os.Environ())
	if err != nil {
		return nil, err
	}
	directory := filepath.Join(config.Directory, "workers")
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return nil, err
	}
	logFile, err := os.OpenFile(filepath.Join(directory, config.WorkerID+".log"),
		os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	command := workerCommand(config, filepath.Join(directory, config.WorkerID+".ready.json"))
	command.Stdout, command.Stderr, command.Env = logFile, logFile, environment
	credentialFile, err := addBarrierCredential(command, config.BarrierCredential)
	if err != nil {
		_ = logFile.Close()
		return nil, err
	}
	defer func() {
		if credentialFile != nil {
			_ = credentialFile.Close()
		}
	}()
	if err := command.Start(); err != nil {
		_ = logFile.Close()
		return nil, err
	}
	startIdentity, err := agentprocess.ProcessStartIdentity(command.Process.Pid)
	if err != nil {
		_ = command.Process.Kill()
		_, _ = command.Process.Wait()
		_ = logFile.Close()
		return nil, err
	}
	return &managedWorker{command: command, logFile: logFile, startIdentity: startIdentity}, nil
}

func workerProcessEnvironment(base []string) ([]string, error) {
	environment := append([]string(nil), base...)
	directory := os.Getenv(codexDirectCoverageDirectoryEnvironment)
	if directory == "" {
		return environment, nil
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("codex coverage destination is not a real directory")
	}
	return mergeEnvironment(environment, []string{"GOCOVERDIR=" + directory}), nil
}

func workerCommand(config workerProcessConfig, readyPath string) *exec.Cmd {
	args := []string{
		"--temporal-address", config.TemporalAddress, "--namespace", "default",
		"--task-queue", config.TaskQueue, "--worker-id", config.WorkerID, "--ready-file", readyPath,
		"--codex-binary", config.CodexBinary, "--codex-home", config.CodexHome,
		"--launcher-binary", config.LauncherBinary, "--effect-binary", config.EffectBinary,
		"--fixture-dir", config.FixtureDirectory, "--destination", config.DestinationPath,
		"--workspace-effects", config.WorkspacePath, "--run-root", config.RunRoot,
		"--barrier-url", config.BarrierURL, "--barrier-point", config.BarrierPoint,
		"--barrier-directory", config.BarrierDirectory,
		"--supervisor-url", config.SupervisorURL, "--fault-boundary", string(config.FaultBoundary),
		"--model", config.Model, "--reasoning-effort", config.ReasoningEffort,
		"--output-schema", config.OutputSchema, "--sandbox", "workspace-write",
	}
	if config.Hermetic {
		args = append(args, "--hermetic")
	}
	return exec.Command(config.Binary, args...)
}

func (w *managedWorker) killAndWait() (time.Time, error) {
	if w == nil || w.waited {
		return time.Time{}, nil
	}
	current, err := agentprocess.ProcessStartIdentity(w.command.Process.Pid)
	if err != nil {
		return time.Time{}, err
	}
	if current != w.startIdentity {
		return time.Time{}, errors.New("worker PID identity changed before fault injection")
	}
	requestedAt := time.Now().UTC()
	if err := w.command.Process.Kill(); err != nil {
		return requestedAt, err
	}
	state, waitErr := w.command.Process.Wait()
	w.waited = true
	closeErr := w.logFile.Close()
	status, ok := state.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		return requestedAt, errors.Join(fmt.Errorf("worker did not exit from SIGKILL: %v", waitErr), closeErr)
	}
	return requestedAt, closeErr
}

func (w *managedWorker) stop(ctx context.Context) error {
	if w == nil || w.waited {
		return nil
	}
	if err := w.command.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	waited := make(chan error, 1)
	go func() { waited <- w.command.Wait() }()
	select {
	case <-ctx.Done():
		_ = w.command.Process.Kill()
		<-waited
		w.waited = true
		return errors.Join(ctx.Err(), w.logFile.Close())
	case err := <-waited:
		w.waited = true
		return errors.Join(err, w.logFile.Close())
	}
}
