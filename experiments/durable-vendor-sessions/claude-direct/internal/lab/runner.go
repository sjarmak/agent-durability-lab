package lab

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/sjarmak/temporal_projects/internal/agentprocess"
)

type RunInvocationInput struct {
	Directory string
	AttemptID string
	ActorID   string
}

type ProcessRecord struct {
	AttemptID     string    `json:"attempt_id"`
	ActorID       string    `json:"actor_id"`
	PID           int       `json:"pid"`
	StartIdentity string    `json:"start_identity"`
	Identity      string    `json:"identity"`
	ObservedAt    time.Time `json:"observed_at"`
	State         string    `json:"state"`
	Failure       string    `json:"failure,omitempty"`
}

type InvocationResult struct {
	Claude               ClaudeStreamResult
	Process              ProcessRecord
	StdoutPath           string
	StderrPath           string
	ProcessStartedPath   string
	ProcessCompletedPath string
}

func RunInvocation(ctx context.Context, invocation Invocation, input RunInvocationInput) (InvocationResult, error) {
	if err := ctx.Err(); err != nil {
		return InvocationResult{}, err
	}
	if err := validateRunInvocation(invocation, input); err != nil {
		return InvocationResult{}, err
	}
	if err := os.MkdirAll(input.Directory, 0o750); err != nil {
		return InvocationResult{}, fmt.Errorf("create invocation directory: %w", err)
	}
	running, err := startInvocation(invocation, input)
	if err != nil {
		return InvocationResult{}, err
	}
	return running.await()
}

type runningInvocation struct {
	command *exec.Cmd
	process ProcessRecord
	paths   runPaths
}

func startInvocation(invocation Invocation, input RunInvocationInput) (runningInvocation, error) {
	paths := invocationPaths(input)
	stdout, stderr, err := createInvocationStreams(paths)
	if err != nil {
		return runningInvocation{}, err
	}
	command := exec.Command(invocation.Binary, invocation.Args...)
	command.Dir = invocation.WorkDir
	command.Env = append(os.Environ(), invocation.Env...)
	command.Stdin = strings.NewReader(invocation.Stdin)
	command.Stdout = stdout
	command.Stderr = stderr
	if err := command.Start(); err != nil {
		_ = stdout.Close()
		_ = stderr.Close()
		return runningInvocation{}, fmt.Errorf("start Claude process: %w", err)
	}
	if err := errors.Join(stdout.Close(), stderr.Close()); err != nil {
		stopStartedProcess(command)
		return runningInvocation{}, fmt.Errorf("close parent stream handles: %w", err)
	}
	process, err := observeProcess(input, command.Process.Pid, "running", "")
	if err != nil {
		stopStartedProcess(command)
		return runningInvocation{}, err
	}
	if err := writeJSONExclusive(paths.started, process); err != nil {
		stopStartedProcess(command)
		return runningInvocation{}, err
	}
	return runningInvocation{command: command, process: process, paths: paths}, nil
}

func (r runningInvocation) await() (InvocationResult, error) {
	// Intentionally do not bind the child lifetime to the Activity context. This
	// is the unsafe control: Worker loss can leave the direct CLI process alive.
	waitErr := r.command.Wait()
	failure := ""
	if waitErr != nil {
		failure = waitErr.Error()
	}
	completed := r.process
	completed.ObservedAt = time.Now().UTC()
	completed.State = "exited"
	completed.Failure = failure
	if err := writeJSONExclusive(r.paths.completed, completed); err != nil {
		return InvocationResult{}, err
	}
	result := InvocationResult{
		Process: r.process, StdoutPath: r.paths.stdout, StderrPath: r.paths.stderr,
		ProcessStartedPath: r.paths.started, ProcessCompletedPath: r.paths.completed,
	}
	if waitErr != nil {
		return result, fmt.Errorf("wait for Claude process: %w", waitErr)
	}
	stdoutReader, err := os.Open(r.paths.stdout)
	if err != nil {
		return result, fmt.Errorf("open Claude stdout: %w", err)
	}
	defer func() { _ = stdoutReader.Close() }()
	result.Claude, err = ParseClaudeStream(stdoutReader)
	if err != nil {
		return result, err
	}
	return result, nil
}

func createInvocationStreams(paths runPaths) (*os.File, *os.File, error) {
	stdout, err := os.OpenFile(paths.stdout, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, nil, fmt.Errorf("create Claude stdout: %w", err)
	}
	stderr, err := os.OpenFile(paths.stderr, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		_ = stdout.Close()
		return nil, nil, fmt.Errorf("create Claude stderr: %w", err)
	}
	return stdout, stderr, nil
}

func stopStartedProcess(command *exec.Cmd) {
	_ = command.Process.Kill()
	_, _ = command.Process.Wait()
}

type runPaths struct {
	stdout, stderr, started, completed string
}

func invocationPaths(input RunInvocationInput) runPaths {
	prefix := filepath.Join(input.Directory, input.AttemptID)
	return runPaths{
		stdout: prefix + ".stdout.jsonl", stderr: prefix + ".stderr",
		started: prefix + ".process-started.json", completed: prefix + ".process-completed.json",
	}
}

func observeProcess(input RunInvocationInput, pid int, state, failure string) (ProcessRecord, error) {
	startIdentity, err := agentprocess.ProcessStartIdentity(pid)
	if err != nil {
		return ProcessRecord{}, fmt.Errorf("identify Claude process: %w", err)
	}
	return ProcessRecord{
		AttemptID: input.AttemptID, ActorID: input.ActorID, PID: pid, StartIdentity: startIdentity,
		Identity: fmt.Sprintf("pid:%d:start:%s", pid, startIdentity), ObservedAt: time.Now().UTC(),
		State: state, Failure: failure,
	}, nil
}

func validateRunInvocation(invocation Invocation, input RunInvocationInput) error {
	if invocation.Binary == "" || invocation.WorkDir == "" || invocation.Stdin == "" ||
		input.Directory == "" || input.AttemptID == "" || input.AttemptID != filepath.Base(input.AttemptID) ||
		input.ActorID == "" {
		return errors.New("direct invocation requires complete command, attempt, actor, and directory identities")
	}
	return nil
}
