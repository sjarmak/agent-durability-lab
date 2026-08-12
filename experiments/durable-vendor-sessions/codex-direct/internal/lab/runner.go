package lab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/sjarmak/temporal_projects/internal/agentprocess"
)

type RunInvocationInput struct {
	Directory         string
	AttemptID         string
	ActorID           string
	ThreadReceiptPath string
	RegistrationGate  bool
	ProcessStartGate  bool
}

const (
	hermeticThreadGateFDEnvironment = "CODEX_HERMETIC_THREAD_GATE_FD"
	processStartGateFDEnvironment   = "CODEX_DIRECT_PROCESS_START_GATE_FD"
)

type ProcessRecord struct {
	AttemptID      string    `json:"attempt_id"`
	ActorID        string    `json:"actor_id"`
	Binary         string    `json:"binary"`
	Args           []string  `json:"args"`
	WorkDir        string    `json:"work_dir"`
	PID            int       `json:"pid"`
	StartIdentity  string    `json:"start_identity"`
	ProcessGroupID int       `json:"process_group_id"`
	Identity       string    `json:"identity"`
	ObservedAt     time.Time `json:"observed_at"`
	State          string    `json:"state"`
	Failure        string    `json:"failure,omitempty"`
}

type InvocationResult struct {
	Codex                CodexStreamResult
	Process              ProcessRecord
	StdoutPath           string
	StderrPath           string
	ProcessStartedPath   string
	ProcessCompletedPath string
}

func RunInvocation(ctx context.Context, invocation Invocation, input RunInvocationInput,
	hooks StreamHooks,
) (InvocationResult, error) {
	if err := ctx.Err(); err != nil {
		return InvocationResult{}, err
	}
	if err := validateRunInvocation(invocation, input); err != nil {
		return InvocationResult{}, err
	}
	if err := os.MkdirAll(input.Directory, 0o750); err != nil {
		return InvocationResult{}, fmt.Errorf("create Codex invocation directory: %w", err)
	}
	paths := invocationPaths(input)
	stdoutFile, stderrFile, err := createInvocationStreams(paths)
	if err != nil {
		return InvocationResult{}, err
	}
	command := exec.Command(invocation.Binary, invocation.Args...)
	command.Dir = invocation.WorkDir
	command.Env = mergeEnvironment(os.Environ(), invocation.Env)
	credentialFile, err := addBarrierCredential(command, invocation.BarrierCredential)
	if err != nil {
		_ = errors.Join(stdoutFile.Close(), stderrFile.Close())
		return InvocationResult{}, err
	}
	defer func() {
		if credentialFile != nil {
			_ = credentialFile.Close()
		}
	}()
	command.Stdin = strings.NewReader(invocation.Stdin)
	var threadGateReader, threadGateWriter *os.File
	if input.RegistrationGate {
		threadGateReader, threadGateWriter, err = addChildGate(command, hermeticThreadGateFDEnvironment)
		if err != nil {
			_ = errors.Join(stdoutFile.Close(), stderrFile.Close())
			return InvocationResult{}, fmt.Errorf("create Codex thread registration gate: %w", err)
		}
	}
	var processGateReader, processGateWriter *os.File
	if input.ProcessStartGate {
		processGateReader, processGateWriter, err = addChildGate(command, processStartGateFDEnvironment)
		if err != nil {
			closeGates(threadGateReader, threadGateWriter)
			_ = errors.Join(stdoutFile.Close(), stderrFile.Close())
			return InvocationResult{}, fmt.Errorf("create Codex process receipt gate: %w", err)
		}
	}
	stdoutPipe, err := command.StdoutPipe()
	if err != nil {
		closeGates(threadGateReader, threadGateWriter, processGateReader, processGateWriter)
		_ = errors.Join(stdoutFile.Close(), stderrFile.Close())
		return InvocationResult{}, fmt.Errorf("open Codex stdout pipe: %w", err)
	}
	command.Stderr = stderrFile
	configureProcessGroup(command)
	if err := command.Start(); err != nil {
		closeGates(threadGateReader, threadGateWriter, processGateReader, processGateWriter)
		_ = errors.Join(stdoutFile.Close(), stderrFile.Close())
		return InvocationResult{}, fmt.Errorf("start Codex process: %w", err)
	}
	if credentialFile != nil {
		_ = credentialFile.Close()
		credentialFile = nil
	}
	closeGates(threadGateReader, processGateReader)
	threadGateReader, processGateReader = nil, nil
	process, err := observeProcess(input, invocation, command.Process.Pid, "running", "")
	if err != nil {
		closeGates(threadGateWriter, processGateWriter)
		stopStartedProcess(command)
		_ = errors.Join(stdoutFile.Close(), stderrFile.Close())
		return InvocationResult{}, err
	}
	if err := writeJSONExclusive(paths.started, process); err != nil {
		closeGates(threadGateWriter, processGateWriter)
		stopStartedProcess(command)
		_ = errors.Join(stdoutFile.Close(), stderrFile.Close())
		return InvocationResult{}, err
	}
	if processGateWriter != nil {
		if _, err := processGateWriter.Write([]byte{1}); err != nil {
			closeGates(threadGateWriter, processGateWriter)
			stopStartedProcess(command)
			_ = errors.Join(stdoutFile.Close(), stderrFile.Close())
			return InvocationResult{}, fmt.Errorf("release Codex process receipt gate: %w", err)
		}
		if err := processGateWriter.Close(); err != nil {
			closeGates(threadGateWriter)
			stopStartedProcess(command)
			_ = errors.Join(stdoutFile.Close(), stderrFile.Close())
			return InvocationResult{}, fmt.Errorf("close Codex process receipt gate: %w", err)
		}
		processGateWriter = nil
	}
	result := InvocationResult{
		Process: process, StdoutPath: paths.stdout, StderrPath: paths.stderr,
		ProcessStartedPath: paths.started, ProcessCompletedPath: paths.completed,
	}
	if hooks.ProcessStarted != nil {
		if hookErr := hooks.ProcessStarted(process); hookErr != nil {
			closeGates(threadGateWriter)
			stopStartedProcess(command)
			closeErr := errors.Join(stdoutFile.Close(), stderrFile.Close())
			recordErr := recordProcessCompletion(paths.completed, process, hookErr)
			return result, errors.Join(fmt.Errorf("observe Codex process start: %w", hookErr), closeErr, recordErr)
		}
	}
	streamHooks := hooks
	streamHooks.ThreadStarted = func(threadID string) error {
		receipt := ThreadReceipt{
			ThreadID: threadID, PhysicalAttemptID: input.AttemptID, ActorID: input.ActorID,
			PID: process.PID, ProcessStart: process.StartIdentity,
			ProcessIdentity: process.Identity, ObservedAt: time.Now().UTC(),
		}
		if err := writeJSONExclusive(input.ThreadReceiptPath, receipt); err != nil {
			return fmt.Errorf("record durable Codex thread registration: %w", err)
		}
		if hooks.ThreadStarted != nil {
			if hookErr := hooks.ThreadStarted(threadID); hookErr != nil {
				return hookErr
			}
		}
		if threadGateWriter != nil {
			if _, writeErr := threadGateWriter.Write([]byte{1}); writeErr != nil {
				return fmt.Errorf("release Codex thread registration gate: %w", writeErr)
			}
			if closeErr := threadGateWriter.Close(); closeErr != nil {
				return fmt.Errorf("close Codex thread registration gate: %w", closeErr)
			}
			threadGateWriter = nil
		}
		return nil
	}
	result.Codex, err = ParseCodexStream(io.TeeReader(stdoutPipe, stdoutFile), streamHooks)
	if err != nil {
		closeGates(threadGateWriter)
		stopStartedProcess(command)
	} else {
		err = command.Wait()
	}
	closeErr := errors.Join(stdoutFile.Close(), stderrFile.Close())
	recordErr := recordProcessCompletion(paths.completed, process, err)
	if finalErr := errors.Join(err, closeErr, recordErr); finalErr != nil {
		return result, fmt.Errorf("complete Codex invocation: %w", finalErr)
	}
	return result, nil
}

func recordProcessCompletion(path string, process ProcessRecord, processErr error) error {
	completed := process
	completed.ObservedAt = time.Now().UTC()
	completed.State = "exited"
	if processErr != nil {
		completed.Failure = processErr.Error()
	}
	return writeJSONExclusive(path, completed)
}

func addChildGate(command *exec.Cmd, environment string) (*os.File, *os.File, error) {
	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, nil, err
	}
	childFD := 3 + len(command.ExtraFiles)
	command.Env = mergeEnvironment(command.Env, []string{fmt.Sprintf("%s=%d", environment, childFD)})
	command.ExtraFiles = append(command.ExtraFiles, reader)
	return reader, writer, nil
}

func closeGates(files ...*os.File) {
	for _, file := range files {
		if file != nil {
			_ = file.Close()
		}
	}
}

func createInvocationStreams(paths runPaths) (*os.File, *os.File, error) {
	stdout, err := os.OpenFile(paths.stdout, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, nil, fmt.Errorf("create Codex stdout: %w", err)
	}
	stderr, err := os.OpenFile(paths.stderr, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		_ = stdout.Close()
		return nil, nil, fmt.Errorf("create Codex stderr: %w", err)
	}
	return stdout, stderr, nil
}

func observeProcess(input RunInvocationInput, invocation Invocation, pid int, state, failure string) (ProcessRecord, error) {
	startIdentity, err := agentprocess.ProcessStartIdentity(pid)
	if err != nil {
		return ProcessRecord{}, fmt.Errorf("identify Codex process: %w", err)
	}
	processGroupID, err := agentprocess.ProcessGroupID(pid)
	if err != nil {
		return ProcessRecord{}, fmt.Errorf("identify Codex process group: %w", err)
	}
	return ProcessRecord{
		AttemptID: input.AttemptID, ActorID: input.ActorID,
		Binary: invocation.Binary, Args: append([]string(nil), invocation.Args...), WorkDir: invocation.WorkDir,
		PID: pid, StartIdentity: startIdentity, ProcessGroupID: processGroupID,
		Identity: fmt.Sprintf("pid:%d:start:%s", pid, startIdentity), ObservedAt: time.Now().UTC(),
		State: state, Failure: failure,
	}, nil
}

func validateRunInvocation(invocation Invocation, input RunInvocationInput) error {
	if invocation.Binary == "" || invocation.WorkDir == "" || invocation.Stdin == "" ||
		input.Directory == "" || input.AttemptID == "" || input.AttemptID != filepath.Base(input.AttemptID) ||
		input.ActorID == "" || input.ThreadReceiptPath == "" ||
		filepath.Dir(input.ThreadReceiptPath) != filepath.Clean(input.Directory) {
		return errors.New("codex invocation requires complete command, attempt, actor, and directory identities")
	}
	return nil
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

func stopStartedProcess(command *exec.Cmd) {
	_ = command.Process.Kill()
	_, _ = command.Process.Wait()
}

func mergeEnvironment(base, overrides []string) []string {
	result := append([]string(nil), base...)
	positions := make(map[string]int, len(result))
	for index, item := range result {
		if key, _, ok := strings.Cut(item, "="); ok {
			positions[key] = index
		}
	}
	for _, item := range overrides {
		key, _, ok := strings.Cut(item, "=")
		if !ok || key == "" {
			continue
		}
		if index, exists := positions[key]; exists {
			result[index] = item
			continue
		}
		positions[key] = len(result)
		result = append(result, item)
	}
	return result
}

func writeJSONExclusive(path string, value any) (returnErr error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create evidence file %q: %w", path, err)
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	encoder := json.NewEncoder(file)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return fmt.Errorf("encode evidence file %q: %w", path, err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync evidence file %q: %w", path, err)
	}
	return nil
}
