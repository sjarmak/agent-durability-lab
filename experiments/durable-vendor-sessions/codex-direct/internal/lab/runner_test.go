package lab

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/internal/failureinject"
)

func TestRunInvocationStreamsExactThreadBoundaryBeforeCompletion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	hermetic := buildHermeticCodex(t)
	effect := buildCodexTestBinary(t, "controlled-effect")
	directory := t.TempDir()
	requestPath := filepath.Join(directory, "request.json")
	threadReceiptPath := filepath.Join(directory, "thread-receipt.json")
	destinationPath := filepath.Join(directory, "destination.db")
	workspacePath := filepath.Join(directory, "effects.jsonl")
	coordinator := failureinject.NewCoordinator()
	server := httptest.NewServer(coordinator.Handler())
	t.Cleanup(server.Close)
	writeTestJSON(t, requestPath, ControlledEffectInput{
		DestinationPath: destinationPath, WorkspacePath: workspacePath, Payload: "controlled-edit",
		ThreadReceiptPath: threadReceiptPath,
		BarrierURL:        server.URL, BarrierPoint: committedEffectBarrier,
		LogicalSessionID: "session-1", LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
		PhysicalAttemptID: "attempt-1", ActorID: "worker-1",
	})
	command := CodexCommand{
		Binary: hermetic, WorkDir: directory, CodexHome: filepath.Join(directory, "codex-home"),
		Model: "gpt-5.6-sol", ReasoningEffort: "low",
		OutputSchema: filepath.Join(directory, "result.schema.json"), Sandbox: "workspace-write",
	}
	invocation, err := command.InitialInvocation(ControlledEffectPrompt(effect + " --request " + requestPath))
	if err != nil {
		t.Fatalf("build invocation: %v", err)
	}
	invocation.Env = append(invocation.Env, "CODEX_HERMETIC_THREAD_ID="+testThreadID)
	threadObserved := make(chan string, 1)
	release := make(chan struct{})
	type outcome struct {
		result InvocationResult
		err    error
	}
	finished := make(chan outcome, 1)
	go func() {
		result, runErr := RunInvocation(ctx, invocation, RunInvocationInput{
			Directory: directory, AttemptID: "attempt-1", ActorID: "worker-1", RegistrationGate: true,
			ThreadReceiptPath: threadReceiptPath,
		}, StreamHooks{ExpectedCommand: effect + " --request " + requestPath, ThreadStarted: func(threadID string) error {
			receipt, readErr := ReadThreadReceipt(threadReceiptPath)
			if readErr != nil {
				return readErr
			}
			if receipt.ThreadID != threadID || receipt.PhysicalAttemptID != "attempt-1" || receipt.ActorID != "worker-1" {
				return fmt.Errorf("thread receipt mismatch: %+v", receipt)
			}
			if receipt.PID <= 0 || receipt.ProcessStart == "" {
				return fmt.Errorf("thread receipt lacks process identity: %+v", receipt)
			}
			threadObserved <- threadID
			<-release
			return nil
		}})
		finished <- outcome{result: result, err: runErr}
	}()
	if got := <-threadObserved; got != testThreadID {
		t.Fatalf("thread boundary = %q, want %q", got, testThreadID)
	}
	if _, err := os.Stat(destinationPath); !os.IsNotExist(err) {
		t.Fatalf("destination exists before durable thread release: %v", err)
	}
	select {
	case result := <-finished:
		t.Fatalf("invocation completed before exact thread release: %+v", result)
	default:
	}
	close(release)
	if _, err := coordinator.WaitForArrivals(ctx, committedEffectBarrier, 1); err != nil {
		t.Fatalf("wait for controlled effect: %v", err)
	}
	if err := coordinator.Release(committedEffectBarrier); err != nil {
		t.Fatalf("release controlled effect: %v", err)
	}
	run := <-finished
	if run.err != nil {
		t.Fatalf("run invocation: %v", run.err)
	}
	destination, err := ReadDestination(context.Background(), destinationPath)
	if err != nil || len(destination.Attempts) != 1 {
		t.Fatalf("destination after release: %+v err=%v", destination, err)
	}
	if run.result.Codex.ThreadID != testThreadID || run.result.Codex.Result != "EFFECT_COMPLETE" {
		t.Fatalf("Codex result = %+v", run.result.Codex)
	}
	if run.result.Process.PID <= 0 || run.result.Process.StartIdentity == "" || run.result.Process.Identity == "" {
		t.Fatalf("process evidence = %+v", run.result.Process)
	}
	for _, path := range []string{
		run.result.StdoutPath, run.result.StderrPath,
		run.result.ProcessStartedPath, run.result.ProcessCompletedPath, threadReceiptPath,
	} {
		if info, statErr := os.Stat(path); statErr != nil || !info.Mode().IsRegular() {
			t.Fatalf("artifact %q: info=%v err=%v", path, info, statErr)
		}
	}
}

func TestRunInvocationRecordsProcessBeforePreThreadBarrier(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	launcher := buildCodexTestBinary(t, "codex-launcher")
	directory := t.TempDir()
	credential, err := failureinject.NewCredential()
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := failureinject.NewAuthenticatedCoordinator(credential, failureinject.Expectation{
		Point: "pre-thread", SessionID: "session-1", Generation: 1, ActorID: "worker-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(coordinator.Handler())
	t.Cleanup(server.Close)
	invocation := Invocation{
		Binary: launcher, Args: []string{"--version"}, WorkDir: directory, Stdin: "input\n",
		Env: []string{
			"CODEX_DIRECT_REAL_BINARY=/bin/true",
			"CODEX_DIRECT_PRE_THREAD_BARRIER_URL=" + server.URL,
			"CODEX_DIRECT_PRE_THREAD_BARRIER_POINT=pre-thread",
			"CODEX_DIRECT_PHYSICAL_ATTEMPT_ID=attempt-1",
			"CODEX_DIRECT_LOGICAL_SESSION_ID=session-1",
			"CODEX_DIRECT_GENERATION=1",
			"CODEX_DIRECT_ACTOR_ID=worker-1",
		},
		BarrierCredential: credential,
	}
	type outcome struct {
		result InvocationResult
		err    error
	}
	finished := make(chan outcome, 1)
	go func() {
		result, err := RunInvocation(ctx, invocation, RunInvocationInput{
			Directory: directory, AttemptID: "attempt-1", ActorID: "worker-1",
			ThreadReceiptPath: filepath.Join(directory, "thread-receipt.json"), ProcessStartGate: true,
		}, StreamHooks{})
		finished <- outcome{result: result, err: err}
	}()
	arrivals, err := coordinator.WaitForArrivals(ctx, "pre-thread", 1)
	if err != nil {
		t.Fatalf("wait for launcher boundary: %v", err)
	}
	started, err := readStrictJSON[ProcessRecord](filepath.Join(directory, "attempt-1.process-started.json"))
	if err != nil {
		t.Fatalf("read process receipt at barrier: %v", err)
	}
	if len(arrivals) != 1 || started.PID != arrivals[0].PID || started.StartIdentity != arrivals[0].ProcessStart {
		t.Fatalf("process receipt %+v differs from barrier %+v", started, arrivals)
	}
	if err := coordinator.Release("pre-thread"); err != nil {
		t.Fatal(err)
	}
	if result := <-finished; result.err == nil {
		t.Fatalf("non-Codex child unexpectedly completed: %+v", result.result)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		data, readErr := os.ReadFile(filepath.Join(directory, entry.Name()))
		if readErr != nil {
			t.Fatal(readErr)
		}
		var credentialBytes bytes.Buffer
		if err := credential.Write(&credentialBytes); err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(data, credentialBytes.Bytes()) {
			t.Fatalf("portable process artifact %q contains raw barrier credential", entry.Name())
		}
	}
}

func TestRunInvocationFailsClosedAcrossSetupAndStreamBoundaries(t *testing.T) {
	hermetic := buildHermeticCodex(t)
	sentinel := errors.New("reject observation")
	t.Run("canceled-context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := RunInvocation(ctx, Invocation{}, RunInvocationInput{}, StreamHooks{}); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled invocation = %v", err)
		}
	})
	t.Run("invalid-input", func(t *testing.T) {
		if _, err := RunInvocation(context.Background(), Invocation{}, RunInvocationInput{}, StreamHooks{}); err == nil {
			t.Fatal("incomplete invocation was accepted")
		}
	})
	t.Run("directory-is-file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "not-a-directory")
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		input := RunInvocationInput{Directory: path, AttemptID: "attempt-1", ActorID: "actor-1",
			ThreadReceiptPath: filepath.Join(path, "thread-receipt.json")}
		invocation := Invocation{Binary: hermetic, Args: []string{"--version"}, WorkDir: filepath.Dir(path), Stdin: "input"}
		if _, err := RunInvocation(context.Background(), invocation, input, StreamHooks{}); err == nil {
			t.Fatal("file-backed invocation directory was accepted")
		}
	})
	t.Run("exclusive-stream", func(t *testing.T) {
		directory := t.TempDir()
		input := RunInvocationInput{Directory: directory, AttemptID: "attempt-1", ActorID: "actor-1",
			ThreadReceiptPath: filepath.Join(directory, "thread-receipt.json")}
		if err := os.WriteFile(filepath.Join(directory, "attempt-1.stdout.jsonl"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		invocation := Invocation{Binary: hermetic, Args: []string{"--version"}, WorkDir: directory, Stdin: "input"}
		if _, err := RunInvocation(context.Background(), invocation, input, StreamHooks{}); err == nil {
			t.Fatal("existing stdout evidence was overwritten")
		}
	})
	t.Run("missing-binary", func(t *testing.T) {
		directory := t.TempDir()
		input := RunInvocationInput{Directory: directory, AttemptID: "attempt-1", ActorID: "actor-1",
			ThreadReceiptPath: filepath.Join(directory, "thread-receipt.json")}
		invocation := Invocation{Binary: filepath.Join(directory, "missing"), Args: []string{"exec"},
			WorkDir: directory, Stdin: "input"}
		if _, err := RunInvocation(context.Background(), invocation, input, StreamHooks{}); err == nil {
			t.Fatal("missing Codex binary was accepted")
		}
	})
	t.Run("process-hook", func(t *testing.T) {
		directory := t.TempDir()
		input := RunInvocationInput{Directory: directory, AttemptID: "attempt-1", ActorID: "actor-1",
			ThreadReceiptPath: filepath.Join(directory, "thread-receipt.json")}
		invocation := Invocation{Binary: hermetic, Args: []string{"--version"}, WorkDir: directory, Stdin: "input"}
		_, err := RunInvocation(context.Background(), invocation, input, StreamHooks{
			ProcessStarted: func(ProcessRecord) error { return sentinel },
		})
		if !errors.Is(err, sentinel) {
			t.Fatalf("process hook failure = %v", err)
		}
	})
	t.Run("thread-hook", func(t *testing.T) {
		directory := t.TempDir()
		command := CodexCommand{
			Binary: hermetic, WorkDir: directory, CodexHome: filepath.Join(directory, "codex-home"),
			Model: "gpt-5.6-sol", ReasoningEffort: "low", OutputSchema: filepath.Join(directory, "schema.json"),
			Sandbox: "workspace-write",
		}
		invocation, err := command.InitialInvocation(ControlledEffectPrompt("/bin/true --request /tmp/request.json"))
		if err != nil {
			t.Fatal(err)
		}
		invocation.Env = append(invocation.Env, "CODEX_HERMETIC_THREAD_ID="+testThreadID)
		input := RunInvocationInput{Directory: directory, AttemptID: "attempt-1", ActorID: "actor-1",
			ThreadReceiptPath: filepath.Join(directory, "thread-receipt.json")}
		_, err = RunInvocation(context.Background(), invocation, input, StreamHooks{
			ThreadStarted: func(string) error { return sentinel },
		})
		if !errors.Is(err, sentinel) {
			t.Fatalf("thread hook failure = %v", err)
		}
	})
}

func buildHermeticCodex(t *testing.T) string {
	t.Helper()
	return buildCodexTestBinary(t, "hermetic-codex")
}

func buildCodexTestBinary(t *testing.T, name string) string {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate runner test")
	}
	repositoryRoot := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "../../../../.."))
	binary := filepath.Join(t.TempDir(), name)
	command := exec.Command("go", "build", "-trimpath", "-o", binary,
		"./experiments/durable-vendor-sessions/codex-direct/cmd/"+name)
	command.Dir = repositoryRoot
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build hermetic Codex: %v\n%s", err, output)
	}
	return binary
}

func writeTestJSON(t *testing.T, path string, value any) {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encode %s: %v", path, err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
