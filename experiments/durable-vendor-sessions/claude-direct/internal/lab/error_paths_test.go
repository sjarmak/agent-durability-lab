package lab

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestIOBoundariesPropagateCancellationAndFilesystemFailures(t *testing.T) {
	t.Parallel()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	directory := t.TempDir()
	attempt := EffectAttempt{
		LogicalSessionID: "session", LogicalTurnID: "turn", LogicalEffectID: "effect",
		PhysicalAttemptID: "attempt", ActorID: "actor", ProcessIdentity: "pid:1:start:one",
		AppliedAt: time.Now().UTC(),
	}
	if err := CommitEffect(canceled, filepath.Join(directory, "destination.db"), attempt); !errors.Is(err, context.Canceled) {
		t.Fatalf("commit cancellation = %v", err)
	}
	if _, err := ReadDestination(canceled, filepath.Join(directory, "destination.db")); !errors.Is(err, context.Canceled) {
		t.Fatalf("read cancellation = %v", err)
	}
	workspace := WorkspaceEffect{
		LogicalEffectID: "effect", PhysicalAttemptID: "attempt", Payload: "payload",
		ActorID: "actor", ProcessIdentity: "pid:1:start:one", AppliedAt: time.Now().UTC(),
	}
	if err := AppendWorkspaceEffect(canceled, filepath.Join(directory, "effects.jsonl"), workspace); !errors.Is(err, context.Canceled) {
		t.Fatalf("workspace cancellation = %v", err)
	}
	if err := writeBytesExclusive(filepath.Join(directory, "missing", "file"), []byte("data")); err == nil {
		t.Fatal("write below missing parent returned nil error")
	}
	if _, err := ReadDestination(context.Background(), filepath.Join(directory, "missing.db")); err == nil {
		t.Fatal("missing destination returned nil error")
	}
	if _, err := ReadDestination(context.Background(), ""); err == nil {
		t.Fatal("empty destination path returned nil error")
	}
	malformed := filepath.Join(directory, "malformed.jsonl")
	if err := os.WriteFile(malformed, []byte("not-json\n"), 0o600); err != nil {
		t.Fatalf("write malformed workspace: %v", err)
	}
	if _, err := ReadWorkspaceEffects(malformed); err == nil {
		t.Fatal("malformed workspace returned nil error")
	}
}

func TestControlledEffectPropagatesInvalidInputCancellationAndBarrierFailure(t *testing.T) {
	t.Parallel()

	if err := RunControlledEffect(context.Background(), ControlledEffectInput{}); err == nil {
		t.Fatal("empty controlled effect returned nil error")
	}
	directory := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		http.Error(response, "rejected", http.StatusConflict)
	}))
	t.Cleanup(server.Close)
	input := ControlledEffectInput{
		DestinationPath: filepath.Join(directory, "destination.db"),
		WorkspacePath:   filepath.Join(directory, "effects.jsonl"), Payload: "payload",
		BarrierURL: server.URL, BarrierPoint: "point", LogicalSessionID: "session",
		LogicalTurnID: "turn", LogicalEffectID: "effect", PhysicalAttemptID: "attempt",
		ActorID: "actor",
	}
	if err := RunControlledEffect(context.Background(), input); err == nil {
		t.Fatal("barrier rejection returned nil error")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	input.PhysicalAttemptID = "attempt-canceled"
	if err := RunControlledEffect(canceled, input); !errors.Is(err, context.Canceled) {
		t.Fatalf("controlled effect cancellation = %v", err)
	}
}

func TestRunnerAndMetadataRejectUnusableExecutables(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	if _, err := RunInvocation(context.Background(), Invocation{}, RunInvocationInput{}); err == nil {
		t.Fatal("empty invocation returned nil error")
	}
	if _, err := RunInvocation(context.Background(), Invocation{
		Binary: filepath.Join(directory, "missing"), WorkDir: directory, Stdin: "prompt\n",
	}, RunInvocationInput{Directory: filepath.Join(directory, "attempt"), AttemptID: "attempt-1", ActorID: "actor"}); err == nil {
		t.Fatal("missing executable returned nil error")
	}
	emptyVersion := filepath.Join(directory, "empty-version")
	if err := os.WriteFile(emptyVersion, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write empty-version executable: %v", err)
	}
	options := ExperimentOptions{
		ClaudeBinary: emptyVersion, WorkerBinary: emptyVersion, EffectBinary: emptyVersion,
		LauncherBinary: emptyVersion,
	}
	if _, err := inspectExperimentBinaries(context.Background(), options); err == nil {
		t.Fatal("empty Claude version returned nil error")
	}
}

func TestFixtureAndBarrierVerificationRejectMissingObservedEffect(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	if err := prepareFixture(context.Background(), directory); err != nil {
		t.Fatalf("prepare fixture: %v", err)
	}
	if _, err := workspaceStatus(context.Background(), directory); err == nil {
		t.Fatal("clean fixture status returned nil error")
	}
	if err := verifyStateAtBarrier(
		context.Background(), filepath.Join(directory, "missing.db"), filepath.Join(directory, "missing.jsonl"), 1,
	); err == nil {
		t.Fatal("missing exact barrier state returned nil error")
	}
}

func TestManagedWorkerNilAndStartFailurePaths(t *testing.T) {
	t.Parallel()

	var worker *managedWorker
	if _, _, err := worker.killAndWait(); err != nil {
		t.Fatalf("nil Worker kill = %v", err)
	}
	if err := worker.stop(context.Background()); err != nil {
		t.Fatalf("nil Worker stop = %v", err)
	}
	if _, err := startWorkerProcess(workerProcessConfig{
		Binary: filepath.Join(t.TempDir(), "missing-worker"), Directory: t.TempDir(), WorkerID: "worker",
	}); err == nil {
		t.Fatal("missing Worker binary returned nil error")
	}
}
