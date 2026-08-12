package lab

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sjarmak/temporal_projects/internal/failureinject"
)

func TestWorkerCommandPinsCodexProfileModelAndFault(t *testing.T) {
	config := workerProcessConfig{
		Binary: "/opt/worker", Directory: "/tmp/run", TemporalAddress: "127.0.0.1:7233",
		TaskQueue: "queue", WorkerID: "worker-1", CodexBinary: "/opt/codex", CodexHome: "/tmp/codex-home",
		LauncherBinary: "/opt/launcher", EffectBinary: "/opt/effect", FixtureDirectory: "/tmp/fixture",
		DestinationPath: "/tmp/destination.db", WorkspacePath: "/tmp/effects.jsonl", RunRoot: "/tmp/attempts",
		BarrierURL: "http://127.0.0.1:8080", BarrierPoint: committedEffectBarrier,
		FaultBoundary: FaultAfterToolEffect, Model: "gpt-5.6-sol", ReasoningEffort: "low", OutputSchema: "/tmp/schema.json",
		Hermetic: true,
	}
	command := workerCommand(config, "/tmp/ready.json")
	joined := strings.Join(command.Args, " ")
	for _, want := range []string{"--codex-home /tmp/codex-home", "--model gpt-5.6-sol", "--reasoning-effort low", "--hermetic"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("command %q lacks %q", joined, want)
		}
	}
}

func TestManagedWorkerRejectsInvalidStartupAndNilShutdown(t *testing.T) {
	var worker *managedWorker
	if _, err := worker.killAndWait(); err != nil {
		t.Fatalf("nil kill: %v", err)
	}
	if err := worker.stop(context.Background()); err != nil {
		t.Fatalf("nil stop: %v", err)
	}
	file := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := startWorkerProcess(workerProcessConfig{Directory: file, WorkerID: "worker-1"}); err == nil {
		t.Fatal("file-backed Worker directory was accepted")
	}
	if _, err := startWorkerProcess(workerProcessConfig{
		Binary: filepath.Join(t.TempDir(), "missing-worker"), Directory: t.TempDir(), WorkerID: "worker-1",
	}); err == nil {
		t.Fatal("missing Worker binary was accepted")
	}
}

func TestWorkerCoverageEnvironmentRequiresExplicitRealDirectory(t *testing.T) {
	coverageDirectory := t.TempDir()
	t.Setenv(codexDirectCoverageDirectoryEnvironment, coverageDirectory)
	environment, err := workerProcessEnvironment([]string{"PATH=/usr/bin", "GOCOVERDIR=discarded"})
	if err != nil {
		t.Fatalf("coverage environment: %v", err)
	}
	want := "GOCOVERDIR=" + coverageDirectory
	if !containsString(environment, want) {
		t.Fatalf("coverage environment %q lacks %q", environment, want)
	}

	file := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(codexDirectCoverageDirectoryEnvironment, file)
	if _, err := workerProcessEnvironment(nil); err == nil {
		t.Fatal("non-directory coverage destination was accepted")
	}
}

func TestWorkerCredentialUsesInheritedFDAndNeverCommandArguments(t *testing.T) {
	credential, err := failureinject.NewCredential()
	if err != nil {
		t.Fatalf("new credential: %v", err)
	}
	command := workerCommand(workerProcessConfig{Binary: "/opt/worker"}, "/tmp/ready")
	command.Env = []string{"PATH=/usr/bin"}
	reader, err := addBarrierCredential(command, credential)
	if err != nil {
		t.Fatalf("add credential: %v", err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	if strings.Contains(strings.Join(command.Args, " "), failureinject.CredentialFDEnvironment) {
		t.Fatal("worker command arguments exposed credential transport")
	}
	if len(command.ExtraFiles) != 1 || !containsString(command.Env, failureinject.CredentialFDEnvironment+"=3") {
		t.Fatalf("credential transport = env %q files %d", command.Env, len(command.ExtraFiles))
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
