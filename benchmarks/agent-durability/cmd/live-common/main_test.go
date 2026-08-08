package main

import (
	"context"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

func TestRunPreservesThreeLiveTrialsForEveryCaseAndProbe(t *testing.T) {
	agentBinary := buildAgent(t)
	result, err := run(context.Background(), t.TempDir(), agentBinary, "test-source-v1")
	if err != nil {
		t.Fatalf("run live suite: %v", err)
	}
	wantPerClass := len(protocol.Cases()) * 3
	if result.ValidPass != wantPerClass*2 || result.ValidFail != wantPerClass || result.Invalid != 0 {
		t.Fatalf("summary = %+v, want passes=%d failures=%d invalid=0", result, wantPerClass*2, wantPerClass)
	}
}

func buildAgent(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate live command test")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", ".."))
	binary := filepath.Join(t.TempDir(), "agent-simulator")
	command := exec.Command("go", "build", "-o", binary, "./cmd/agent-simulator")
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build agent simulator: %v: %s", err, output)
	}
	return binary
}
