package lab

import (
	"context"
	"path/filepath"
	"testing"
)

func TestStartWorkerReportsLaunchAndExclusiveLogFailures(t *testing.T) {
	directory := t.TempDir()
	options := Options{WorkerBinary: filepath.Join(directory, "missing-worker"), AgentBinary: "agent"}
	arguments := []string{directory, "127.0.0.1:7233", "queue", "store", "barrier", "worker-1"}
	if _, err := startWorker(options, arguments[0], arguments[1], arguments[2], arguments[3], arguments[4], arguments[5]); err == nil {
		t.Fatal("startWorker accepted a missing Worker binary")
	}
	if _, err := startWorker(options, arguments[0], arguments[1], arguments[2], arguments[3], arguments[4], arguments[5]); err == nil {
		t.Fatal("startWorker reused an append-only Worker log")
	}
}

func TestManagedProcessStopIsIdempotentAfterWait(t *testing.T) {
	process := &managedProcess{waited: true}
	if err := process.stop(context.Background()); err != nil {
		t.Fatalf("stop waited process: %v", err)
	}
	if err := process.killAndWait(); err != nil {
		t.Fatalf("kill waited process: %v", err)
	}
}
