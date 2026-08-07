package lab

import (
	"context"
	"path/filepath"
	"testing"
)

func TestStartWorkerProcessPreservesLaunchFailureAndExclusiveLog(t *testing.T) {
	t.Parallel()
	runDirectory := t.TempDir()
	missingBinary := filepath.Join(t.TempDir(), "missing-worker")
	if _, err := startWorkerProcess(missingBinary, runDirectory, "127.0.0.1:7233", "queue", "worker-1"); err == nil {
		t.Fatal("missing Worker binary started")
	}
	if _, err := startWorkerProcess(missingBinary, runDirectory, "127.0.0.1:7233", "queue", "worker-1"); err == nil {
		t.Fatal("existing Worker log was overwritten")
	}
}

func TestManagedWorkerShutdownIsIdempotentAfterWait(t *testing.T) {
	t.Parallel()
	worker := &managedWorker{waited: true}
	status, err := worker.killAndWait()
	if err != nil || status != "already-waited" {
		t.Fatalf("second kill = %q, %v", status, err)
	}
	if err := worker.stop(context.Background()); err != nil {
		t.Fatalf("second stop: %v", err)
	}
}
