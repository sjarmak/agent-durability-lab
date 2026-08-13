package lab

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/internal/failureinject"
)

func TestManagedWorkerKillAndStopAreBounded(t *testing.T) {
	for _, test := range []struct {
		name   string
		script string
		act    func(*managedWorker) error
	}{
		{name: "kill", script: ": > \"$2\"; while :; do :; done", act: func(worker *managedWorker) error {
			status, err := worker.killAndWait()
			if err != nil || !strings.Contains(status, "killed") {
				return errors.New("worker did not report SIGKILL")
			}
			status, err = worker.killAndWait()
			if err != nil || status != "already-waited" {
				return errors.New("second kill was not idempotent")
			}
			return nil
		}},
		{name: "graceful stop", script: "trap 'exit 0' INT; : > \"$2\"; while :; do :; done", act: func(worker *managedWorker) error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := worker.stop(ctx); err != nil {
				return err
			}
			return worker.stop(ctx)
		}},
		{name: "forced stop", script: "trap '' INT; : > \"$2\"; while :; do :; done", act: func(worker *managedWorker) error {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			err := worker.stop(ctx)
			if !errors.Is(err, context.DeadlineExceeded) {
				return errors.New("forced stop did not retain deadline error")
			}
			return nil
		}},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			worker := startTestWorker(t, test.script)
			if err := test.act(worker); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStartWorkerProcessRejectsCredentialAndExecutableFailures(t *testing.T) {
	t.Parallel()

	config := workerProcessConfig{Binary: "/absent/worker", RunDirectory: t.TempDir(), WorkerID: "worker-1"}
	if _, err := startWorkerProcess(config); err == nil {
		t.Fatal("missing credential accepted")
	}
	credential, err := failureinject.NewCredential()
	if err != nil {
		t.Fatalf("NewCredential: %v", err)
	}
	config.Credential = credential
	config.WorkerID = "worker-2"
	if _, err := startWorkerProcess(config); err == nil {
		t.Fatal("missing Worker executable accepted")
	}
}

func startTestWorker(t *testing.T, script string) *managedWorker {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "worker")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\n"+script+"\n"), 0o700); err != nil {
		t.Fatalf("write helper Worker: %v", err)
	}
	credential, err := failureinject.NewCredential()
	if err != nil {
		t.Fatalf("NewCredential: %v", err)
	}
	ready := filepath.Join(t.TempDir(), "ready")
	worker, err := startWorkerProcess(workerProcessConfig{
		Binary: binary, RunDirectory: t.TempDir(), WorkerID: "worker-1", Credential: credential, Address: ready,
	})
	if err != nil {
		t.Fatalf("startWorkerProcess: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("helper Worker did not become ready")
		}
		time.Sleep(time.Millisecond)
	}
	return worker
}
