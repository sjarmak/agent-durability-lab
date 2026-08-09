//go:build linux

package lab

import (
	"context"
	"os/exec"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/internal/agentprocess"
)

func TestWaitForProcessExitUsesExactPidfdIdentity(t *testing.T) {
	command := exec.Command("sh", "-c", "read line")
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatalf("open process stdin: %v", err)
	}
	if err := command.Start(); err != nil {
		t.Fatalf("start process: %v", err)
	}
	startIdentity, err := agentprocess.ProcessStartIdentity(command.Process.Pid)
	if err != nil {
		t.Fatalf("identify process: %v", err)
	}
	record := ProcessRecord{PID: command.Process.Pid, StartIdentity: startIdentity}
	wrong := record
	wrong.StartIdentity = "wrong"
	if err := waitForProcessExit(context.Background(), wrong); err == nil {
		t.Fatal("wrong process-start identity returned nil error")
	}

	waited := make(chan error, 1)
	go func() { waited <- waitForProcessExit(context.Background(), record) }()
	if err := stdin.Close(); err != nil {
		t.Fatalf("close process stdin: %v", err)
	}
	select {
	case err := <-waited:
		if err != nil {
			t.Fatalf("wait for exact process exit: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pidfd did not observe process exit")
	}
	_ = command.Wait()
	if err := waitForProcessExit(context.Background(), record); err != nil {
		t.Fatalf("already-exited process: %v", err)
	}
}
