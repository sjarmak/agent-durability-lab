package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/experiments/durable-vendor-sessions/codex-direct/internal/lab"
	"github.com/sjarmak/temporal_projects/internal/failureinject"
)

func TestRunExecutesPublishedControlledEffectRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	coordinator := failureinject.NewCoordinator()
	server := httptest.NewServer(coordinator.Handler())
	t.Cleanup(server.Close)
	directory := t.TempDir()
	requestPath := filepath.Join(directory, "request.json")
	request := lab.ControlledEffectInput{
		DestinationPath: filepath.Join(directory, "destination.db"),
		WorkspacePath:   filepath.Join(directory, "effects.jsonl"), Payload: "controlled-edit",
		ThreadReceiptPath: filepath.Join(directory, "thread-receipt.json"),
		BarrierURL:        server.URL, BarrierPoint: "effect-committed",
		LogicalSessionID: "session-1", LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
		PhysicalAttemptID: "attempt-1", ActorID: "codex-attempt-1",
	}
	receipt, err := json.Marshal(lab.ThreadReceipt{
		ThreadID: "019ff302-7730-7f21-90ed-73c37fb4e8fa", PhysicalAttemptID: request.PhysicalAttemptID,
		ActorID: request.ActorID, PID: 1, ProcessStart: "command-test",
		ProcessIdentity: "pid:1:start:command-test", ObservedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("encode receipt: %v", err)
	}
	if err := os.WriteFile(request.ThreadReceiptPath, receipt, 0o600); err != nil {
		t.Fatalf("write receipt: %v", err)
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("encode request: %v", err)
	}
	if err := os.WriteFile(requestPath, encoded, 0o600); err != nil {
		t.Fatalf("write request: %v", err)
	}
	finished := make(chan error, 1)
	go func() { finished <- run(ctx, []string{"--request", requestPath}) }()
	if _, err := coordinator.WaitForArrivals(ctx, request.BarrierPoint, 1); err != nil {
		t.Fatalf("wait for effect: %v", err)
	}
	if err := coordinator.Release(request.BarrierPoint); err != nil {
		t.Fatalf("release effect: %v", err)
	}
	if err := <-finished; err != nil {
		t.Fatalf("run effect: %v", err)
	}
	if err := run(context.Background(), []string{"--request", requestPath}); err == nil {
		t.Fatal("duplicate physical effect unexpectedly succeeded")
	}
}

func TestRunRejectsIncompleteRequest(t *testing.T) {
	requestPath := filepath.Join(t.TempDir(), "request.json")
	if err := os.WriteFile(requestPath, []byte(`{"workspace_path":"relative"}`), 0o600); err != nil {
		t.Fatalf("write request: %v", err)
	}
	if err := run(context.Background(), []string{"--request", requestPath}); err == nil {
		t.Fatal("unsafe request unexpectedly succeeded")
	}
}
