package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/sjarmak/temporal_projects/experiments/durable-vendor-sessions/claude-direct/internal/lab"
	"github.com/sjarmak/temporal_projects/internal/failureinject"
)

func TestRunExecutesThePublishedControlledEffectRequest(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	coordinator := failureinject.NewCoordinator()
	server := httptest.NewServer(coordinator.Handler())
	t.Cleanup(server.Close)
	request := lab.ControlledEffectInput{
		DestinationPath: filepath.Join(directory, "destination.db"),
		WorkspacePath:   filepath.Join(directory, "workspace", "effects.jsonl"),
		Payload:         "controlled-edit", BarrierURL: server.URL, BarrierPoint: "effect-committed",
		LogicalSessionID: "session-1", LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
		PhysicalAttemptID: "attempt-1", ActorID: "claude-attempt-1",
	}
	requestPath := filepath.Join(directory, "request.json")
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("encode request: %v", err)
	}
	if err := os.WriteFile(requestPath, encoded, 0o600); err != nil {
		t.Fatalf("write request: %v", err)
	}
	result := make(chan error, 1)
	go func() {
		result <- run(context.Background(), []string{"--request", requestPath})
	}()
	if _, err := coordinator.WaitForArrivals(context.Background(), request.BarrierPoint, 1); err != nil {
		t.Fatalf("wait for effect: %v", err)
	}
	if err := coordinator.Release(request.BarrierPoint); err != nil {
		t.Fatalf("release effect: %v", err)
	}
	if err := <-result; err != nil {
		t.Fatalf("run controlled effect: %v", err)
	}
}

func TestRunRejectsMissingRequestFlag(t *testing.T) {
	t.Parallel()

	if err := run(context.Background(), nil); err == nil {
		t.Fatal("missing request returned nil error")
	}
}
