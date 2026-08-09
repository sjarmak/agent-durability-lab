package lab

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/internal/failureinject"
)

func TestControlledEffectCommitsBeforeItsExactBarrierAndBlocksUntilRelease(t *testing.T) {
	t.Parallel()

	coordinator := failureinject.NewCoordinator()
	server := httptest.NewServer(coordinator.Handler())
	t.Cleanup(server.Close)
	directory := t.TempDir()
	input := ControlledEffectInput{
		DestinationPath:   directory + "/destination.db",
		WorkspacePath:     directory + "/workspace/effects.jsonl",
		Payload:           "controlled-edit",
		BarrierURL:        server.URL,
		BarrierPoint:      "claude-tool-effect-committed",
		LogicalSessionID:  "logical-session-1",
		LogicalTurnID:     "turn-1",
		LogicalEffectID:   "effect-1",
		PhysicalAttemptID: "activity-attempt-1",
		ActorID:           "claude-attempt-1",
	}
	result := make(chan error, 1)
	go func() {
		result <- RunControlledEffect(context.Background(), input)
	}()

	arrivals, err := coordinator.WaitForArrivals(context.Background(), input.BarrierPoint, 1)
	if err != nil {
		t.Fatalf("wait for exact barrier: %v", err)
	}
	if len(arrivals) != 1 || arrivals[0].ID != input.PhysicalAttemptID || arrivals[0].ActorID != input.ActorID {
		t.Fatalf("arrivals = %+v", arrivals)
	}
	snapshot, err := ReadDestination(context.Background(), input.DestinationPath)
	if err != nil {
		t.Fatalf("read destination at barrier: %v", err)
	}
	if len(snapshot.Attempts) != 1 || !snapshot.Attempts[0].Applied {
		t.Fatalf("destination at barrier = %+v", snapshot)
	}
	workspace, err := ReadWorkspaceEffects(input.WorkspacePath)
	if err != nil {
		t.Fatalf("read workspace at barrier: %v", err)
	}
	if len(workspace) != 1 || workspace[0].PhysicalAttemptID != input.PhysicalAttemptID || workspace[0].Payload != input.Payload {
		t.Fatalf("workspace at barrier = %+v", workspace)
	}
	select {
	case err := <-result:
		t.Fatalf("effect returned before barrier release: %v", err)
	default:
	}
	if err := coordinator.Release(input.BarrierPoint); err != nil {
		t.Fatalf("release barrier: %v", err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("effect after release: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("effect did not return after release")
	}
}
