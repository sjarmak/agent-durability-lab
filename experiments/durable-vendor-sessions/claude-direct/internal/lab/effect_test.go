package lab

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/internal/failureinject"
	"github.com/sjarmak/temporal_projects/internal/workstore"
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

func TestControlledEffectUsesFencedSupervisorAsAuthoritativeDestination(t *testing.T) {
	store := openSupervisorTestStore(t)
	supervisor := newTurnSupervisor(context.Background(), store,
		func(context.Context, *workstore.Store, workstore.Lease) (supervisedResult, error) {
			return supervisedResult{}, errors.New("unused runner")
		}, sequentialCapabilities())
	supervisorServer := httptest.NewServer(newSupervisorHandler(supervisor))
	t.Cleanup(supervisorServer.Close)
	lease := claimSupervisorTestGeneration(t, store, "logical-session-1", "worker-1", "owner-1", 1, false)
	coordinator := failureinject.NewCoordinator()
	barrierServer := httptest.NewServer(coordinator.Handler())
	t.Cleanup(barrierServer.Close)
	input := ControlledEffectInput{
		SupervisorURL: supervisorServer.URL, OwnershipGeneration: lease.Generation,
		OwnerCapability: lease.OwnerToken, Payload: "controlled-edit",
		WorkspacePath: t.TempDir() + "/effects.jsonl",
		BarrierURL:    barrierServer.URL, BarrierPoint: committedEffectBarrier,
		LogicalSessionID: lease.SessionID, LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
		PhysicalAttemptID: "generation-1", ActorID: "supervisor-g1",
	}
	result := make(chan error, 1)
	go func() { result <- RunControlledEffect(context.Background(), input) }()
	arrivals, err := coordinator.WaitForArrivals(context.Background(), committedEffectBarrier, 1)
	if err != nil {
		t.Fatalf("wait for fenced effect: %v", err)
	}
	if len(arrivals) != 1 || arrivals[0].Generation != 1 {
		t.Fatalf("fenced arrival = %+v", arrivals)
	}
	snapshot, err := store.Snapshot(context.Background(), lease.SessionID)
	if err != nil {
		t.Fatalf("authority snapshot: %v", err)
	}
	if len(snapshot.Effects) != 1 || snapshot.Effects[0].ID != input.LogicalEffectID ||
		snapshot.Effects[0].Value != input.Payload || snapshot.Effects[0].Generation != lease.Generation {
		t.Fatalf("authoritative effect = %+v", snapshot.Effects)
	}
	if err := coordinator.Release(committedEffectBarrier); err != nil {
		t.Fatalf("release fenced effect: %v", err)
	}
	if err := <-result; err != nil {
		t.Fatalf("fenced effect: %v", err)
	}

	stale := input
	stale.OwnershipGeneration = lease.Generation + 1
	stale.OwnerCapability = "stale-owner"
	stale.PhysicalAttemptID = "stale-generation"
	if err := RunControlledEffect(context.Background(), stale); !errors.Is(err, workstore.ErrStaleOwner) {
		t.Fatalf("stale controlled effect = %v, want ErrStaleOwner", err)
	}
	snapshot, err = store.Snapshot(context.Background(), lease.SessionID)
	if err != nil {
		t.Fatalf("authority snapshot after stale effect: %v", err)
	}
	if len(snapshot.Effects) != 1 {
		t.Fatalf("stale controlled effect mutated destination: %+v", snapshot.Effects)
	}
	workspace, err := ReadWorkspaceEffects(input.WorkspacePath)
	if err != nil {
		t.Fatalf("read fenced workspace: %v", err)
	}
	if len(workspace) != 1 || workspace[0].PhysicalAttemptID != input.PhysicalAttemptID {
		t.Fatalf("stale controlled effect mutated workspace: %+v", workspace)
	}
}

func TestAppendWorkspaceEffectOnceDeduplicatesConcurrentDeliveryAndRejectsConflict(t *testing.T) {
	path := t.TempDir() + "/effects.jsonl"
	effect := WorkspaceEffect{
		LogicalEffectID: "effect-1", PhysicalAttemptID: "physical-1", Payload: "controlled-edit",
		ActorID: "actor-1", ProcessIdentity: "pid:101:start:one", AppliedAt: time.Now().UTC(),
	}
	errorsByAttempt := make(chan error, 2)
	for attempt := 1; attempt <= 2; attempt++ {
		attempt := attempt
		go func() {
			candidate := effect
			candidate.PhysicalAttemptID = fmt.Sprintf("physical-%d", attempt)
			errorsByAttempt <- AppendWorkspaceEffectOnce(context.Background(), path, candidate)
		}()
	}
	for range 2 {
		if err := <-errorsByAttempt; err != nil {
			t.Fatalf("idempotent workspace effect: %v", err)
		}
	}
	conflict := effect
	conflict.Payload = "conflict"
	if err := AppendWorkspaceEffectOnce(context.Background(), path, conflict); !errors.Is(err, ErrWorkspaceEffectConflict) {
		t.Fatalf("conflicting workspace effect = %v, want ErrWorkspaceEffectConflict", err)
	}
	workspace, err := ReadWorkspaceEffects(path)
	if err != nil {
		t.Fatalf("read idempotent workspace: %v", err)
	}
	if len(workspace) != 1 || workspace[0].LogicalEffectID != effect.LogicalEffectID ||
		workspace[0].Payload != effect.Payload {
		t.Fatalf("idempotent workspace = %+v", workspace)
	}
}
