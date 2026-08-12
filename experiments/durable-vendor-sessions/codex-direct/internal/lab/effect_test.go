package lab

import (
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/internal/failureinject"
	"github.com/sjarmak/temporal_projects/internal/workstore"
)

func TestFencedEffectCommitsDirectlyThroughAuthorityStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "authority.db")
	store, err := workstore.Open(path)
	if err != nil {
		t.Fatalf("open authority store: %v", err)
	}
	decision, err := store.StartOrAttach(context.Background(), workstore.StartRequest{
		SessionID: "session-1", Mode: workstore.ModeFenced, CandidateOwner: "owner-1",
		WorkerID: "worker-1", Attempt: 1,
	})
	if err != nil {
		t.Fatalf("claim authority: %v", err)
	}
	if err := store.RegisterProcess(context.Background(), decision.Lease, workstore.Process{
		PID: 101, StartIdentity: "boot:101", ProcessGroupID: 101,
	}); err != nil {
		t.Fatalf("register process: %v", err)
	}
	input := ControlledEffectInput{
		AuthorityStorePath: path, OwnershipGeneration: decision.Lease.Generation,
		OwnerCapability: decision.Lease.OwnerToken, LogicalSessionID: decision.Lease.SessionID,
		LogicalEffectID: "effect-1", Payload: "controlled-edit",
	}
	if err := commitFencedEffect(context.Background(), input); err != nil {
		t.Fatalf("commit fenced effect: %v", err)
	}
	snapshot, err := store.Snapshot(context.Background(), decision.Lease.SessionID)
	if err != nil {
		t.Fatalf("read authority: %v", err)
	}
	if len(snapshot.Effects) != 1 || snapshot.Effects[0].Effect.ID != input.LogicalEffectID {
		t.Fatalf("authority effects = %+v", snapshot.Effects)
	}
	stale := input
	stale.OwnerCapability = "stale"
	if err := commitFencedEffect(context.Background(), stale); !errors.Is(err, workstore.ErrStaleOwner) {
		t.Fatalf("stale fenced effect = %v, want ErrStaleOwner", err)
	}
}

func TestControlledEffectCommitsBothReceiptsBeforeExactBarrier(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	coordinator := failureinject.NewCoordinator()
	server := httptest.NewServer(coordinator.Handler())
	t.Cleanup(server.Close)
	directory := t.TempDir()
	input := ControlledEffectInput{
		DestinationPath:   filepath.Join(directory, "destination.db"),
		WorkspacePath:     filepath.Join(directory, "workspace", "effects.jsonl"),
		ThreadReceiptPath: filepath.Join(directory, "thread-receipt.json"),
		Payload:           "controlled-edit", BarrierURL: server.URL, BarrierPoint: committedEffectBarrier,
		LogicalSessionID: "session-1", LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
		PhysicalAttemptID: "attempt-1", ActorID: "codex-attempt-1",
	}
	writeTestJSON(t, input.ThreadReceiptPath, ThreadReceipt{
		ThreadID: "019ff302-7730-7f21-90ed-73c37fb4e8fa", PhysicalAttemptID: input.PhysicalAttemptID,
		ActorID: input.ActorID, PID: 1, ProcessStart: "effect-test",
		ProcessIdentity: "pid:1:start:effect-test", ObservedAt: time.Now().UTC(),
	})
	finished := make(chan error, 1)
	go func() { finished <- RunControlledEffect(ctx, input) }()

	arrivals, err := coordinator.WaitForArrivals(ctx, committedEffectBarrier, 1)
	if err != nil {
		t.Fatalf("wait for effect: %v", err)
	}
	if len(arrivals) != 1 || arrivals[0].ID != input.PhysicalAttemptID || arrivals[0].ActorID != input.ActorID {
		t.Fatalf("arrival = %+v", arrivals)
	}
	destination, err := ReadDestination(context.Background(), input.DestinationPath)
	if err != nil {
		t.Fatalf("read destination: %v", err)
	}
	workspace, err := ReadWorkspaceEffects(input.WorkspacePath)
	if err != nil {
		t.Fatalf("read workspace: %v", err)
	}
	if len(destination.Attempts) != 1 || len(workspace) != 1 ||
		destination.Attempts[0].PhysicalAttemptID != input.PhysicalAttemptID ||
		workspace[0].PhysicalAttemptID != input.PhysicalAttemptID {
		t.Fatalf("receipts at barrier: destination=%+v workspace=%+v", destination, workspace)
	}
	select {
	case err := <-finished:
		t.Fatalf("effect returned before release: %v", err)
	default:
	}
	if err := coordinator.Release(committedEffectBarrier); err != nil {
		t.Fatalf("release effect: %v", err)
	}
	if err := <-finished; err != nil {
		t.Fatalf("effect: %v", err)
	}
}

func TestControlledEffectRejectsMissingOrMismatchedThreadReceipt(t *testing.T) {
	directory := t.TempDir()
	input := ControlledEffectInput{
		DestinationPath:   filepath.Join(directory, "destination.db"),
		WorkspacePath:     filepath.Join(directory, "effects.jsonl"),
		ThreadReceiptPath: filepath.Join(directory, "thread-receipt.json"),
		Payload:           "controlled-edit", BarrierURL: "http://127.0.0.1:1", BarrierPoint: committedEffectBarrier,
		LogicalSessionID: "session-1", LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
		PhysicalAttemptID: "attempt-1", ActorID: "codex-attempt-1",
	}
	if err := RunControlledEffect(context.Background(), input); err == nil {
		t.Fatal("missing thread receipt unexpectedly allowed an effect")
	}
	writeTestJSON(t, input.ThreadReceiptPath, ThreadReceipt{
		ThreadID: "019ff302-7730-7f21-90ed-73c37fb4e8fa", PhysicalAttemptID: "other-attempt",
		ActorID: input.ActorID, PID: 1, ProcessStart: "test",
		ProcessIdentity: "pid:1:start:test", ObservedAt: time.Now().UTC(),
	})
	if err := RunControlledEffect(context.Background(), input); err == nil {
		t.Fatal("mismatched thread receipt unexpectedly allowed an effect")
	}
	if _, err := os.Stat(input.DestinationPath); !os.IsNotExist(err) {
		t.Fatalf("rejected effect mutated destination: %v", err)
	}
}

func TestControlledEffectRejectsThreadOutsideCanonicalRegistration(t *testing.T) {
	directory := t.TempDir()
	input := ControlledEffectInput{
		DestinationPath: filepath.Join(directory, "destination.db"), WorkspacePath: filepath.Join(directory, "effects.jsonl"),
		ThreadReceiptPath:   filepath.Join(directory, "thread-receipt.json"),
		CanonicalThreadPath: filepath.Join(directory, "canonical-thread.json"),
		Payload:             "controlled-edit", BarrierURL: "http://127.0.0.1:1", BarrierPoint: committedEffectBarrier,
		LogicalSessionID: "session-1", LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
		PhysicalAttemptID: "attempt-1", ActorID: "codex-attempt-1",
	}
	writeTestJSON(t, input.ThreadReceiptPath, ThreadReceipt{
		ThreadID: "019ff302-7730-7f21-90ed-73c37fb4e8fa", PhysicalAttemptID: input.PhysicalAttemptID,
		ActorID: input.ActorID, PID: 1, ProcessStart: "test",
		ProcessIdentity: "pid:1:start:test", ObservedAt: time.Now().UTC(),
	})
	writeTestJSON(t, input.CanonicalThreadPath, CanonicalThread{
		LogicalSessionID: input.LogicalSessionID, LogicalTurnID: input.LogicalTurnID,
		ThreadID: "019ff302-7730-7f21-90ed-73c37fb4e8fb", FirstPhysicalAttemptID: "other",
		RegisteredAt: time.Now().UTC(),
	})
	if err := RunControlledEffect(context.Background(), input); err == nil {
		t.Fatal("non-canonical thread unexpectedly allowed an effect")
	}
	if _, err := os.Stat(input.DestinationPath); !os.IsNotExist(err) {
		t.Fatalf("non-canonical effect mutated destination: %v", err)
	}
}

func TestUnsafeDestinationPreservesDuplicateLogicalEffects(t *testing.T) {
	path := filepath.Join(t.TempDir(), "destination.db")
	first := testEffectAttempt("attempt-1", "actor-1")
	second := testEffectAttempt("attempt-2", "actor-2")
	if err := CommitEffect(context.Background(), path, first); err != nil {
		t.Fatalf("first effect: %v", err)
	}
	if err := CommitEffect(context.Background(), path, second); err != nil {
		t.Fatalf("second effect: %v", err)
	}
	snapshot, err := ReadDestination(context.Background(), path)
	if err != nil {
		t.Fatalf("read destination: %v", err)
	}
	if len(snapshot.Attempts) != 2 || snapshot.Attempts[0].LogicalEffectID != snapshot.Attempts[1].LogicalEffectID {
		t.Fatalf("destination = %+v", snapshot)
	}
	if err := CommitEffect(context.Background(), path, first); err == nil {
		t.Fatal("reused physical attempt unexpectedly succeeded")
	}
}

func TestDestinationRejectsCanceledInvalidAndMissingState(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	path := filepath.Join(t.TempDir(), "destination.db")
	if err := CommitEffect(ctx, path, testEffectAttempt("attempt-1", "actor-1")); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled commit = %v", err)
	}
	if _, err := ReadDestination(ctx, path); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read = %v", err)
	}
	if err := CommitEffect(context.Background(), "", EffectAttempt{}); err == nil {
		t.Fatal("invalid destination effect was accepted")
	}
	if _, err := ReadDestination(context.Background(), ""); err == nil {
		t.Fatal("empty destination path was accepted")
	}
	if _, err := ReadDestination(context.Background(), path); err == nil {
		t.Fatal("missing destination was accepted")
	}
	if err := CommitEffect(context.Background(), filepath.Dir(path), testEffectAttempt("attempt-1", "actor-1")); err == nil {
		t.Fatal("directory was accepted as a destination database")
	}
}

func testEffectAttempt(physical, actor string) EffectAttempt {
	return EffectAttempt{
		LogicalSessionID: "session-1", LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
		PhysicalAttemptID: physical, ActorID: actor, ProcessIdentity: "pid:1:start:" + actor,
		AppliedAt: time.Date(2026, 8, 11, 20, 0, 0, 0, time.UTC),
	}
}
