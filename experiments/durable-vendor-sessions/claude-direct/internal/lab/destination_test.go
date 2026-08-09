package lab

import (
	"context"
	"testing"
	"time"
)

func TestUnsafeDestinationAppliesDistinctPhysicalAttemptsForOneLogicalEffect(t *testing.T) {
	t.Parallel()

	path := t.TempDir() + "/destination.db"
	first := EffectAttempt{
		LogicalSessionID:  "logical-session-1",
		LogicalTurnID:     "turn-1",
		LogicalEffectID:   "effect-1",
		PhysicalAttemptID: "activity-attempt-1",
		ActorID:           "claude-attempt-1",
		ProcessIdentity:   "pid:101:start:one",
		AppliedAt:         time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC),
	}
	second := first
	second.PhysicalAttemptID = "activity-attempt-2"
	second.ActorID = "claude-attempt-2"
	second.ProcessIdentity = "pid:202:start:two"
	second.AppliedAt = first.AppliedAt.Add(time.Second)

	if err := CommitEffect(context.Background(), path, first); err != nil {
		t.Fatalf("commit first effect: %v", err)
	}
	if err := CommitEffect(context.Background(), path, second); err != nil {
		t.Fatalf("commit second effect: %v", err)
	}
	snapshot, err := ReadDestination(context.Background(), path)
	if err != nil {
		t.Fatalf("read destination: %v", err)
	}
	if len(snapshot.Attempts) != 2 || !snapshot.Attempts[0].Applied || !snapshot.Attempts[1].Applied {
		t.Fatalf("attempts = %+v, want two applied physical attempts", snapshot.Attempts)
	}
	if snapshot.Attempts[0].LogicalEffectID != snapshot.Attempts[1].LogicalEffectID {
		t.Fatalf("logical effects differ: %+v", snapshot.Attempts)
	}
}

func TestDestinationRejectsReusedPhysicalAttemptIdentity(t *testing.T) {
	t.Parallel()

	path := t.TempDir() + "/destination.db"
	attempt := EffectAttempt{
		LogicalSessionID: "logical-session-1", LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
		PhysicalAttemptID: "physical-1", ActorID: "claude-1", ProcessIdentity: "pid:1:start:one",
		AppliedAt: time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC),
	}
	if err := CommitEffect(context.Background(), path, attempt); err != nil {
		t.Fatalf("commit first effect: %v", err)
	}
	if err := CommitEffect(context.Background(), path, attempt); err == nil {
		t.Fatal("reused physical attempt identity returned nil error")
	}
}
