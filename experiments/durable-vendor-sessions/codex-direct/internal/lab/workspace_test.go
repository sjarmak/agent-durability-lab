package lab

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWorkspaceEffectOnceIsIdempotentAndRejectsConflict(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "effects.jsonl")
	effect := WorkspaceEffect{
		LogicalEffectID: "effect-1", PhysicalAttemptID: "attempt-1", Payload: "controlled-edit",
		ActorID: "actor-1", ProcessIdentity: "pid:1:start:test", AppliedAt: time.Now().UTC(),
	}
	if err := AppendWorkspaceEffectOnce(context.Background(), path, effect); err != nil {
		t.Fatalf("append effect once: %v", err)
	}
	retry := effect
	retry.PhysicalAttemptID = "attempt-2"
	if err := AppendWorkspaceEffectOnce(context.Background(), path, retry); err != nil {
		t.Fatalf("idempotent retry: %v", err)
	}
	conflict := retry
	conflict.Payload = "different-edit"
	if err := AppendWorkspaceEffectOnce(context.Background(), path, conflict); !errors.Is(err, ErrWorkspaceEffectConflict) {
		t.Fatalf("conflicting retry = %v", err)
	}
	effects, err := ReadWorkspaceEffects(path)
	if err != nil || len(effects) != 1 || effects[0].PhysicalAttemptID != effect.PhysicalAttemptID {
		t.Fatalf("workspace effects = %+v, err=%v", effects, err)
	}
}

func TestWorkspaceJournalRejectsInvalidInputAndCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "effects.jsonl")
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := AppendWorkspaceEffect(canceled, path, WorkspaceEffect{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled append = %v", err)
	}
	if err := AppendWorkspaceEffect(context.Background(), "", WorkspaceEffect{}); err == nil {
		t.Fatal("invalid workspace effect was accepted")
	}
	if err := os.WriteFile(path, []byte("not-json\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadWorkspaceEffects(path); err == nil {
		t.Fatal("malformed workspace journal was accepted")
	}
	effect := WorkspaceEffect{
		LogicalEffectID: "effect-1", PhysicalAttemptID: "attempt-1", Payload: "controlled-edit",
		ActorID: "actor-1", ProcessIdentity: "pid:1:start:test", AppliedAt: time.Now().UTC(),
	}
	encoded := `{"logical_effect_id":"effect-1","physical_attempt_id":"attempt-1","payload":"controlled-edit","actor_id":"actor-1","process_identity":"pid:1:start:test","applied_at":"` +
		effect.AppliedAt.Format(time.RFC3339Nano) + `"}`
	if err := os.WriteFile(path, []byte(encoded+"\n"+encoded+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadWorkspaceEffects(path); err == nil {
		t.Fatal("duplicate physical identity was accepted")
	}
}
