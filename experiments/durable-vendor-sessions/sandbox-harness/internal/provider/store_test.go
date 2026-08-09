package provider

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestStoreDistinguishesUnsafeAndIdempotentCommandDelivery(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name        string
		mode        Mode
		wantApplied []bool
		wantEffects []string
	}{
		{name: "unsafe", mode: ModeUnsafe, wantApplied: []bool{true, true}, wantEffects: []string{"effect-1", "effect-1"}},
		{name: "idempotent", mode: ModeIdempotent, wantApplied: []bool{true, false}, wantEffects: []string{"effect-1"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			store := createStore(t, test.mode)
			instance := apply(t, store, Request{
				Kind: OperationStart, OperationID: "start-1", PhysicalAttemptID: "start-attempt-1",
			})
			var applied []bool
			for index := range 2 {
				result := apply(t, store, Request{
					Kind: OperationCommand, OperationID: "command-1",
					PhysicalAttemptID: "command-attempt-" + string(rune('1'+index)),
					InstanceID:        instance.InstanceID, LogicalEffectID: "effect-1", Payload: "fixture",
				})
				applied = append(applied, result.Applied)
			}
			snapshot, err := store.Snapshot(context.Background())
			if err != nil {
				t.Fatalf("Snapshot() error = %v", err)
			}
			if got := snapshot.Instance(instance.InstanceID).Effects; !equalStrings(got, test.wantEffects) {
				t.Fatalf("effects = %v, want %v", got, test.wantEffects)
			}
			if !equalBools(applied, test.wantApplied) {
				t.Fatalf("applied = %v, want %v", applied, test.wantApplied)
			}
		})
	}
}

func TestStoreRestoresExactSnapshotPrefix(t *testing.T) {
	t.Parallel()
	store := createStore(t, ModeIdempotent)
	origin := apply(t, store, Request{Kind: OperationStart, OperationID: "start-origin", PhysicalAttemptID: "start-origin-1"})
	apply(t, store, Request{
		Kind: OperationCommand, OperationID: "command-a", PhysicalAttemptID: "command-a-1",
		InstanceID: origin.InstanceID, LogicalEffectID: "effect-a", Payload: "a",
	})
	snapshotResult := apply(t, store, Request{
		Kind: OperationSnapshot, OperationID: "snapshot-a", PhysicalAttemptID: "snapshot-a-1", InstanceID: origin.InstanceID,
	})
	apply(t, store, Request{
		Kind: OperationCommand, OperationID: "command-b", PhysicalAttemptID: "command-b-1",
		InstanceID: origin.InstanceID, LogicalEffectID: "effect-b", Payload: "b",
	})
	fork := apply(t, store, Request{
		Kind: OperationStartFromSnapshot, OperationID: "start-fork", PhysicalAttemptID: "start-fork-1",
		SnapshotID: snapshotResult.SnapshotID,
	})

	state, err := store.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	forkState := state.Instance(fork.InstanceID)
	if !equalStrings(forkState.Effects, []string{"effect-a"}) {
		t.Fatalf("fork effects = %v, want snapshot prefix", forkState.Effects)
	}
	if forkState.ParentSnapshotID != snapshotResult.SnapshotID {
		t.Fatalf("parent snapshot = %q, want %q", forkState.ParentSnapshotID, snapshotResult.SnapshotID)
	}
	if forkState.WorkspaceSHA256 != state.Snapshot(snapshotResult.SnapshotID).WorkspaceSHA256 {
		t.Fatalf("fork workspace hash does not match snapshot")
	}
}

func TestStoreRejectsStaleAttachedWriterInFencedMode(t *testing.T) {
	t.Parallel()
	store := createStore(t, ModeFenced)
	if err := store.SetAuthority(context.Background(), Authority{Generation: 2, Capability: "owner-2"}); err != nil {
		t.Fatalf("SetAuthority() error = %v", err)
	}
	instance := apply(t, store, Request{Kind: OperationStart, OperationID: "start-1", PhysicalAttemptID: "start-1"})
	_, err := store.Apply(context.Background(), Request{
		Kind: OperationCommand, OperationID: "stale-command", PhysicalAttemptID: "stale-command-1",
		InstanceID: instance.InstanceID, LogicalEffectID: "effect-stale", Payload: "stale",
		Generation: 1, Capability: "owner-1",
	})
	if err == nil {
		t.Fatal("Apply() error = nil, want stale authority rejection")
	}
	state, snapshotErr := store.Snapshot(context.Background())
	if snapshotErr != nil {
		t.Fatalf("Snapshot() error = %v", snapshotErr)
	}
	if got := state.Instance(instance.InstanceID).Effects; len(got) != 0 {
		t.Fatalf("stale effects = %v, want none", got)
	}
}

func TestStoreAuthorityCannotBeDowngradedOrRebound(t *testing.T) {
	t.Parallel()
	store := createStore(t, ModeFenced)
	if err := store.SetAuthority(context.Background(), Authority{Generation: 2, Capability: "owner-2"}); err != nil {
		t.Fatalf("SetAuthority(current) error = %v", err)
	}
	if err := store.SetAuthority(context.Background(), Authority{Generation: 1, Capability: "owner-1"}); !errors.Is(err, ErrStaleAuthority) {
		t.Fatalf("SetAuthority(downgrade) error = %v, want ErrStaleAuthority", err)
	}
	if err := store.SetAuthority(context.Background(), Authority{Generation: 2, Capability: "different-owner"}); !errors.Is(err, ErrStaleAuthority) {
		t.Fatalf("SetAuthority(rebind) error = %v, want ErrStaleAuthority", err)
	}
	if err := store.SetAuthority(context.Background(), Authority{Generation: 2, Capability: "owner-2"}); err != nil {
		t.Fatalf("SetAuthority(idempotent) error = %v", err)
	}
	state, err := store.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if state.Authority.Generation != 2 || state.Authority.CapabilitySHA256 != hashString("owner-2") {
		t.Fatalf("authority after rejected changes = %+v", state.Authority)
	}
}

func createStore(t *testing.T, mode Mode) *Store {
	t.Helper()
	store, err := Create(filepath.Join(t.TempDir(), "provider.db"), mode)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	return store
}

func apply(t *testing.T, store *Store, request Request) Result {
	t.Helper()
	result, err := store.Apply(context.Background(), request)
	if err != nil {
		t.Fatalf("Apply(%s) error = %v", request.Kind, err)
	}
	return result
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func equalBools(left, right []bool) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
