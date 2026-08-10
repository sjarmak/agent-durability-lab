package semantics

import (
	"context"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
	"github.com/sjarmak/temporal_projects/internal/workstore"
)

func TestOldEffectBoundaryIsStableAcrossActivityRetries(t *testing.T) {
	runtime := &EpisodeRuntime{oldEffectBoundaryReady: make(chan struct{})}
	want := oldEffectBoundary{
		identity: protocol.Identity{
			WorkItemID: "item-001", Generation: 1, CapabilityHash: workstore.HashToken("owner-1"),
			ProcessIdentity: "agent/pid-101",
		},
		barrierEventID: "event-000010",
	}
	if err := runtime.recordOldEffectBoundary(want); err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= 2; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		got, err := runtime.waitOldEffectBoundary(ctx)
		cancel()
		if err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
		if got != want {
			t.Fatalf("attempt %d boundary = %+v, want %+v", attempt, got, want)
		}
	}
	conflict := want
	conflict.identity.ProcessIdentity = "agent/pid-202"
	if err := runtime.recordOldEffectBoundary(conflict); err == nil {
		t.Fatal("conflicting retried supersession boundary was accepted")
	}
}

func TestEnsureOldEffectBoundaryRejectsConflictingAttachedRetry(t *testing.T) {
	runtime := &EpisodeRuntime{oldEffectBoundaryReady: make(chan struct{})}
	want := oldEffectBoundary{
		identity: protocol.Identity{
			WorkItemID: "item-001", Generation: 1, CapabilityHash: workstore.HashToken("owner-1"),
			ProcessIdentity: "agent/pid-101",
		},
		barrierEventID: "event-000010",
	}
	if err := runtime.recordOldEffectBoundary(want); err != nil {
		t.Fatal(err)
	}
	conflict := want.identity
	conflict.ProcessIdentity = "agent/pid-202"
	if err := runtime.ensureOldEffectBoundary(WorkInput{}, conflict); err == nil {
		t.Fatal("conflicting attached retry reused the cached supersession boundary")
	}
}
