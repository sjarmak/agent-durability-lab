package semantics

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
	"github.com/sjarmak/temporal_projects/internal/workstore"
	"go.temporal.io/sdk/temporal"
)

type observedDoneContext struct {
	context.Context
	done     <-chan struct{}
	observed chan struct{}
	once     sync.Once
}

func (c *observedDoneContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.done
}

func (c *observedDoneContext) Err() error {
	select {
	case <-c.done:
		return context.Canceled
	default:
		return nil
	}
}

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

func TestPendingLaunchRetryAttachesToStableLease(t *testing.T) {
	wantLease := workstore.Lease{
		SessionID: "operation/item-001", Generation: 1, OwnerToken: "owner-1",
	}
	pending := &pendingLaunch{decision: workstore.Decision{Action: workstore.ActionLaunch, Lease: wantLease}}

	first := pending.claim()
	if first.Action != workstore.ActionLaunch || first.Lease != wantLease {
		t.Fatalf("first claim = %+v, want launch of stable lease %+v", first, wantLease)
	}
	second := pending.claim()
	if second.Action != workstore.ActionAttach || second.Lease != wantLease {
		t.Fatalf("retry claim = %+v, want attach to stable lease %+v", second, wantLease)
	}
	if pending.decision.Action != workstore.ActionLaunch {
		t.Fatalf("cached decision was mutated: %+v", pending.decision)
	}
}

func TestObsoleteProtectedWorkWaitsForParentCancellation(t *testing.T) {
	done := make(chan struct{})
	observed := make(chan struct{})
	ctx := &observedDoneContext{Context: context.Background(), done: done, observed: observed}
	input := WorkInput{
		Case: protocol.CaseQueuedExecutingSupersession, Probe: protocol.ProbeUnfaulted,
		Item: Item{ID: "item-001"},
	}
	result := make(chan error, 1)
	go func() { result <- awaitObsoleteCancellation(ctx, input) }()

	select {
	case <-observed:
	case <-time.After(time.Second):
		t.Fatal("obsolete work did not begin waiting for parent cancellation")
	}
	select {
	case err := <-result:
		t.Fatalf("obsolete work returned before parent cancellation: %v", err)
	default:
	}

	close(done)
	select {
	case err := <-result:
		if !temporal.IsCanceledError(err) {
			t.Fatalf("obsolete work returned %v, want Temporal cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("obsolete work did not return after parent cancellation")
	}
}

func TestObsoleteProtectedWorkPreservesDeadlineFailure(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Unix(0, 0))
	defer cancel()
	input := WorkInput{
		Case: protocol.CaseQueuedExecutingSupersession, Probe: protocol.ProbeProtected,
		Item: Item{ID: "item-001"},
	}

	err := awaitObsoleteCancellation(ctx, input)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("obsolete Work deadline returned %v, want context deadline exceeded", err)
	}
}
