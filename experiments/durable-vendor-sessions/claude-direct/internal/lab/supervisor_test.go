package lab

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/sjarmak/temporal_projects/internal/workstore"
)

func testTurnSupervisorConcurrentCallersLaunchOnce(t *testing.T) {
	store := openSupervisorTestStore(t)
	started := make(chan struct{})
	release := make(chan struct{})
	decisions := make(chan supervisorDecision, 2)
	var runs atomic.Int32
	supervisor := newTurnSupervisor(context.Background(), store,
		func(ctx context.Context, store *workstore.Store, lease workstore.Lease) (supervisedResult, error) {
			runs.Add(1)
			if err := store.RegisterProcess(ctx, lease, supervisorTestProcess(lease.Generation)); err != nil {
				return supervisedResult{}, err
			}
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return supervisedResult{}, ctx.Err()
			}
			if err := store.CommitEffect(ctx, lease, workstore.Effect{ID: "effect-1", Value: "one"}); err != nil {
				return supervisedResult{}, err
			}
			return supervisedResult{
				VendorSessionID: "vendor-session-1",
				Outcome:         workstore.Outcome{Value: "EFFECT_COMPLETE"},
			}, nil
		}, sequentialCapabilities(), withSupervisorDecisionObserver(func(decision supervisorDecision) {
			decisions <- decision
		}))

	requests := []supervisorStartRequest{
		{SessionID: "session-1", WorkerID: "worker-1", Attempt: 1},
		{SessionID: "session-1", WorkerID: "worker-2", Attempt: 2},
	}
	type response struct {
		receipt supervisorReceipt
		err     error
	}
	responses := make(chan response, len(requests))
	go func() {
		receipt, err := supervisor.StartOrAttach(context.Background(), requests[0])
		responses <- response{receipt: receipt, err: err}
	}()
	if decision := <-decisions; decision.Action != workstore.ActionLaunch {
		t.Fatalf("first decision = %+v, want launch", decision)
	}
	<-started
	go func() {
		receipt, err := supervisor.StartOrAttach(context.Background(), requests[1])
		responses <- response{receipt: receipt, err: err}
	}()
	if decision := <-decisions; decision.Action != workstore.ActionAttach {
		t.Fatalf("second decision = %+v, want attach", decision)
	}
	close(release)

	actions := map[workstore.Action]int{}
	var first supervisorReceipt
	for range requests {
		response := <-responses
		if response.err != nil {
			t.Fatalf("start or attach: %v", response.err)
		}
		actions[response.receipt.Action]++
		if first == (supervisorReceipt{}) {
			first = response.receipt
			continue
		}
		if response.receipt.Generation != first.Generation ||
			response.receipt.OwnerTokenHash != first.OwnerTokenHash ||
			response.receipt.VendorSessionID != first.VendorSessionID ||
			response.receipt.Outcome != first.Outcome {
			t.Fatalf("receipts do not identify one execution: %+v / %+v", first, response.receipt)
		}
	}
	if runs.Load() != 1 || actions[workstore.ActionLaunch] != 1 || actions[workstore.ActionAttach] != 1 {
		t.Fatalf("runs/actions = %d/%+v, want one launch and one attach", runs.Load(), actions)
	}
	if first.Generation != 1 || first.OwnerTokenHash != workstore.HashToken("capability-1") || first.OwnerTokenHash == "capability-1" {
		t.Fatalf("receipt authority = %+v, want generation 1 and only the capability hash", first)
	}
}

func TestTurnSupervisorCallerLossDoesNotCancelOwnedExecution(t *testing.T) {
	store := openSupervisorTestStore(t)
	started := make(chan struct{})
	release := make(chan struct{})
	decisions := make(chan supervisorDecision, 2)
	supervisor := newTurnSupervisor(context.Background(), store,
		func(ctx context.Context, store *workstore.Store, lease workstore.Lease) (supervisedResult, error) {
			if err := store.RegisterProcess(ctx, lease, supervisorTestProcess(lease.Generation)); err != nil {
				return supervisedResult{}, err
			}
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return supervisedResult{}, fmt.Errorf("supervisor-owned execution canceled with caller: %w", ctx.Err())
			}
			if err := store.CommitEffect(ctx, lease, workstore.Effect{ID: "effect-1", Value: "one"}); err != nil {
				return supervisedResult{}, err
			}
			return supervisedResult{VendorSessionID: "vendor-session-1", Outcome: workstore.Outcome{Value: "done"}}, nil
		}, sequentialCapabilities(), withSupervisorDecisionObserver(func(decision supervisorDecision) {
			decisions <- decision
		}))

	callerCtx, cancelCaller := context.WithCancel(context.Background())
	firstResult := make(chan error, 1)
	go func() {
		_, err := supervisor.StartOrAttach(callerCtx, supervisorStartRequest{
			SessionID: "session-1", WorkerID: "worker-1", Attempt: 1,
		})
		firstResult <- err
	}()
	if decision := <-decisions; decision.Action != workstore.ActionLaunch {
		t.Fatalf("first decision = %+v, want launch", decision)
	}
	<-started
	cancelCaller()
	if err := <-firstResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("lost caller result = %v, want context.Canceled", err)
	}

	attached := make(chan struct {
		receipt supervisorReceipt
		err     error
	}, 1)
	go func() {
		receipt, err := supervisor.StartOrAttach(context.Background(), supervisorStartRequest{
			SessionID: "session-1", WorkerID: "worker-2", Attempt: 2,
		})
		attached <- struct {
			receipt supervisorReceipt
			err     error
		}{receipt: receipt, err: err}
	}()
	if decision := <-decisions; decision.Action != workstore.ActionAttach {
		t.Fatalf("recovery decision = %+v, want attach", decision)
	}
	close(release)
	result := <-attached
	if result.err != nil || result.receipt.Action != workstore.ActionAttach || result.receipt.Outcome.Value != "done" {
		t.Fatalf("reattached result = %+v, err = %v", result.receipt, result.err)
	}
}

func TestTurnSupervisorRejectsInvalidAndUnavailableStarts(t *testing.T) {
	store := openSupervisorTestStore(t)
	runner := func(context.Context, *workstore.Store, workstore.Lease) (supervisedResult, error) {
		return supervisedResult{}, errors.New("must not run")
	}
	request := supervisorStartRequest{SessionID: "session-1", WorkerID: "worker-1", Attempt: 1}

	var nilSupervisor *turnSupervisor
	if _, err := nilSupervisor.StartOrAttach(context.Background(), request); !errors.Is(err, workstore.ErrInvalidRequest) {
		t.Fatalf("nil supervisor = %v, want ErrInvalidRequest", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := newTurnSupervisor(context.Background(), store, runner, sequentialCapabilities()).StartOrAttach(canceled, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled request = %v, want context.Canceled", err)
	}
	validator := newTurnSupervisor(context.Background(), store, runner, sequentialCapabilities(),
		withSupervisorStartValidator(func(supervisorStartRequest) error { return errors.New("wrong turn") }))
	if _, err := validator.StartOrAttach(context.Background(), request); !errors.Is(err, workstore.ErrInvalidRequest) {
		t.Fatalf("validator error = %v, want ErrInvalidRequest", err)
	}
	capabilityFailure := newTurnSupervisor(context.Background(), store, runner,
		func() (string, error) { return "", errors.New("entropy unavailable") })
	if _, err := capabilityFailure.StartOrAttach(context.Background(), request); err == nil || !strings.Contains(err.Error(), "entropy unavailable") {
		t.Fatalf("capability failure = %v", err)
	}
	emptyCapability := newTurnSupervisor(context.Background(), store, runner, func() (string, error) { return "", nil })
	if _, err := emptyCapability.StartOrAttach(context.Background(), request); !errors.Is(err, workstore.ErrInvalidRequest) {
		t.Fatalf("empty capability = %v, want ErrInvalidRequest", err)
	}

	decision, err := store.StartOrAttach(context.Background(), workstore.StartRequest{
		SessionID: "untracked", Mode: workstore.ModeFenced, CandidateOwner: "outside-capability",
		WorkerID: "outside-worker", Attempt: 1,
	})
	if err != nil {
		t.Fatalf("create outside authority: %v", err)
	}
	if _, err := newTurnSupervisor(context.Background(), store, runner, sequentialCapabilities()).StartOrAttach(
		context.Background(), supervisorStartRequest{SessionID: "untracked", WorkerID: "worker-2", Attempt: 2},
	); !errors.Is(err, errSupervisorExecutionUnavailable) {
		t.Fatalf("untracked attach for generation %d = %v, want execution unavailable", decision.Lease.Generation, err)
	}
}

func TestFencedClaudeAuthorityErrorClassifiesRevokedAndStaleRuns(t *testing.T) {
	runErr := errors.New("run ended")
	config := fencedClaudeRunConfig{}
	store := openSupervisorTestStore(t)
	current := claimSupervisorTestPendingGeneration(t, store, "current", "worker-1", "current-capability", 1)
	if err := config.authorityError(context.Background(), store, current, runErr); !errors.Is(err, runErr) {
		t.Fatalf("live authority error = %v, want run error", err)
	}
	canceledContext, cancel := context.WithCancel(context.Background())
	cancel()
	if err := config.authorityError(canceledContext, store, workstore.Lease{SessionID: "missing"}, runErr); !errors.Is(err, workstore.ErrSessionNotFound) {
		t.Fatalf("missing authority error = %v, want ErrSessionNotFound", err)
	}
	if _, err := store.CancelSession(context.Background(), workstore.CancelRequest{SessionID: current.SessionID, RequestID: "cancel-1"}); err != nil {
		t.Fatalf("cancel current authority: %v", err)
	}
	if err := config.authorityError(canceledContext, store, current, runErr); !errors.Is(err, workstore.ErrSessionCanceled) || !errors.Is(err, runErr) {
		t.Fatalf("canceled authority error = %v", err)
	}

	stale := claimSupervisorTestGeneration(t, store, "replaced", "worker-a", "old-capability", 1, false)
	claimSupervisorTestGeneration(t, store, "replaced", "worker-b", "new-capability", 2, true)
	if err := config.authorityError(canceledContext, store, stale, runErr); !errors.Is(err, workstore.ErrStaleOwner) || !errors.Is(err, runErr) {
		t.Fatalf("stale authority error = %v", err)
	}
}

func testTurnSupervisorReplacementFencesOldEffectAndCompletion(t *testing.T) {
	store := openSupervisorTestStore(t)
	oldAtBoundary := make(chan struct{})
	oldEffect := make(chan error, 1)
	supervisor := newTurnSupervisor(context.Background(), store,
		func(ctx context.Context, store *workstore.Store, lease workstore.Lease) (supervisedResult, error) {
			if err := store.RegisterProcess(ctx, lease, supervisorTestProcess(lease.Generation)); err != nil {
				return supervisedResult{}, err
			}
			if lease.Generation == 1 {
				close(oldAtBoundary)
				<-ctx.Done()
				snapshot, err := store.Snapshot(context.Background(), lease.SessionID)
				if err != nil {
					return supervisedResult{}, err
				}
				if snapshot.ActiveGeneration != 2 {
					return supervisedResult{}, fmt.Errorf("old execution signaled before generation 2 committed: %+v", snapshot)
				}
				err = store.CommitEffect(context.Background(), lease, workstore.Effect{ID: "effect-1", Value: "old"})
				oldEffect <- err
				if err != nil {
					return supervisedResult{}, err
				}
				return supervisedResult{VendorSessionID: "vendor-old", Outcome: workstore.Outcome{Value: "old"}}, nil
			}
			if err := store.CommitEffect(ctx, lease, workstore.Effect{ID: "effect-1", Value: "replacement"}); err != nil {
				return supervisedResult{}, err
			}
			return supervisedResult{VendorSessionID: "vendor-replacement", Outcome: workstore.Outcome{Value: "replacement"}}, nil
		}, sequentialCapabilities())

	first := make(chan error, 1)
	go func() {
		_, err := supervisor.StartOrAttach(context.Background(), supervisorStartRequest{
			SessionID: "session-1", WorkerID: "worker-1", Attempt: 1,
		})
		first <- err
	}()
	<-oldAtBoundary
	replacement, err := supervisor.StartOrAttach(context.Background(), supervisorStartRequest{
		SessionID: "session-1", WorkerID: "worker-2", Attempt: 2, Replace: true,
	})
	if err != nil {
		t.Fatalf("replace owner: %v", err)
	}
	if replacement.Action != workstore.ActionLaunch || replacement.Generation != 2 ||
		replacement.OwnerTokenHash != workstore.HashToken("capability-2") || replacement.Outcome.Value != "replacement" {
		t.Fatalf("replacement receipt = %+v", replacement)
	}
	if err := <-oldEffect; !errors.Is(err, workstore.ErrStaleOwner) {
		t.Fatalf("old effect = %v, want ErrStaleOwner", err)
	}
	if err := <-first; !errors.Is(err, workstore.ErrStaleOwner) {
		t.Fatalf("old supervised result = %v, want ErrStaleOwner", err)
	}

	snapshot, err := store.Snapshot(context.Background(), "session-1")
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snapshot.ActiveGeneration != 2 || len(snapshot.Effects) != 1 ||
		snapshot.Effects[0].Generation != 2 || snapshot.Outcome == nil || snapshot.Outcome.Value != "replacement" {
		t.Fatalf("replacement authority snapshot = %+v", snapshot)
	}
}

func testTurnSupervisorRedeliveryReplacesFailedExecutionWithoutManualFlag(t *testing.T) {
	store := openSupervisorTestStore(t)
	failed := errors.New("generation one exited")
	var runs atomic.Int32
	supervisor := newTurnSupervisor(context.Background(), store,
		func(ctx context.Context, store *workstore.Store, lease workstore.Lease) (supervisedResult, error) {
			runs.Add(1)
			if err := store.RegisterProcess(ctx, lease, supervisorTestProcess(lease.Generation)); err != nil {
				return supervisedResult{}, err
			}
			if lease.Generation == 1 {
				return supervisedResult{}, failed
			}
			if err := store.CommitEffect(ctx, lease, workstore.Effect{ID: "effect-1", Value: "replacement"}); err != nil {
				return supervisedResult{}, err
			}
			return supervisedResult{
				VendorSessionID: "vendor-replacement", Outcome: workstore.Outcome{Value: "replacement"},
			}, nil
		}, sequentialCapabilities())

	if _, err := supervisor.StartOrAttach(context.Background(), supervisorStartRequest{
		SessionID: "session-1", WorkerID: "worker-1", Attempt: 1,
	}); !errors.Is(err, failed) {
		t.Fatalf("generation one = %v, want failure", err)
	}
	replacement, err := supervisor.StartOrAttach(context.Background(), supervisorStartRequest{
		SessionID: "session-1", WorkerID: "worker-2", Attempt: 2,
	})
	if err != nil {
		t.Fatalf("automatic replacement: %v", err)
	}
	if replacement.Action != workstore.ActionLaunch || replacement.Generation != 2 ||
		replacement.OwnerTokenHash != workstore.HashToken("capability-2") || replacement.Outcome.Value != "replacement" {
		t.Fatalf("automatic replacement = %+v", replacement)
	}
	if err := store.CommitEffect(context.Background(), workstore.Lease{
		SessionID: "session-1", Generation: 1, OwnerToken: "capability-1",
	}, workstore.Effect{ID: "late", Value: "late"}); !errors.Is(err, workstore.ErrStaleOwner) {
		t.Fatalf("generation-one late effect = %v, want ErrStaleOwner", err)
	}
	if runs.Load() != 2 {
		t.Fatalf("supervised executions = %d, want 2", runs.Load())
	}
}

func testTurnSupervisorCancellationRevokesBeforeSignalingExecution(t *testing.T) {
	store := openSupervisorTestStore(t)
	started := make(chan struct{})
	lateEffect := make(chan error, 1)
	supervisor := newTurnSupervisor(context.Background(), store,
		func(ctx context.Context, store *workstore.Store, lease workstore.Lease) (supervisedResult, error) {
			if err := store.RegisterProcess(ctx, lease, supervisorTestProcess(lease.Generation)); err != nil {
				return supervisedResult{}, err
			}
			close(started)
			<-ctx.Done()
			snapshot, err := store.Snapshot(context.Background(), lease.SessionID)
			if err != nil {
				return supervisedResult{}, err
			}
			if snapshot.Cancellation == nil {
				return supervisedResult{}, errors.New("execution was signaled before durable revocation")
			}
			err = store.CommitEffect(context.Background(), lease, workstore.Effect{ID: "late", Value: "late"})
			lateEffect <- err
			return supervisedResult{}, err
		}, sequentialCapabilities())

	result := make(chan error, 1)
	go func() {
		_, err := supervisor.StartOrAttach(context.Background(), supervisorStartRequest{
			SessionID: "session-1", WorkerID: "worker-1", Attempt: 1,
		})
		result <- err
	}()
	<-started
	decision, err := supervisor.Cancel(context.Background(), "session-1", "cancel-1")
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if decision.Action != workstore.CancelActionCommitted || decision.Cancellation == nil {
		t.Fatalf("cancel decision = %+v", decision)
	}
	if err := <-lateEffect; !errors.Is(err, workstore.ErrSessionCanceled) {
		t.Fatalf("late effect = %v, want ErrSessionCanceled", err)
	}
	if err := <-result; !errors.Is(err, workstore.ErrSessionCanceled) {
		t.Fatalf("supervised result = %v, want ErrSessionCanceled", err)
	}
	snapshot, err := store.Snapshot(context.Background(), "session-1")
	if err != nil {
		t.Fatalf("cancellation snapshot: %v", err)
	}
	if snapshot.Cancellation == nil || snapshot.Cancellation.Acknowledgement == nil ||
		snapshot.Cancellation.Acknowledgement.Process != supervisorTestProcess(1) {
		t.Fatalf("cancellation acknowledgement = %+v", snapshot.Cancellation)
	}
}

func openSupervisorTestStore(t *testing.T) *workstore.Store {
	t.Helper()
	store, err := workstore.Open(t.TempDir() + "/authority.db")
	if err != nil {
		t.Fatalf("open authority store: %v", err)
	}
	return store
}

func supervisorTestProcess(generation uint64) workstore.Process {
	return workstore.Process{
		PID: int(100 + generation), StartIdentity: fmt.Sprintf("boot:%d", generation), ProcessGroupID: int(100 + generation),
	}
}

func sequentialCapabilities() capabilityGenerator {
	var sequence atomic.Uint64
	return func() (string, error) {
		return fmt.Sprintf("capability-%d", sequence.Add(1)), nil
	}
}
