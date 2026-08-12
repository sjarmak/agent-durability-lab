package lab

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/internal/workstore"
)

func TestSupervisorCallerLossReattachesToOneOwnedExecution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store := openCodexSupervisorStore(t)
	started := make(chan struct{})
	release := make(chan struct{})
	decisions := make(chan supervisorDecision, 2)
	var runs atomic.Int32
	supervisor := newTurnSupervisor(ctx, store,
		func(runContext context.Context, store *workstore.Store, lease workstore.Lease) (supervisedResult, error) {
			runs.Add(1)
			if err := store.RegisterProcess(runContext, lease, codexSupervisorTestProcess(lease.Generation)); err != nil {
				return supervisedResult{}, err
			}
			close(started)
			select {
			case <-release:
			case <-runContext.Done():
				return supervisedResult{}, runContext.Err()
			}
			return supervisedResult{ThreadID: testThreadID, Outcome: workstore.Outcome{Value: "EFFECT_COMPLETE"}}, nil
		}, sequentialCapabilities(), withSupervisorDecisionObserver(func(decision supervisorDecision) {
			decisions <- decision
		}))
	caller, cancelCaller := context.WithCancel(ctx)
	first := make(chan error, 1)
	go func() {
		_, err := supervisor.StartOrAttach(caller, supervisorStartRequest{SessionID: "session-1", WorkerID: "worker-1", Attempt: 1})
		first <- err
	}()
	if decision := <-decisions; decision.Action != workstore.ActionLaunch {
		t.Fatalf("first decision = %+v", decision)
	}
	<-started
	cancelCaller()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("caller loss = %v", err)
	}
	attached := make(chan supervisorReceipt, 1)
	attachedErr := make(chan error, 1)
	go func() {
		receipt, err := supervisor.StartOrAttach(ctx, supervisorStartRequest{SessionID: "session-1", WorkerID: "worker-2", Attempt: 2})
		attached <- receipt
		attachedErr <- err
	}()
	if decision := <-decisions; decision.Action != workstore.ActionAttach || decision.Generation != 1 {
		t.Fatalf("recovery decision = %+v", decision)
	}
	close(release)
	receipt := <-attached
	if err := <-attachedErr; err != nil || receipt.Action != workstore.ActionAttach ||
		receipt.ThreadID != testThreadID || runs.Load() != 1 {
		t.Fatalf("attachment = %+v runs=%d err=%v", receipt, runs.Load(), err)
	}
}

func TestSupervisorCommitsReplacementBeforeStoppingStaleGeneration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store := openCodexSupervisorStore(t)
	oldStarted := make(chan struct{})
	oldEffect := make(chan error, 1)
	supervisor := newTurnSupervisor(ctx, store,
		func(runContext context.Context, store *workstore.Store, lease workstore.Lease) (supervisedResult, error) {
			if err := store.RegisterProcess(runContext, lease, codexSupervisorTestProcess(lease.Generation)); err != nil {
				return supervisedResult{}, err
			}
			if lease.Generation == 1 {
				close(oldStarted)
				<-runContext.Done()
				snapshot, err := store.Snapshot(ctx, lease.SessionID)
				if err != nil || snapshot.ActiveGeneration != 2 {
					return supervisedResult{}, fmt.Errorf("old generation stopped before replacement commit: %+v: %w", snapshot, err)
				}
				err = store.CommitEffect(ctx, lease, workstore.Effect{ID: "effect-1", Value: "stale"})
				oldEffect <- err
				return supervisedResult{}, err
			}
			if err := store.CommitEffect(runContext, lease, workstore.Effect{ID: "effect-1", Value: "current"}); err != nil {
				return supervisedResult{}, err
			}
			return supervisedResult{ThreadID: testThreadID, Outcome: workstore.Outcome{Value: "EFFECT_COMPLETE"}}, nil
		}, sequentialCapabilities())
	first := make(chan error, 1)
	go func() {
		_, err := supervisor.StartOrAttach(ctx, supervisorStartRequest{SessionID: "session-1", WorkerID: "worker-1", Attempt: 1})
		first <- err
	}()
	<-oldStarted
	replacement, err := supervisor.StartOrAttach(ctx, supervisorStartRequest{
		SessionID: "session-1", WorkerID: "worker-2", Attempt: 2, Replace: true,
	})
	if err != nil || replacement.Generation != 2 || replacement.Action != workstore.ActionLaunch {
		t.Fatalf("replacement = %+v err=%v", replacement, err)
	}
	if err := <-oldEffect; !errors.Is(err, workstore.ErrStaleOwner) {
		t.Fatalf("stale effect = %v", err)
	}
	if err := <-first; !errors.Is(err, workstore.ErrStaleOwner) {
		t.Fatalf("stale execution = %v", err)
	}
}

func TestSupervisorRevokesBeforeCancellationSignal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store := openCodexSupervisorStore(t)
	started := make(chan struct{})
	lateEffect := make(chan error, 1)
	supervisor := newTurnSupervisor(ctx, store,
		func(runContext context.Context, store *workstore.Store, lease workstore.Lease) (supervisedResult, error) {
			if err := store.RegisterProcess(runContext, lease, codexSupervisorTestProcess(lease.Generation)); err != nil {
				return supervisedResult{}, err
			}
			close(started)
			<-runContext.Done()
			snapshot, err := store.Snapshot(ctx, lease.SessionID)
			if err != nil || snapshot.Cancellation == nil {
				return supervisedResult{}, errors.New("execution signaled before durable revocation")
			}
			err = store.CommitEffect(ctx, lease, workstore.Effect{ID: "late", Value: "late"})
			lateEffect <- err
			return supervisedResult{}, err
		}, sequentialCapabilities())
	finished := make(chan error, 1)
	go func() {
		_, err := supervisor.StartOrAttach(ctx, supervisorStartRequest{SessionID: "session-1", WorkerID: "worker-1", Attempt: 1})
		finished <- err
	}()
	<-started
	decision, err := supervisor.Cancel(ctx, "session-1", "cancel-1")
	if err != nil || decision.Action != workstore.CancelActionCommitted {
		t.Fatalf("cancel = %+v err=%v", decision, err)
	}
	if err := <-lateEffect; !errors.Is(err, workstore.ErrSessionCanceled) {
		t.Fatalf("late effect = %v", err)
	}
	if err := <-finished; !errors.Is(err, workstore.ErrSessionCanceled) {
		t.Fatalf("execution = %v", err)
	}
}

func TestTrialCleanupWaitsForCanceledSupervisorExecution(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := openCodexSupervisorStore(t)
	started := make(chan struct{})
	stopping := make(chan struct{})
	release := make(chan struct{})
	supervisor := newTurnSupervisor(ctx, store,
		func(runContext context.Context, _ *workstore.Store, _ workstore.Lease) (supervisedResult, error) {
			close(started)
			<-runContext.Done()
			close(stopping)
			<-release
			return supervisedResult{}, runContext.Err()
		}, sequentialCapabilities())
	requestDone := make(chan error, 1)
	go func() {
		_, err := supervisor.StartOrAttach(context.Background(), supervisorStartRequest{
			SessionID: "session-1", WorkerID: "worker-1", Attempt: 1,
		})
		requestDone <- err
	}()
	<-started

	trial := codexTrial{supervisor: supervisor, supervisorCancel: cancel}
	cleanupDone := make(chan error, 1)
	go func() {
		var cleanupErr error
		trial.cleanup(&cleanupErr)
		cleanupDone <- cleanupErr
	}()
	<-stopping
	if _, err := supervisor.StartOrAttach(context.Background(), supervisorStartRequest{
		SessionID: "session-2", WorkerID: "worker-2", Attempt: 1,
	}); !errors.Is(err, errSupervisorExecutionUnavailable) {
		t.Fatalf("start during cleanup = %v", err)
	}
	select {
	case err := <-cleanupDone:
		t.Fatalf("cleanup returned before supervised execution stopped: %v", err)
	default:
	}
	close(release)
	if err := <-cleanupDone; err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if err := <-requestDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("supervised request = %v", err)
	}
}

func TestSupervisorWaitReportsUnexpectedExecutionFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := openCodexSupervisorStore(t)
	want := errors.New("supervised execution failed")
	supervisor := newTurnSupervisor(ctx, store,
		func(context.Context, *workstore.Store, workstore.Lease) (supervisedResult, error) {
			return supervisedResult{}, want
		}, sequentialCapabilities())
	requestDone := make(chan error, 1)
	go func() {
		_, err := supervisor.StartOrAttach(context.Background(), supervisorStartRequest{
			SessionID: "session-1", WorkerID: "worker-1", Attempt: 1,
		})
		requestDone <- err
	}()
	if err := <-requestDone; !errors.Is(err, want) {
		t.Fatalf("supervised request = %v", err)
	}
	cancel()
	if err := supervisor.Wait(context.Background()); !errors.Is(err, want) {
		t.Fatalf("wait = %v, want %v", err, want)
	}
}

func TestSupervisorWaitIgnoresRecoveredSupersededFailureButKeepsCleanupFailure(t *testing.T) {
	for _, test := range []struct {
		name       string
		firstError error
		wantError  error
	}{
		{name: "recovered process failure", firstError: errCodexStreamIncomplete},
		{
			name: "process control failure",
			firstError: errors.Join(errCodexStreamIncomplete,
				fmt.Errorf("%w: terminate process group", errSupervisedProcessControl)),
			wantError: errSupervisedProcessControl,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := openCodexSupervisorStore(t)
			supervisor := newTurnSupervisor(t.Context(), store,
				func(ctx context.Context, store *workstore.Store, lease workstore.Lease) (supervisedResult, error) {
					if err := store.RegisterProcess(ctx, lease, codexSupervisorTestProcess(lease.Generation)); err != nil {
						return supervisedResult{}, err
					}
					if lease.Generation == 1 {
						return supervisedResult{}, test.firstError
					}
					return supervisedResult{Outcome: workstore.Outcome{Value: "EFFECT_COMPLETE"}}, nil
				}, sequentialCapabilities())

			if _, err := supervisor.StartOrAttach(t.Context(), supervisorStartRequest{
				SessionID: "session-1", WorkerID: "worker-1", Attempt: 1,
			}); !errors.Is(err, test.firstError) {
				t.Fatalf("first execution = %v, want %v", err, test.firstError)
			}
			if receipt, err := supervisor.StartOrAttach(t.Context(), supervisorStartRequest{
				SessionID: "session-1", WorkerID: "worker-2", Attempt: 2,
			}); err != nil || receipt.Generation != 2 {
				t.Fatalf("replacement = %+v, %v", receipt, err)
			}
			err := supervisor.Wait(t.Context())
			if test.wantError == nil && err != nil {
				t.Fatalf("wait after recovered replacement = %v", err)
			}
			if test.wantError != nil && !errors.Is(err, test.wantError) {
				t.Fatalf("wait = %v, want %v", err, test.wantError)
			}
		})
	}
}

func TestSupervisorWaitDoesNotMaskControlFailureJoinedWithCancellation(t *testing.T) {
	generic := errors.New("unexpected parser failure")
	if err := unexpectedSupervisorExecutionError(errors.Join(context.Canceled, generic)); !errors.Is(err, generic) {
		t.Fatalf("unexpected generic error = %v", err)
	}
	nested := fmt.Errorf("outer: %w", errors.Join(context.Canceled,
		errors.Join(generic, fmt.Errorf("%w: terminate", errSupervisedProcessControl))))
	if err := unexpectedSupervisorExecutionError(nested); !errors.Is(err, generic) ||
		!errors.Is(err, errSupervisedProcessControl) {
		t.Fatalf("unexpected nested error = %v", err)
	}
	want := fmt.Errorf("%w: terminate process group", errSupervisedProcessControl)
	err := unexpectedSupervisorExecutionError(errors.Join(context.Canceled, want))
	if !errors.Is(err, errSupervisedProcessControl) {
		t.Fatalf("unexpected execution error = %v", err)
	}
	want = fmt.Errorf("%w: persist receipt", errSupervisorCancellationAcknowledgment)
	err = unexpectedSupervisorExecutionError(errors.Join(workstore.ErrSessionCanceled, want))
	if !errors.Is(err, errSupervisorCancellationAcknowledgment) {
		t.Fatalf("unexpected acknowledgment error = %v", err)
	}
	want = errors.Join(errSupervisedExecutionDidNotExit, errSupervisedTerminationUnverified)
	err = unexpectedSupervisorExecutionError(errors.Join(context.Canceled, want))
	if !errors.Is(err, errSupervisedExecutionDidNotExit) || !errors.Is(err, errSupervisedTerminationUnverified) {
		t.Fatalf("unexpected termination error = %v", err)
	}
}

func TestSupervisorDoesNotAcknowledgeUnverifiedCancellationTermination(t *testing.T) {
	ctx := context.Background()
	store := openCodexSupervisorStore(t)
	decision, err := store.StartOrAttach(ctx, workstore.StartRequest{
		SessionID: "session-1", Mode: workstore.ModeFenced, CandidateOwner: "owner-1",
		WorkerID: "worker-1", Attempt: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	process := codexSupervisorTestProcess(1)
	if err := store.RegisterProcess(ctx, decision.Lease, process); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CancelSession(ctx, workstore.CancelRequest{
		SessionID: "session-1", RequestID: "cancel-1",
	}); err != nil {
		t.Fatal(err)
	}
	supervisor := newTurnSupervisor(ctx, store,
		func(context.Context, *workstore.Store, workstore.Lease) (supervisedResult, error) {
			return supervisedResult{}, errors.Join(workstore.ErrSessionCanceled, errSupervisedTerminationUnverified)
		}, sequentialCapabilities())
	execution := &supervisedExecution{
		lease: decision.Lease, cancel: func() {}, done: make(chan struct{}),
	}
	supervisor.runExecution(ctx, execution)
	if !errors.Is(execution.err, errSupervisedTerminationUnverified) {
		t.Fatalf("execution error = %v", execution.err)
	}
	snapshot, err := store.Snapshot(ctx, "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Cancellation == nil || snapshot.Cancellation.Acknowledgement != nil {
		t.Fatalf("unverified termination was acknowledged: %+v", snapshot.Cancellation)
	}
}

func TestWaitForSupervisorExecutionsKeepsCompletedFailureOnTimeout(t *testing.T) {
	want := errors.New("completed generation failed")
	completed := &supervisedExecution{done: make(chan struct{}), err: want}
	close(completed.done)
	hung := &supervisedExecution{done: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := waitForSupervisorExecutions(ctx, []*supervisedExecution{hung, completed})
	if !errors.Is(err, want) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait = %v, want completed error and deadline", err)
	}
}

func openCodexSupervisorStore(t *testing.T) *workstore.Store {
	t.Helper()
	store, err := workstore.Open(t.TempDir() + "/authority.db")
	if err != nil {
		t.Fatalf("open authority store: %v", err)
	}
	return store
}

func codexSupervisorTestProcess(generation uint64) workstore.Process {
	return workstore.Process{PID: int(100 + generation), StartIdentity: fmt.Sprintf("boot:%d", generation), ProcessGroupID: int(100 + generation)}
}

func sequentialCapabilities() capabilityGenerator {
	var sequence atomic.Uint64
	return func() (string, error) { return fmt.Sprintf("capability-%d", sequence.Add(1)), nil }
}
