package workstore

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestUnsafeRetryLaunchesCompetingWriter(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	first := mustStart(t, store, StartRequest{
		SessionID: "session-1", Mode: ModeUnsafe, CandidateOwner: "owner-1", WorkerID: "worker-1", Attempt: 1,
	})
	second := mustStart(t, store, StartRequest{
		SessionID: "session-1", Mode: ModeUnsafe, CandidateOwner: "owner-2", WorkerID: "worker-2", Attempt: 2,
	})

	if first.Action != ActionLaunch || second.Action != ActionLaunch {
		t.Fatalf("unsafe decisions = %q, %q; want two launches", first.Action, second.Action)
	}
	if first.Lease.OwnerToken == second.Lease.OwnerToken {
		t.Fatal("unsafe retry reused an owner token")
	}
	mustRegisterTestProcess(t, store, first.Lease, 101)
	mustRegisterTestProcess(t, store, second.Lease, 102)

	mustCommitEffect(t, store, first.Lease, "effect-1")
	mustCommitEffect(t, store, second.Lease, "effect-2")

	snapshot, err := store.Snapshot(ctx, "session-1")
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if got := len(snapshot.Executors); got != 2 {
		t.Fatalf("executors = %d; want 2", got)
	}
	if got := len(snapshot.Effects); got != 2 {
		t.Fatalf("effects = %d; want 2", got)
	}
}

func TestStableSessionReattachesRetry(t *testing.T) {
	store := openTestStore(t)

	first := mustStart(t, store, StartRequest{
		SessionID: "session-1", Mode: ModeReattach, CandidateOwner: "owner-1", WorkerID: "worker-1", Attempt: 1,
	})
	second := mustStart(t, store, StartRequest{
		SessionID: "session-1", Mode: ModeReattach, CandidateOwner: "owner-2", WorkerID: "worker-2", Attempt: 2,
	})

	if first.Action != ActionLaunch {
		t.Fatalf("first action = %q; want launch", first.Action)
	}
	if second.Action != ActionAttach {
		t.Fatalf("retry action = %q; want attach", second.Action)
	}
	if second.Lease != first.Lease {
		t.Fatalf("retry lease = %+v; want %+v", second.Lease, first.Lease)
	}
}

func TestPendingLaunchRecoveryUsesFencedReplacementButAttachesToRegisteredProcess(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	pending := mustStart(t, store, StartRequest{
		SessionID: "session-1", Mode: ModeFenced, CandidateOwner: "owner-1", WorkerID: "worker-1", Attempt: 1,
	})
	pendingSnapshot, err := store.Snapshot(ctx, "session-1")
	if err != nil {
		t.Fatalf("snapshot pending launch: %v", err)
	}
	if len(pendingSnapshot.Executors) != 1 || pendingSnapshot.Executors[0].Status != ExecutorStatusLaunchPending ||
		pendingSnapshot.Executors[0].PID != 0 || pendingSnapshot.Executors[0].ProcessStart != "" {
		t.Fatalf("pending executor = %+v; want unregistered launch_pending", pendingSnapshot.Executors)
	}

	replacement := mustStart(t, store, StartRequest{
		SessionID: "session-1", Mode: ModeFenced, CandidateOwner: "owner-2", WorkerID: "worker-2", Attempt: 2,
		ReplacePendingLaunch: true,
	})
	if replacement.Action != ActionLaunch || replacement.Lease.Generation != 2 {
		t.Fatalf("replacement = %+v; want generation 2 launch", replacement)
	}
	if err := store.RegisterProcess(ctx, pending.Lease, Process{PID: 41, StartIdentity: "boot:41"}); !errors.Is(err, ErrStaleOwner) {
		t.Fatalf("obsolete registration = %v; want ErrStaleOwner", err)
	}
	if err := store.RegisterProcess(ctx, replacement.Lease, Process{PID: 42, StartIdentity: "boot:42"}); err != nil {
		t.Fatalf("register replacement: %v", err)
	}

	reattach := mustStart(t, store, StartRequest{
		SessionID: "session-1", Mode: ModeFenced, CandidateOwner: "owner-3", WorkerID: "worker-3", Attempt: 3,
		ReplacePendingLaunch: true,
	})
	if reattach.Action != ActionAttach || reattach.Lease != replacement.Lease {
		t.Fatalf("registered retry = %+v; want attach to generation 2", reattach)
	}
	registeredSnapshot, err := store.Snapshot(ctx, "session-1")
	if err != nil {
		t.Fatalf("snapshot registered process: %v", err)
	}
	if len(registeredSnapshot.Executors) != 2 ||
		registeredSnapshot.Executors[0].Status != ExecutorStatusSuperseded ||
		registeredSnapshot.Executors[1].Status != ExecutorStatusRunning {
		t.Fatalf("executor lifecycle = %+v", registeredSnapshot.Executors)
	}
	assertEventKinds(t, registeredSnapshot.Events,
		"pending_launch_replaced", "process_registration_rejected_stale", "process_registered", "activity_reattached")
	for _, event := range registeredSnapshot.Events {
		if event.Kind == "process_registration_rejected_stale" {
			if event.Attempt != 1 || event.WorkerID != "worker-1" || event.PID != 41 || event.Details["process_start"] != "boot:41" {
				t.Fatalf("stale registration identity = %+v", event)
			}
			return
		}
	}
	t.Fatal("stale registration event not found")
}

func TestPendingLaunchRecoveryRejectsDelayedOlderAttempt(t *testing.T) {
	store := openTestStore(t)
	first := mustStart(t, store, StartRequest{
		SessionID: "session-1", Mode: ModeFenced, CandidateOwner: "owner-1", WorkerID: "worker-1", Attempt: 1,
	})
	newer := mustStart(t, store, StartRequest{
		SessionID: "session-1", Mode: ModeFenced, CandidateOwner: "owner-3", WorkerID: "worker-3", Attempt: 3,
		ReplacePendingLaunch: true,
	})
	if newer.Lease.Generation != first.Lease.Generation+1 {
		t.Fatalf("newer generation = %d; want %d", newer.Lease.Generation, first.Lease.Generation+1)
	}

	_, err := store.StartOrAttach(context.Background(), StartRequest{
		SessionID: "session-1", Mode: ModeFenced, CandidateOwner: "owner-2", WorkerID: "worker-2", Attempt: 2,
		ReplacePendingLaunch: true,
	})
	if !errors.Is(err, ErrStaleOwner) {
		t.Fatalf("delayed attempt 2 = %v; want ErrStaleOwner", err)
	}
	snapshot, err := store.Snapshot(context.Background(), "session-1")
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snapshot.ActiveGeneration != newer.Lease.Generation || len(snapshot.Executors) != 2 {
		t.Fatalf("delayed attempt changed ownership: %+v", snapshot)
	}
	assertEventKinds(t, snapshot.Events, "pending_launch_replacement_rejected_stale_attempt")
}

func TestPendingExecutorCannotMutateBeforeProcessRegistration(t *testing.T) {
	store := openTestStore(t)
	decision := mustStart(t, store, StartRequest{
		SessionID: "session-1", Mode: ModeFenced, CandidateOwner: "owner-1", WorkerID: "worker-1", Attempt: 1,
	})
	tests := []struct {
		name string
		run  func() error
	}{
		{name: "progress", run: func() error { return store.RecordProgress(context.Background(), decision.Lease, "started") }},
		{name: "effect", run: func() error {
			return store.CommitEffect(context.Background(), decision.Lease, Effect{ID: "effect", Value: "changed"})
		}},
		{name: "completion", run: func() error {
			return store.Complete(context.Background(), decision.Lease, Outcome{Value: "done"})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.run(); !errors.Is(err, ErrExecutorNotRunning) {
				t.Fatalf("pending %s = %v; want ErrExecutorNotRunning", test.name, err)
			}
		})
	}
	snapshot, err := store.Snapshot(context.Background(), "session-1")
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(snapshot.Effects) != 0 || snapshot.Outcome != nil {
		t.Fatalf("pending mutation changed state: effects=%+v outcome=%+v", snapshot.Effects, snapshot.Outcome)
	}
	assertEventKinds(t, snapshot.Events,
		"progress_rejected_not_running", "effect_rejected_not_running", "completion_rejected_not_running")
}

func TestCompletedSessionReturnsOutcomeWithoutLaunch(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	decision := mustStart(t, store, StartRequest{
		SessionID: "session-1", Mode: ModeReattach, CandidateOwner: "owner-1", WorkerID: "worker-1", Attempt: 1,
	})
	mustRegisterTestProcess(t, store, decision.Lease, 101)
	want := Outcome{Value: "accepted", ArtifactRef: "sha256:abc"}
	if err := store.Complete(ctx, decision.Lease, want); err != nil {
		t.Fatalf("complete: %v", err)
	}

	retry := mustStart(t, store, StartRequest{
		SessionID: "session-1", Mode: ModeReattach, CandidateOwner: "owner-2", WorkerID: "worker-2", Attempt: 2,
	})
	if retry.Action != ActionComplete {
		t.Fatalf("retry action = %q; want complete", retry.Action)
	}
	if retry.Outcome == nil || *retry.Outcome != want {
		t.Fatalf("retry outcome = %+v; want %+v", retry.Outcome, want)
	}
}

func TestFencedReplacementRejectsStaleWriterAndCompletion(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	old := mustStart(t, store, StartRequest{
		SessionID: "session-1", Mode: ModeFenced, CandidateOwner: "owner-1", WorkerID: "worker-1", Attempt: 1,
	})
	mustRegisterTestProcess(t, store, old.Lease, 101)
	replacement := mustStart(t, store, StartRequest{
		SessionID: "session-1", Mode: ModeFenced, CandidateOwner: "owner-2", WorkerID: "worker-2", Attempt: 2, ReplaceOwner: true,
	})

	if replacement.Action != ActionLaunch {
		t.Fatalf("replacement action = %q; want launch", replacement.Action)
	}
	if replacement.Lease.Generation != old.Lease.Generation+1 {
		t.Fatalf("replacement generation = %d; want %d", replacement.Lease.Generation, old.Lease.Generation+1)
	}
	if err := store.CommitEffect(ctx, old.Lease, Effect{ID: "stale-effect", Value: "stale"}); !errors.Is(err, ErrStaleOwner) {
		t.Fatalf("stale effect error = %v; want ErrStaleOwner", err)
	}
	if err := store.Complete(ctx, old.Lease, Outcome{Value: "stale"}); !errors.Is(err, ErrStaleOwner) {
		t.Fatalf("stale completion error = %v; want ErrStaleOwner", err)
	}
	if err := store.RecordProgress(ctx, old.Lease, "still-running"); !errors.Is(err, ErrStaleOwner) {
		t.Fatalf("stale progress error = %v; want ErrStaleOwner", err)
	}

	mustRegisterTestProcess(t, store, replacement.Lease, 102)
	mustCommitEffect(t, store, replacement.Lease, "replacement-effect")
	want := Outcome{Value: "replacement"}
	if err := store.Complete(ctx, replacement.Lease, want); err != nil {
		t.Fatalf("replacement completion: %v", err)
	}

	snapshot, err := store.Snapshot(ctx, "session-1")
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(snapshot.Effects) != 1 || snapshot.Effects[0].OwnerTokenHash != HashToken(replacement.Lease.OwnerToken) {
		t.Fatalf("effects = %+v; want only replacement owner", snapshot.Effects)
	}
	if snapshot.Outcome == nil || *snapshot.Outcome != want {
		t.Fatalf("outcome = %+v; want %+v", snapshot.Outcome, want)
	}
	assertEventKinds(t, snapshot.Events, "effect_rejected_stale", "completion_rejected_stale", "progress_rejected_stale", "outcome_accepted")
}

func TestConcurrentStartOrAttachHasOneLauncher(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	const contenders = 8

	decisions := make(chan Decision, contenders)
	errs := make(chan error, contenders)
	var wg sync.WaitGroup
	for i := range contenders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			decision, err := store.StartOrAttach(ctx, StartRequest{
				SessionID: "session-1", Mode: ModeReattach, CandidateOwner: fmt.Sprintf("owner-%d", i),
				WorkerID: fmt.Sprintf("worker-%d", i), Attempt: 1,
			})
			if err != nil {
				errs <- err
				return
			}
			decisions <- decision
		}()
	}
	wg.Wait()
	close(decisions)
	close(errs)
	for err := range errs {
		t.Fatalf("start or attach: %v", err)
	}

	launches := 0
	var lease Lease
	for decision := range decisions {
		if decision.Action == ActionLaunch {
			launches++
		}
		if lease == (Lease{}) {
			lease = decision.Lease
		}
		if decision.Lease != lease {
			t.Fatalf("decision lease = %+v; want %+v", decision.Lease, lease)
		}
	}
	if launches != 1 {
		t.Fatalf("launches = %d; want 1", launches)
	}
}

func TestCommitEffectOnceIsAtomicIdempotentAndRejectsConflict(t *testing.T) {
	store := openTestStore(t)
	decision, err := store.StartOrAttach(context.Background(), StartRequest{
		SessionID: "session-1", Mode: ModeFenced, CandidateOwner: "owner-1",
		WorkerID: "worker-1", Attempt: 1,
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := store.RegisterProcess(context.Background(), decision.Lease, Process{
		PID: 101, StartIdentity: "boot:101", ProcessGroupID: 101,
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	effect := Effect{ID: "effect-1", Value: "payload"}
	errorsByCall := make(chan error, 2)
	for range 2 {
		go func() { errorsByCall <- store.CommitEffectOnce(context.Background(), decision.Lease, effect) }()
	}
	for range 2 {
		if err := <-errorsByCall; err != nil {
			t.Fatalf("idempotent commit: %v", err)
		}
	}
	if err := store.CommitEffectOnce(context.Background(), decision.Lease, Effect{
		ID: effect.ID, Value: "conflict",
	}); !errors.Is(err, ErrEffectConflict) {
		t.Fatalf("conflicting commit = %v, want ErrEffectConflict", err)
	}
	snapshot, err := store.Snapshot(context.Background(), decision.Lease.SessionID)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(snapshot.Effects) != 1 || snapshot.Effects[0].Effect != effect {
		t.Fatalf("effects = %+v, want one %+v", snapshot.Effects, effect)
	}
	assertEventKinds(t, snapshot.Events, "effect_accepted", "effect_idempotently_observed", "effect_conflict_rejected")
}

func TestEvidenceExportIsOrderedJSONL(t *testing.T) {
	store := openTestStore(t)
	decision := mustStart(t, store, StartRequest{
		SessionID: "session-1", Mode: ModeFenced, CandidateOwner: "owner-1", WorkerID: "worker-1", Attempt: 1,
	})
	mustRegisterTestProcess(t, store, decision.Lease, 101)
	mustCommitEffect(t, store, decision.Lease, "effect-1")
	if err := store.Complete(context.Background(), decision.Lease, Outcome{Value: "done"}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	path := filepath.Join(t.TempDir(), "evidence.jsonl")
	if err := store.ExportJSONL(context.Background(), "session-1", path); err != nil {
		t.Fatalf("export evidence: %v", err)
	}

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open evidence: %v", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			t.Errorf("close evidence: %v", err)
		}
	}()

	var previous uint64
	count := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var event Event
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			t.Fatalf("decode evidence line %d: %v", count+1, err)
		}
		if event.Sequence <= previous {
			t.Fatalf("sequence %d follows %d", event.Sequence, previous)
		}
		previous = event.Sequence
		count++
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan evidence: %v", err)
	}
	if count < 3 {
		t.Fatalf("evidence lines = %d; want at least 3", count)
	}
}

func TestProcessProgressObservationAndCompletionConflicts(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	decision := mustStart(t, store, StartRequest{
		SessionID: "session-1", Mode: ModeFenced, CandidateOwner: "owner-1", WorkerID: "worker-1", Attempt: 1,
	})
	if err := store.RegisterProcess(ctx, decision.Lease, Process{PID: 42, StartIdentity: "boot:start"}); err != nil {
		t.Fatalf("register process: %v", err)
	}
	if err := store.RecordProgress(ctx, decision.Lease, "tool-running"); err != nil {
		t.Fatalf("record progress: %v", err)
	}
	if err := store.RecordObservation(ctx, Event{Kind: "operator_observed", SessionID: "session-1"}); err != nil {
		t.Fatalf("record observation: %v", err)
	}
	want := Outcome{Value: "accepted"}
	if err := store.Complete(ctx, decision.Lease, want); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if err := store.Complete(ctx, decision.Lease, want); err != nil {
		t.Fatalf("duplicate completion: %v", err)
	}
	if err := store.Complete(ctx, decision.Lease, Outcome{Value: "conflict"}); !errors.Is(err, ErrOutcomeAlreadyAccepted) {
		t.Fatalf("conflicting completion = %v; want ErrOutcomeAlreadyAccepted", err)
	}
	snapshot, err := store.Snapshot(ctx, "session-1")
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snapshot.Executors[0].PID != 42 || snapshot.Executors[0].ProcessStart != "boot:start" {
		t.Fatalf("executor = %+v", snapshot.Executors[0])
	}
	assertEventKinds(t, snapshot.Events, "process_registered", "agent_progress", "operator_observed", "completion_duplicate", "completion_rejected_terminal")
}

func TestTerminalRejectionClosesCompetingExecutorState(t *testing.T) {
	store := openTestStore(t)
	first := mustStart(t, store, StartRequest{
		SessionID: "session-1", Mode: ModeUnsafe, CandidateOwner: "owner-1", WorkerID: "worker-1", Attempt: 1,
	})
	second := mustStart(t, store, StartRequest{
		SessionID: "session-1", Mode: ModeUnsafe, CandidateOwner: "owner-2", WorkerID: "worker-2", Attempt: 2,
	})
	mustRegisterTestProcess(t, store, first.Lease, 101)
	mustRegisterTestProcess(t, store, second.Lease, 102)
	if err := store.Complete(context.Background(), first.Lease, Outcome{Value: "accepted"}); err != nil {
		t.Fatalf("first completion: %v", err)
	}
	if err := store.Complete(context.Background(), second.Lease, Outcome{Value: "rejected"}); !errors.Is(err, ErrOutcomeAlreadyAccepted) {
		t.Fatalf("second completion = %v; want ErrOutcomeAlreadyAccepted", err)
	}
	snapshot, err := store.Snapshot(context.Background(), "session-1")
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snapshot.Executors[1].Status != "terminal_rejected" {
		t.Fatalf("competing executor status = %q; want terminal_rejected", snapshot.Executors[1].Status)
	}
}

func TestCancellationCommitsTerminalRevocation(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	decision := mustStart(t, store, StartRequest{
		SessionID: "session-1", Mode: ModeFenced, CandidateOwner: "owner-1", WorkerID: "worker-1", Attempt: 1,
	})
	mustRegisterTestProcess(t, store, decision.Lease, 101)

	cancelDecision, err := store.CancelSession(ctx, CancelRequest{SessionID: "session-1", RequestID: "cancel-1"})
	if err != nil {
		t.Fatalf("cancel session: %v", err)
	}
	if cancelDecision.Action != CancelActionCommitted {
		t.Fatalf("cancel action = %q; want %q", cancelDecision.Action, CancelActionCommitted)
	}
	if cancelDecision.Cancellation == nil {
		t.Fatal("committed cancellation is nil")
	}
	if cancelDecision.Cancellation.RequestID != "cancel-1" ||
		cancelDecision.Cancellation.Generation != decision.Lease.Generation ||
		cancelDecision.Cancellation.OwnerTokenHash != HashToken(decision.Lease.OwnerToken) {
		t.Fatalf("cancellation = %+v; want active lease identity", cancelDecision.Cancellation)
	}

	mutations := []struct {
		name string
		run  func() error
	}{
		{name: "register", run: func() error {
			return store.RegisterProcess(ctx, decision.Lease, Process{PID: 102, StartIdentity: "boot:102"})
		}},
		{name: "progress", run: func() error { return store.RecordProgress(ctx, decision.Lease, "late") }},
		{name: "effect", run: func() error {
			return store.CommitEffect(ctx, decision.Lease, Effect{ID: "late", Value: "late"})
		}},
		{name: "completion", run: func() error {
			return store.Complete(ctx, decision.Lease, Outcome{Value: "late"})
		}},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			if err := mutation.run(); !errors.Is(err, ErrSessionCanceled) {
				t.Fatalf("post-cancel mutation = %v; want ErrSessionCanceled", err)
			}
		})
	}
	if _, err := store.StartOrAttach(ctx, StartRequest{
		SessionID: "session-1", Mode: ModeFenced, CandidateOwner: "owner-2", WorkerID: "worker-2", Attempt: 2,
		ReplaceOwner: true,
	}); !errors.Is(err, ErrSessionCanceled) {
		t.Fatalf("post-cancel replacement = %v; want ErrSessionCanceled", err)
	}

	snapshot, err := store.Snapshot(ctx, "session-1")
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snapshot.Cancellation == nil || snapshot.Cancellation.RequestID != "cancel-1" {
		t.Fatalf("snapshot cancellation = %+v", snapshot.Cancellation)
	}
	if snapshot.Executors[0].Status != ExecutorStatusCanceled {
		t.Fatalf("executor status = %q; want %q", snapshot.Executors[0].Status, ExecutorStatusCanceled)
	}
	if len(snapshot.Effects) != 0 || snapshot.Outcome != nil {
		t.Fatalf("post-cancel state changed: effects=%+v outcome=%+v", snapshot.Effects, snapshot.Outcome)
	}
	assertEventKinds(t, snapshot.Events, "cancellation_committed", "process_registration_rejected_canceled",
		"progress_rejected_canceled", "effect_rejected_canceled", "completion_rejected_canceled",
		"start_rejected_canceled")
}

func TestCancellationAndCompletionUseFirstCommittedTerminalTransition(t *testing.T) {
	t.Run("completion first", func(t *testing.T) {
		store := openTestStore(t)
		decision := mustStart(t, store, StartRequest{
			SessionID: "session-1", Mode: ModeFenced, CandidateOwner: "owner-1", WorkerID: "worker-1", Attempt: 1,
		})
		mustRegisterTestProcess(t, store, decision.Lease, 101)
		want := Outcome{Value: "accepted"}
		if err := store.Complete(context.Background(), decision.Lease, want); err != nil {
			t.Fatalf("complete: %v", err)
		}

		cancelDecision, err := store.CancelSession(context.Background(), CancelRequest{SessionID: "session-1", RequestID: "cancel-1"})
		if err != nil {
			t.Fatalf("cancel after completion: %v", err)
		}
		if cancelDecision.Action != CancelActionAlreadyCompleted || cancelDecision.Outcome == nil || *cancelDecision.Outcome != want {
			t.Fatalf("cancel decision = %+v; want existing outcome", cancelDecision)
		}
		snapshot, err := store.Snapshot(context.Background(), "session-1")
		if err != nil {
			t.Fatalf("snapshot: %v", err)
		}
		if snapshot.Cancellation != nil || snapshot.Outcome == nil || *snapshot.Outcome != want {
			t.Fatalf("terminal state = cancellation %+v outcome %+v", snapshot.Cancellation, snapshot.Outcome)
		}
		assertEventKinds(t, snapshot.Events, "cancellation_observed_completed")
	})

	t.Run("cancellation first", func(t *testing.T) {
		store := openTestStore(t)
		decision := mustStart(t, store, StartRequest{
			SessionID: "session-1", Mode: ModeFenced, CandidateOwner: "owner-1", WorkerID: "worker-1", Attempt: 1,
		})
		mustRegisterTestProcess(t, store, decision.Lease, 101)
		if _, err := store.CancelSession(context.Background(), CancelRequest{SessionID: "session-1", RequestID: "cancel-1"}); err != nil {
			t.Fatalf("cancel session: %v", err)
		}
		if err := store.Complete(context.Background(), decision.Lease, Outcome{Value: "late"}); !errors.Is(err, ErrSessionCanceled) {
			t.Fatalf("completion after cancel = %v; want ErrSessionCanceled", err)
		}
		snapshot, err := store.Snapshot(context.Background(), "session-1")
		if err != nil {
			t.Fatalf("snapshot: %v", err)
		}
		if snapshot.Cancellation == nil || snapshot.Outcome != nil {
			t.Fatalf("terminal state = cancellation %+v outcome %+v", snapshot.Cancellation, snapshot.Outcome)
		}
	})
}

func TestCommitEffectWithoutAuthorityIsAnExplicitPostCancellationNegativeControl(t *testing.T) {
	store := openTestStore(t)
	decision := mustStart(t, store, StartRequest{
		SessionID: "session-1", Mode: ModeFenced, CandidateOwner: "owner-1", WorkerID: "worker-1", Attempt: 1,
	})
	mustRegisterTestProcess(t, store, decision.Lease, 101)
	if _, err := store.CancelSession(context.Background(), CancelRequest{
		SessionID: "session-1", RequestID: "cancel-1",
	}); err != nil {
		t.Fatalf("cancel session: %v", err)
	}

	effect := Effect{ID: "late-effect", Value: "unsafe"}
	if err := store.CommitEffectWithoutAuthority(context.Background(), decision.Lease, effect); err != nil {
		t.Fatalf("commit negative-control effect: %v", err)
	}
	snapshot, err := store.Snapshot(context.Background(), "session-1")
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(snapshot.Effects) != 1 || snapshot.Effects[0].Effect != effect {
		t.Fatalf("effects = %+v, want explicit unsafe effect", snapshot.Effects)
	}
	assertEventKinds(t, snapshot.Events, "cancellation_committed", "effect_accepted_without_authority")
}

func TestCancellationIsIdempotent(t *testing.T) {
	store := openTestStore(t)
	decision := mustStart(t, store, StartRequest{
		SessionID: "session-1", Mode: ModeFenced, CandidateOwner: "owner-1", WorkerID: "worker-1", Attempt: 1,
	})
	mustRegisterTestProcess(t, store, decision.Lease, 101)
	first, err := store.CancelSession(context.Background(), CancelRequest{SessionID: "session-1", RequestID: "cancel-1"})
	if err != nil {
		t.Fatalf("first cancel: %v", err)
	}
	for _, requestID := range []string{"cancel-1", "cancel-2"} {
		repeated, err := store.CancelSession(context.Background(), CancelRequest{SessionID: "session-1", RequestID: requestID})
		if err != nil {
			t.Fatalf("repeat cancel %q: %v", requestID, err)
		}
		if repeated.Action != CancelActionAlreadyCanceled || repeated.Cancellation == nil ||
			*repeated.Cancellation != *first.Cancellation {
			t.Fatalf("repeat decision = %+v; want original cancellation %+v", repeated, first.Cancellation)
		}
	}
	snapshot, err := store.Snapshot(context.Background(), "session-1")
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	assertEventKinds(t, snapshot.Events, "cancellation_committed", "cancellation_observed_already_canceled")
}

func TestCancellationAcknowledgementRequiresExactExecutorIdentity(t *testing.T) {
	store := openTestStore(t)
	decision := mustStart(t, store, StartRequest{
		SessionID: "session-1", Mode: ModeFenced, CandidateOwner: "owner-1", WorkerID: "worker-1", Attempt: 1,
	})
	process := Process{PID: 101, StartIdentity: "boot:101", ProcessGroupID: 101}
	if err := store.RegisterProcess(context.Background(), decision.Lease, process); err != nil {
		t.Fatalf("register process: %v", err)
	}
	cancelDecision, err := store.CancelSession(context.Background(), CancelRequest{SessionID: "session-1", RequestID: "cancel-1"})
	if err != nil {
		t.Fatalf("cancel session: %v", err)
	}
	if cancelDecision.Cancellation == nil || cancelDecision.Cancellation.Target.Process != process {
		t.Fatalf("cancel target = %+v; want process %+v", cancelDecision.Cancellation, process)
	}

	staleRequests := []CancellationAcknowledgementRequest{
		{RequestID: "cancel-1", Lease: Lease{SessionID: "session-1", Generation: 2, OwnerToken: "owner-2"}, Process: process},
		{RequestID: "cancel-1", Lease: decision.Lease, Process: Process{PID: 101, StartIdentity: "boot:reused", ProcessGroupID: 101}},
		{RequestID: "different-request", Lease: decision.Lease, Process: process},
	}
	for _, request := range staleRequests {
		if err := store.AcknowledgeCancellation(context.Background(), request); !errors.Is(err, ErrStaleOwner) {
			t.Errorf("stale acknowledgement %+v = %v; want ErrStaleOwner", request, err)
		}
	}
	request := CancellationAcknowledgementRequest{RequestID: "cancel-1", Lease: decision.Lease, Process: process}
	if err := store.AcknowledgeCancellation(context.Background(), request); err != nil {
		t.Fatalf("acknowledge cancellation: %v", err)
	}
	if err := store.AcknowledgeCancellation(context.Background(), request); err != nil {
		t.Fatalf("duplicate acknowledgement: %v", err)
	}

	snapshot, err := store.Snapshot(context.Background(), "session-1")
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snapshot.Cancellation == nil || snapshot.Cancellation.Acknowledgement == nil {
		t.Fatal("cancellation acknowledgement was not persisted")
	}
	ack := snapshot.Cancellation.Acknowledgement
	if ack.Generation != decision.Lease.Generation || ack.Process != process || ack.AcknowledgedAt.IsZero() {
		t.Fatalf("acknowledgement = %+v; want exact executor identity and timestamp", ack)
	}
	assertEventKinds(t, snapshot.Events, "cancellation_ack_rejected_stale", "cancellation_acknowledged", "cancellation_acknowledgement_duplicate")
}

func TestCancellationAcknowledgementRequiresCanceledSession(t *testing.T) {
	store := openTestStore(t)
	decision := mustStart(t, store, StartRequest{
		SessionID: "session-1", Mode: ModeFenced, CandidateOwner: "owner-1", WorkerID: "worker-1", Attempt: 1,
	})
	process := Process{PID: 101, StartIdentity: "boot:101", ProcessGroupID: 101}
	if err := store.RegisterProcess(context.Background(), decision.Lease, process); err != nil {
		t.Fatalf("register process: %v", err)
	}
	err := store.AcknowledgeCancellation(context.Background(), CancellationAcknowledgementRequest{
		RequestID: "cancel-1", Lease: decision.Lease, Process: process,
	})
	if !errors.Is(err, ErrSessionNotCanceled) {
		t.Fatalf("acknowledge active session = %v; want ErrSessionNotCanceled", err)
	}
}

func TestStoreRejectsInvalidAndUnknownOperations(t *testing.T) {
	if _, err := Open(""); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Open empty path = %v; want ErrInvalidRequest", err)
	}
	store := openTestStore(t)
	if store.Path() == "" {
		t.Fatal("store path is empty")
	}
	valid := StartRequest{SessionID: "session-1", Mode: ModeFenced, CandidateOwner: "owner-1", WorkerID: "worker-1", Attempt: 1}
	invalid := []StartRequest{
		{},
		{SessionID: "session", Mode: "bad", CandidateOwner: "owner", WorkerID: "worker", Attempt: 1},
		{SessionID: "session", Mode: ModeReattach, CandidateOwner: "owner", WorkerID: "worker", Attempt: 1, ReplaceOwner: true},
		{SessionID: "session", Mode: ModeReattach, CandidateOwner: "owner", WorkerID: "worker", Attempt: 1, ReplacePendingLaunch: true},
		{SessionID: "session", Mode: ModeFenced, CandidateOwner: "owner", WorkerID: "worker", Attempt: 1, ReplaceOwner: true, ReplacePendingLaunch: true},
	}
	for _, request := range invalid {
		if _, err := store.StartOrAttach(context.Background(), request); !errors.Is(err, ErrInvalidRequest) {
			t.Errorf("StartOrAttach(%+v) = %v; want ErrInvalidRequest", request, err)
		}
	}
	decision := mustStart(t, store, valid)
	if _, err := store.StartOrAttach(context.Background(), StartRequest{
		SessionID: "session-1", Mode: ModeUnsafe, CandidateOwner: "owner-2", WorkerID: "worker-2", Attempt: 2,
	}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("mode change = %v; want ErrInvalidRequest", err)
	}
	unknown := Lease{SessionID: "session-1", Generation: 99, OwnerToken: "unknown"}
	tests := []struct {
		name string
		run  func() error
	}{
		{name: "incomplete process", run: func() error { return store.RegisterProcess(context.Background(), decision.Lease, Process{}) }},
		{name: "empty progress", run: func() error { return store.RecordProgress(context.Background(), decision.Lease, "") }},
		{name: "empty observation", run: func() error { return store.RecordObservation(context.Background(), Event{}) }},
		{name: "empty effect", run: func() error { return store.CommitEffect(context.Background(), decision.Lease, Effect{}) }},
		{name: "empty outcome", run: func() error { return store.Complete(context.Background(), decision.Lease, Outcome{}) }},
		{name: "incomplete lease", run: func() error { return store.RecordProgress(context.Background(), Lease{}, "phase") }},
		{name: "unknown lease", run: func() error { return store.RecordProgress(context.Background(), unknown, "phase") }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.run(); err == nil {
				t.Fatal("operation returned nil error")
			}
		})
	}
	if _, err := store.Snapshot(context.Background(), "missing"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("missing snapshot = %v; want ErrSessionNotFound", err)
	}
}

func TestStoreHonorsCanceledContext(t *testing.T) {
	store := openTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request := StartRequest{SessionID: "session", Mode: ModeReattach, CandidateOwner: "owner", WorkerID: "worker", Attempt: 1}
	if _, err := store.StartOrAttach(ctx, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("StartOrAttach canceled = %v; want context.Canceled", err)
	}
	if _, err := store.Snapshot(ctx, "session"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Snapshot canceled = %v; want context.Canceled", err)
	}
	if err := store.RecordObservation(ctx, Event{Kind: "event", SessionID: "session"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("RecordObservation canceled = %v; want context.Canceled", err)
	}
}

func TestReadSnapshotDoesNotCreateMissingStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.db")
	if _, err := ReadSnapshot(context.Background(), path, "session-1"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ReadSnapshot missing store = %v; want os.ErrNotExist", err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing store was created: %v", err)
	}
}

func TestEvidenceExportRejectsMissingSession(t *testing.T) {
	store := openTestStore(t)
	err := store.ExportJSONL(context.Background(), "missing", filepath.Join(t.TempDir(), "events.jsonl"))
	if !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("ExportJSONL = %v; want ErrSessionNotFound", err)
	}
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "work.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return store
}

func mustStart(t *testing.T, store *Store, request StartRequest) Decision {
	t.Helper()
	decision, err := store.StartOrAttach(context.Background(), request)
	if err != nil {
		t.Fatalf("start or attach: %v", err)
	}
	return decision
}

func mustRegisterTestProcess(t *testing.T, store *Store, lease Lease, pid int) {
	t.Helper()
	if err := store.RegisterProcess(context.Background(), lease, Process{
		PID: pid, StartIdentity: fmt.Sprintf("boot:%d", pid),
	}); err != nil {
		t.Fatalf("register process %d: %v", pid, err)
	}
}

func mustCommitEffect(t *testing.T, store *Store, lease Lease, id string) {
	t.Helper()
	if err := store.CommitEffect(context.Background(), lease, Effect{ID: id, Value: id}); err != nil {
		t.Fatalf("commit effect: %v", err)
	}
}

func assertEventKinds(t *testing.T, events []Event, want ...string) {
	t.Helper()
	kinds := make(map[string]bool, len(events))
	for _, event := range events {
		kinds[event.Kind] = true
	}
	for _, kind := range want {
		if !kinds[kind] {
			t.Errorf("missing event kind %q", kind)
		}
	}
}
