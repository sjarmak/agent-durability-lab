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

func TestCompletedSessionReturnsOutcomeWithoutLaunch(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	decision := mustStart(t, store, StartRequest{
		SessionID: "session-1", Mode: ModeReattach, CandidateOwner: "owner-1", WorkerID: "worker-1", Attempt: 1,
	})
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
	replacement := mustStart(t, store, StartRequest{
		SessionID: "session-1", Mode: ModeFenced, CandidateOwner: "owner-2", WorkerID: "worker-2", Attempt: 2, Replace: true,
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

func TestEvidenceExportIsOrderedJSONL(t *testing.T) {
	store := openTestStore(t)
	decision := mustStart(t, store, StartRequest{
		SessionID: "session-1", Mode: ModeFenced, CandidateOwner: "owner-1", WorkerID: "worker-1", Attempt: 1,
	})
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
	defer file.Close()

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
		{SessionID: "session", Mode: ModeReattach, CandidateOwner: "owner", WorkerID: "worker", Attempt: 1, Replace: true},
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
