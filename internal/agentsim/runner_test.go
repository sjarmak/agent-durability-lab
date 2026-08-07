package agentsim

import (
	"context"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/sjarmak/temporal_projects/internal/failureinject"
	"github.com/sjarmak/temporal_projects/internal/workstore"
)

func TestRunProducesProgressEffectAndOutcomeAtNamedBoundaries(t *testing.T) {
	store, barrierURL, coordinator := newTestDependencies(t)
	decision := mustDecision(t, store, workstore.StartRequest{
		SessionID: "session-1", Mode: workstore.ModeFenced, CandidateOwner: "owner-1", WorkerID: "worker-1", Attempt: 1,
	})
	runner := New(store, failureinject.NewClient(barrierURL))
	result := make(chan runResponse, 1)
	go func() {
		got, err := runner.Run(context.Background(), Config{
			Lease: decision.Lease, ActorID: "agent-1", PID: 123, ProcessStart: "boot:123", ProcessGroupID: 123,
			Effect: workstore.Effect{ID: "effect-1", Value: "changed"}, Outcome: workstore.Outcome{Value: "done"},
		})
		result <- runResponse{result: got, err: err}
	}()

	waitBarrier(t, coordinator, "before-effect/1")
	snapshot := mustSnapshot(t, store, "session-1")
	assertEvent(t, snapshot.Events, "agent_progress")
	if len(snapshot.Effects) != 0 {
		t.Fatalf("effects before release = %d; want 0", len(snapshot.Effects))
	}
	releaseBarrier(t, coordinator, "before-effect/1")

	waitBarrier(t, coordinator, "before-completion/1")
	snapshot = mustSnapshot(t, store, "session-1")
	if len(snapshot.Effects) != 1 {
		t.Fatalf("effects after effect release = %d; want 1", len(snapshot.Effects))
	}
	if snapshot.Outcome != nil {
		t.Fatalf("outcome before completion release = %+v; want nil", snapshot.Outcome)
	}
	releaseBarrier(t, coordinator, "before-completion/1")

	response := <-result
	if response.err != nil {
		t.Fatalf("run: %v", response.err)
	}
	if !response.result.EffectAccepted || !response.result.CompletionAccepted {
		t.Fatalf("result = %+v; want both accepted", response.result)
	}
	snapshot = mustSnapshot(t, store, "session-1")
	if snapshot.Executors[0].ProcessGroupID != 123 {
		t.Fatalf("registered process group = %d; want 123", snapshot.Executors[0].ProcessGroupID)
	}
}

func TestRunCanBlockBeforeProcessRegistration(t *testing.T) {
	store, barrierURL, coordinator := newTestDependencies(t)
	decision := mustDecision(t, store, workstore.StartRequest{
		SessionID: "session-1", Mode: workstore.ModeFenced, CandidateOwner: "owner-1", WorkerID: "worker-1", Attempt: 1,
	})
	runner := New(store, failureinject.NewClient(barrierURL))
	result := runAsync(runner, Config{
		Lease: decision.Lease, ActorID: "agent-1", PID: 123, ProcessStart: "boot:123",
		Effect: workstore.Effect{ID: "effect-1", Value: "changed"}, Outcome: workstore.Outcome{Value: "done"},
		BlockBeforeRegistration: true,
	})

	arrivals, err := coordinator.WaitForArrivals(context.Background(), "before-registration/1", 1)
	if err != nil {
		t.Fatalf("wait before registration: %v", err)
	}
	if arrivals[0].PID != 123 || arrivals[0].ProcessStart != "boot:123" {
		t.Fatalf("arrival process identity = %+v", arrivals[0])
	}
	snapshot := mustSnapshot(t, store, "session-1")
	if snapshot.Executors[0].Status != workstore.ExecutorStatusLaunchPending || snapshot.Executors[0].PID != 0 {
		t.Fatalf("pre-registration executor = %+v", snapshot.Executors[0])
	}
	releaseBarrier(t, coordinator, "before-registration/1")
	waitBarrier(t, coordinator, "before-effect/1")
	releaseBarrier(t, coordinator, "before-effect/1")
	waitBarrier(t, coordinator, "before-completion/1")
	releaseBarrier(t, coordinator, "before-completion/1")
	if response := <-result; response.err != nil {
		t.Fatalf("run after registration release: %v", response.err)
	}
}

func TestReplacementRejectsDelayedStaleAgent(t *testing.T) {
	store, barrierURL, coordinator := newTestDependencies(t)
	old := mustDecision(t, store, workstore.StartRequest{
		SessionID: "session-1", Mode: workstore.ModeFenced, CandidateOwner: "owner-1", WorkerID: "worker-1", Attempt: 1,
	})
	runner := New(store, failureinject.NewClient(barrierURL))
	oldResult := runAsync(runner, Config{
		Lease: old.Lease, ActorID: "agent-old", PID: 123, ProcessStart: "boot:123",
		Effect: workstore.Effect{ID: "old-effect", Value: "stale"}, Outcome: workstore.Outcome{Value: "stale"},
	})
	waitBarrier(t, coordinator, "before-effect/1")

	replacement := mustDecision(t, store, workstore.StartRequest{
		SessionID: "session-1", Mode: workstore.ModeFenced, CandidateOwner: "owner-2", WorkerID: "worker-2", Attempt: 2, ReplaceOwner: true,
	})
	newResult := runAsync(runner, Config{
		Lease: replacement.Lease, ActorID: "agent-new", PID: 456, ProcessStart: "boot:456",
		Effect: workstore.Effect{ID: "new-effect", Value: "accepted"}, Outcome: workstore.Outcome{Value: "replacement"},
	})
	waitBarrier(t, coordinator, "before-effect/2")
	releaseBarrier(t, coordinator, "before-effect/2")
	waitBarrier(t, coordinator, "before-completion/2")
	releaseBarrier(t, coordinator, "before-completion/2")

	newResponse := <-newResult
	if newResponse.err != nil || !newResponse.result.EffectAccepted || !newResponse.result.CompletionAccepted {
		t.Fatalf("replacement result = %+v, error = %v", newResponse.result, newResponse.err)
	}

	releaseBarrier(t, coordinator, "before-effect/1")
	waitBarrier(t, coordinator, "before-completion/1")
	releaseBarrier(t, coordinator, "before-completion/1")
	oldResponse := <-oldResult
	if oldResponse.err != nil {
		t.Fatalf("stale run: %v", oldResponse.err)
	}
	if oldResponse.result.EffectAccepted || oldResponse.result.CompletionAccepted {
		t.Fatalf("stale result = %+v; want both rejected", oldResponse.result)
	}

	snapshot := mustSnapshot(t, store, "session-1")
	if snapshot.Outcome == nil || snapshot.Outcome.Value != "replacement" {
		t.Fatalf("outcome after stale completion = %+v; want replacement", snapshot.Outcome)
	}
	assertEvent(t, snapshot.Events, "effect_rejected_stale")
	assertEvent(t, snapshot.Events, "completion_rejected_stale")
}

func TestCancellationRejectsDelayedAgentEffectAndCompletion(t *testing.T) {
	store, barrierURL, coordinator := newTestDependencies(t)
	decision := mustDecision(t, store, workstore.StartRequest{
		SessionID: "session-1", Mode: workstore.ModeFenced, CandidateOwner: "owner-1", WorkerID: "worker-1", Attempt: 1,
	})
	runner := New(store, failureinject.NewClient(barrierURL))
	result := runAsync(runner, Config{
		Lease: decision.Lease, ActorID: "agent-1", PID: 123, ProcessStart: "boot:123", ProcessGroupID: 123,
		Effect: workstore.Effect{ID: "effect-1", Value: "changed"}, Outcome: workstore.Outcome{Value: "done"},
	})
	waitBarrier(t, coordinator, "before-effect/1")
	if _, err := store.CancelSession(context.Background(), workstore.CancelRequest{
		SessionID: "session-1", RequestID: "cancel-1",
	}); err != nil {
		t.Fatalf("cancel session: %v", err)
	}
	releaseBarrier(t, coordinator, "before-effect/1")
	waitBarrier(t, coordinator, "before-completion/1")
	releaseBarrier(t, coordinator, "before-completion/1")

	response := <-result
	if response.err != nil {
		t.Fatalf("run after cancellation: %v", response.err)
	}
	if response.result.EffectAccepted || response.result.EffectRejection != "session_canceled" ||
		response.result.CompletionAccepted || response.result.CompletionRejection != "session_canceled" {
		t.Fatalf("canceled result = %+v; want both mutations rejected", response.result)
	}
	snapshot := mustSnapshot(t, store, "session-1")
	if len(snapshot.Effects) != 0 || snapshot.Outcome != nil {
		t.Fatalf("canceled agent mutated state: effects=%+v outcome=%+v", snapshot.Effects, snapshot.Outcome)
	}
	assertEvent(t, snapshot.Events, "effect_rejected_canceled")
	assertEvent(t, snapshot.Events, "completion_rejected_canceled")
}

func TestPendingLaunchReplacementRejectsObsoleteProcessBeforeProgress(t *testing.T) {
	store, barrierURL, _ := newTestDependencies(t)
	old := mustDecision(t, store, workstore.StartRequest{
		SessionID: "session-1", Mode: workstore.ModeFenced, CandidateOwner: "owner-1", WorkerID: "worker-1", Attempt: 1,
	})
	replacement := mustDecision(t, store, workstore.StartRequest{
		SessionID: "session-1", Mode: workstore.ModeFenced, CandidateOwner: "owner-2", WorkerID: "worker-2", Attempt: 2,
		ReplacePendingLaunch: true,
	})

	_, err := New(store, failureinject.NewClient(barrierURL)).Run(
		context.Background(), validConfig(old.Lease, "obsolete-agent", 123, "stale-effect", "stale-outcome"),
	)
	if !errors.Is(err, workstore.ErrStaleOwner) {
		t.Fatalf("obsolete process = %v; want ErrStaleOwner", err)
	}
	snapshot := mustSnapshot(t, store, "session-1")
	if len(snapshot.Effects) != 0 || snapshot.Outcome != nil {
		t.Fatalf("obsolete process mutated state: effects=%+v outcome=%+v", snapshot.Effects, snapshot.Outcome)
	}
	if snapshot.ActiveGeneration != replacement.Lease.Generation {
		t.Fatalf("active generation = %d; want %d", snapshot.ActiveGeneration, replacement.Lease.Generation)
	}
	assertEvent(t, snapshot.Events, "process_registration_rejected_stale")
}

func TestUnsafeCompetingAgentReportsExistingTerminalOutcome(t *testing.T) {
	store, barrierURL, coordinator := newTestDependencies(t)
	first := mustDecision(t, store, workstore.StartRequest{
		SessionID: "session-1", Mode: workstore.ModeUnsafe, CandidateOwner: "owner-1", WorkerID: "worker-1", Attempt: 1,
	})
	second := mustDecision(t, store, workstore.StartRequest{
		SessionID: "session-1", Mode: workstore.ModeUnsafe, CandidateOwner: "owner-2", WorkerID: "worker-2", Attempt: 2,
	})
	runner := New(store, failureinject.NewClient(barrierURL))
	firstResult := runAsync(runner, validConfig(first.Lease, "agent-1", 123, "effect-1", "first"))
	waitBarrier(t, coordinator, "before-effect/1")
	releaseBarrier(t, coordinator, "before-effect/1")
	waitBarrier(t, coordinator, "before-completion/1")
	releaseBarrier(t, coordinator, "before-completion/1")
	if response := <-firstResult; response.err != nil {
		t.Fatalf("first agent: %v", response.err)
	}

	secondResult := runAsync(runner, validConfig(second.Lease, "agent-2", 456, "effect-2", "second"))
	waitBarrier(t, coordinator, "before-effect/2")
	releaseBarrier(t, coordinator, "before-effect/2")
	waitBarrier(t, coordinator, "before-completion/2")
	releaseBarrier(t, coordinator, "before-completion/2")
	response := <-secondResult
	if response.err != nil {
		t.Fatalf("second agent: %v", response.err)
	}
	if response.result.CompletionAccepted || response.result.CompletionRejection != "terminal_outcome_exists" {
		t.Fatalf("second result = %+v", response.result)
	}
}

func TestRunnerValidationAndCanceledBarrier(t *testing.T) {
	store, barrierURL, _ := newTestDependencies(t)
	decision := mustDecision(t, store, workstore.StartRequest{
		SessionID: "session-1", Mode: workstore.ModeFenced, CandidateOwner: "owner-1", WorkerID: "worker-1", Attempt: 1,
	})
	valid := validConfig(decision.Lease, "agent-1", 123, "effect-1", "done")
	tests := []struct {
		name   string
		runner *Runner
		config Config
	}{
		{name: "missing dependencies", runner: New(nil, nil), config: valid},
		{name: "missing process identity", runner: New(store, failureinject.NewClient(barrierURL)), config: Config{Lease: valid.Lease, Effect: valid.Effect, Outcome: valid.Outcome}},
		{name: "missing lease", runner: New(store, failureinject.NewClient(barrierURL)), config: Config{ActorID: "agent", PID: 1, ProcessStart: "boot:1", Effect: valid.Effect, Outcome: valid.Outcome}},
		{name: "missing result", runner: New(store, failureinject.NewClient(barrierURL)), config: Config{Lease: valid.Lease, ActorID: "agent", PID: 1, ProcessStart: "boot:1"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := test.runner.Run(context.Background(), test.config); err == nil {
				t.Fatal("Run returned nil error")
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := New(store, failureinject.NewClient(barrierURL)).Run(ctx, valid)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Run = %v; want context.Canceled", err)
	}
}

type runResponse struct {
	result Result
	err    error
}

func runAsync(runner *Runner, config Config) <-chan runResponse {
	result := make(chan runResponse, 1)
	go func() {
		got, err := runner.Run(context.Background(), config)
		result <- runResponse{result: got, err: err}
	}()
	return result
}

func validConfig(lease workstore.Lease, actorID string, pid int, effectID, outcome string) Config {
	return Config{
		Lease: lease, ActorID: actorID, PID: pid, ProcessStart: "boot:start",
		Effect: workstore.Effect{ID: effectID, Value: "changed"}, Outcome: workstore.Outcome{Value: outcome},
	}
}

func newTestDependencies(t *testing.T) (*workstore.Store, string, *failureinject.Coordinator) {
	t.Helper()
	store, err := workstore.Open(filepath.Join(t.TempDir(), "work.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	coordinator := failureinject.NewCoordinator()
	server := httptest.NewServer(coordinator.Handler())
	t.Cleanup(server.Close)
	return store, server.URL, coordinator
}

func mustDecision(t *testing.T, store *workstore.Store, request workstore.StartRequest) workstore.Decision {
	t.Helper()
	decision, err := store.StartOrAttach(context.Background(), request)
	if err != nil {
		t.Fatalf("start or attach: %v", err)
	}
	return decision
}

func waitBarrier(t *testing.T, coordinator *failureinject.Coordinator, point string) {
	t.Helper()
	if _, err := coordinator.WaitForArrivals(context.Background(), point, 1); err != nil {
		t.Fatalf("wait for barrier %q: %v", point, err)
	}
}

func releaseBarrier(t *testing.T, coordinator *failureinject.Coordinator, point string) {
	t.Helper()
	if err := coordinator.Release(point); err != nil {
		t.Fatalf("release barrier %q: %v", point, err)
	}
}

func mustSnapshot(t *testing.T, store *workstore.Store, sessionID string) workstore.Snapshot {
	t.Helper()
	snapshot, err := store.Snapshot(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	return snapshot
}

func assertEvent(t *testing.T, events []workstore.Event, kind string) {
	t.Helper()
	for _, event := range events {
		if event.Kind == kind {
			return
		}
	}
	t.Fatalf("missing event kind %q", kind)
}
