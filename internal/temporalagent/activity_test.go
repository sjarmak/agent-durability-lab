package temporalagent

import (
	"context"
	"errors"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/temporalio-labs/agent-durability-lab/internal/failureinject"
	"github.com/temporalio-labs/agent-durability-lab/internal/workstore"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
)

func TestActivityLaunchesAgentAndReturnsCanonicalOutcome(t *testing.T) {
	store, err := workstore.Open(filepath.Join(t.TempDir(), "work.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	coordinator := failureinject.NewCoordinator()
	server := httptest.NewServer(coordinator.Handler())
	t.Cleanup(server.Close)
	agentBinary := buildAgentSimulator(t)
	activities := Activities{
		StorePath: store.Path(), AgentBinary: agentBinary, BarrierURL: server.URL,
		RunDirectory: t.TempDir(), WorkerID: "worker-test", AgentBuild: "test-build",
	}

	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestActivityEnvironment()
	environment.RegisterActivity(activities.RunAgent)
	response := make(chan activityResponse, 1)
	go func() {
		value, runErr := environment.ExecuteActivity(activities.RunAgent, ActivityInput{
			SessionID: "session-1", Mode: workstore.ModeFenced,
		})
		response <- activityResponse{value: value, err: runErr}
	}()

	waitActivityBarrier(t, coordinator, "before-effect/1")
	releaseActivityBarrier(t, coordinator, "before-effect/1")
	waitActivityBarrier(t, coordinator, "before-completion/1")
	releaseActivityBarrier(t, coordinator, "before-completion/1")

	result := <-response
	if result.err != nil {
		t.Fatalf("execute Activity: %v", result.err)
	}
	var outcome workstore.Outcome
	if err := result.value.Get(&outcome); err != nil {
		t.Fatalf("decode Activity result: %v", err)
	}
	if outcome.Value != "outcome/session-1/g1" {
		t.Fatalf("outcome = %+v; want generation 1", outcome)
	}
}

func TestActivityReturnsPreviouslyAcceptedOutcomeWithoutLaunching(t *testing.T) {
	store, err := workstore.Open(filepath.Join(t.TempDir(), "work.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	decision, err := store.StartOrAttach(context.Background(), workstore.StartRequest{
		SessionID: "session-1", Mode: workstore.ModeReattach, CandidateOwner: "owner-1", WorkerID: "old-worker", Attempt: 1,
	})
	if err != nil {
		t.Fatalf("start session: %v", err)
	}
	want := workstore.Outcome{Value: "already-durable"}
	if err := store.Complete(context.Background(), decision.Lease, want); err != nil {
		t.Fatalf("complete session: %v", err)
	}
	activities := Activities{
		StorePath: store.Path(), AgentBinary: "unused", BarrierURL: "http://unused",
		RunDirectory: t.TempDir(), WorkerID: "retry-worker", AgentBuild: "test-build",
	}
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestActivityEnvironment()
	environment.RegisterActivity(activities.RunAgent)
	value, err := environment.ExecuteActivity(activities.RunAgent, ActivityInput{
		SessionID: "session-1", Mode: workstore.ModeReattach,
	})
	if err != nil {
		t.Fatalf("execute Activity: %v", err)
	}
	var outcome workstore.Outcome
	if err := value.Get(&outcome); err != nil {
		t.Fatalf("decode outcome: %v", err)
	}
	if outcome != want {
		t.Fatalf("outcome = %+v; want %+v", outcome, want)
	}
}

func TestActivityPreHeartbeatBoundaryBlocksUntilRelease(t *testing.T) {
	store, err := workstore.Open(filepath.Join(t.TempDir(), "work.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	decision, err := store.StartOrAttach(context.Background(), workstore.StartRequest{
		SessionID: "session-1", Mode: workstore.ModeFenced, CandidateOwner: "owner-1", WorkerID: "worker-test", Attempt: 1,
	})
	if err != nil {
		t.Fatalf("start session: %v", err)
	}
	coordinator := failureinject.NewCoordinator()
	server := httptest.NewServer(coordinator.Handler())
	t.Cleanup(server.Close)
	activities := Activities{BarrierURL: server.URL, WorkerID: "worker-test"}
	result := make(chan error, 1)
	go func() {
		result <- activities.blockBeforeFirstHeartbeat(context.Background(), store, decision.Lease, 1)
	}()
	waitActivityBarrier(t, coordinator, "activity-before-first-heartbeat/1")
	select {
	case err := <-result:
		t.Fatalf("pre-heartbeat boundary returned before release: %v", err)
	default:
	}
	releaseActivityBarrier(t, coordinator, "activity-before-first-heartbeat/1")
	if err := <-result; err != nil {
		t.Fatalf("pre-heartbeat boundary: %v", err)
	}
}

func TestActivityValidationRejectsIncompleteConfiguration(t *testing.T) {
	tests := []struct {
		name       string
		activities Activities
		input      ActivityInput
	}{
		{name: "missing dependencies", input: ActivityInput{SessionID: "session", Mode: workstore.ModeFenced}},
		{
			name:       "invalid mode",
			activities: Activities{StorePath: "x", AgentBinary: "x", BarrierURL: "x", RunDirectory: "x", WorkerID: "x", AgentBuild: "x"},
			input:      ActivityInput{SessionID: "session", Mode: "invalid"},
		},
		{
			name:       "replacement without fence",
			activities: Activities{StorePath: "x", AgentBinary: "x", BarrierURL: "x", RunDirectory: "x", WorkerID: "x", AgentBuild: "x"},
			input:      ActivityInput{SessionID: "session", Mode: workstore.ModeReattach, ReplaceOnRetry: true},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.activities.validate(test.input); err == nil {
				t.Fatal("validate returned nil error")
			}
		})
	}
}

func TestOwnerTokensAreRandomAndOpaque(t *testing.T) {
	first, err := newOwnerToken()
	if err != nil {
		t.Fatalf("first token: %v", err)
	}
	second, err := newOwnerToken()
	if err != nil {
		t.Fatalf("second token: %v", err)
	}
	if len(first) != 64 || len(second) != 64 || first == second {
		t.Fatalf("tokens have lengths %d/%d and equality %v", len(first), len(second), first == second)
	}
}

func TestSessionDirectoryNameIsStableAndPathSafe(t *testing.T) {
	first := sessionDirectoryName("../outside/session")
	second := sessionDirectoryName("../outside/session")
	other := sessionDirectoryName("different-session")
	if first != second || first == other {
		t.Fatalf("directory names = %q, %q, %q", first, second, other)
	}
	if first == "." || first == ".." || strings.ContainsAny(first, `/\\`) {
		t.Fatalf("unsafe session directory name %q", first)
	}
}

func TestActivityReportsStoreAndLaunchFailures(t *testing.T) {
	tests := []struct {
		name       string
		activities Activities
	}{
		{
			name: "store path is directory",
			activities: Activities{
				StorePath: t.TempDir(), AgentBinary: "unused", BarrierURL: "http://unused",
				RunDirectory: t.TempDir(), WorkerID: "worker", AgentBuild: "build",
			},
		},
		{
			name: "agent binary is missing",
			activities: Activities{
				StorePath: filepath.Join(t.TempDir(), "work.db"), AgentBinary: filepath.Join(t.TempDir(), "missing"),
				BarrierURL: "http://unused", RunDirectory: t.TempDir(), WorkerID: "worker", AgentBuild: "build",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			environment := suite.NewTestActivityEnvironment()
			environment.RegisterActivity(test.activities.RunAgent)
			if _, err := environment.ExecuteActivity(test.activities.RunAgent, ActivityInput{
				SessionID: "session-1", Mode: workstore.ModeFenced,
			}); err == nil {
				t.Fatal("ExecuteActivity returned nil error")
			}
		})
	}
}

func TestActivityWaitAndObservationHonorCancellation(t *testing.T) {
	store, err := workstore.Open(filepath.Join(t.TempDir(), "work.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	decision, err := store.StartOrAttach(context.Background(), workstore.StartRequest{
		SessionID: "session-1", Mode: workstore.ModeFenced, CandidateOwner: "owner", WorkerID: "worker", Attempt: 1,
	})
	if err != nil {
		t.Fatalf("start session: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	activities := Activities{BarrierURL: "http://unused", WorkerID: "worker"}
	if _, err := activities.waitForOutcome(ctx, store, decision.Lease); !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForOutcome = %v; want context.Canceled", err)
	}
	if err := activities.blockBeforeFirstHeartbeat(ctx, store, decision.Lease, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("blockBeforeFirstHeartbeat = %v; want context.Canceled", err)
	}
}

type activityResponse struct {
	value converter.EncodedValue
	err   error
}

func buildAgentSimulator(t *testing.T) string {
	t.Helper()
	root := temporalAgentRepositoryRoot(t)
	path := filepath.Join(t.TempDir(), "agent-simulator")
	command := exec.Command("go", "build", "-o", path, "./cmd/agent-simulator")
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build agent simulator: %v\n%s", err, output)
	}
	return path
}

func temporalAgentRepositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate Activity test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func waitActivityBarrier(t *testing.T, coordinator *failureinject.Coordinator, point string) {
	t.Helper()
	if _, err := coordinator.WaitForArrivals(context.Background(), point, 1); err != nil {
		t.Fatalf("wait for barrier %q: %v", point, err)
	}
}

func releaseActivityBarrier(t *testing.T, coordinator *failureinject.Coordinator, point string) {
	t.Helper()
	if err := coordinator.Release(point); err != nil {
		t.Fatalf("release barrier %q: %v", point, err)
	}
}
