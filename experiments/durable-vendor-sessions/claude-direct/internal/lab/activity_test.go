package lab

import (
	"context"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/internal/workstore"
	"go.temporal.io/sdk/testsuite"
)

func TestRunClaudeActivityDerivesDistinctPhysicalAttemptFromTemporalIdentity(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	workDirectory := t.TempDir()
	var capturedInvocation Invocation
	var capturedRun RunInvocationInput
	activities := Activities{
		Command: ClaudeCommand{
			Binary: "/opt/claude", WorkDir: workDirectory, Model: "haiku",
			MaxBudgetUSD: "0.25", MaxTurns: 2,
		},
		LauncherBinary:  "/opt/launcher",
		FaultBoundary:   FaultNone,
		EffectBinary:    "/opt/controlled-effect",
		DestinationPath: filepath.Join(root, "destination.db"),
		WorkspacePath:   filepath.Join(root, "workspace", "effects.jsonl"),
		EffectPayload:   "controlled-edit",
		BarrierURL:      "http://127.0.0.1:8080",
		BarrierPoint:    "claude-tool-effect-committed",
		RunRoot:         root,
		WorkerID:        "worker-one",
		Invoke: func(_ context.Context, invocation Invocation, input RunInvocationInput) (InvocationResult, error) {
			capturedInvocation = invocation
			capturedRun = input
			return InvocationResult{
				Claude:  ClaudeStreamResult{SessionID: "vendor-session-1", Result: "EFFECT_COMPLETE"},
				Process: ProcessRecord{AttemptID: input.AttemptID, ActorID: input.ActorID, PID: 101, StartIdentity: "start-one", Identity: "pid:101:start:start-one"},
			}, nil
		},
	}
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestActivityEnvironment()
	environment.RegisterActivity(activities.RunClaude)
	encoded, err := environment.ExecuteActivity(activities.RunClaude, ClaudeActivityInput{
		LogicalSessionID: "logical-session-1", LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
	})
	if err != nil {
		t.Fatalf("execute Activity: %v", err)
	}
	var result ClaudeActivityResult
	if err := encoded.Get(&result); err != nil {
		t.Fatalf("decode Activity result: %v", err)
	}
	if result.TemporalAttempt != 1 || result.VendorSessionID != "vendor-session-1" || result.Result != "EFFECT_COMPLETE" {
		t.Fatalf("result = %+v", result)
	}
	if !strings.Contains(capturedRun.AttemptID, "attempt-1") || capturedRun.ActorID != "worker-one-attempt-1" {
		t.Fatalf("run identity = %+v", capturedRun)
	}
	if capturedRun.Directory != filepath.Join(root, capturedRun.AttemptID) {
		t.Fatalf("run directory = %q", capturedRun.Directory)
	}
	joined := strings.Join(capturedInvocation.Args, " ")
	if strings.Contains(joined, "--session-id") || strings.Contains(joined, "--resume") {
		t.Fatalf("unsafe Activity invocation contains recovery control: %s", joined)
	}
	if !strings.Contains(capturedInvocation.Stdin, "controlled-effect --request") {
		t.Fatalf("Activity prompt = %q", capturedInvocation.Stdin)
	}
}

func TestFencedActivityCancellationDurablyRevokesSupervisorBeforeReturning(t *testing.T) {
	store := openSupervisorTestStore(t)
	started := make(chan struct{})
	stopped := make(chan struct{})
	supervisor := newTurnSupervisor(context.Background(), store,
		func(ctx context.Context, store *workstore.Store, lease workstore.Lease) (supervisedResult, error) {
			if err := store.RegisterProcess(ctx, lease, supervisorTestProcess(lease.Generation)); err != nil {
				return supervisedResult{}, err
			}
			close(started)
			<-ctx.Done()
			close(stopped)
			return supervisedResult{}, workstore.ErrSessionCanceled
		}, sequentialCapabilities())
	server := httptest.NewServer(newSupervisorHandler(supervisor))
	t.Cleanup(server.Close)
	activities := Activities{SupervisorURL: server.URL, WorkerID: "worker-one"}
	activityContext, cancelActivity := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := activities.runFencedClaude(activityContext, ClaudeActivityInput{
			LogicalSessionID: "logical-session-1", LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
			RecoveryMode: RecoveryModeFenced,
		}, 1, "temporal-cancel-operation-1")
		result <- err
	}()
	select {
	case <-started:
	case err := <-result:
		t.Fatalf("fenced Activity ended before supervisor start: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("fenced Activity did not reach supervisor start")
	}
	cancelActivity()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled fenced Activity = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled fenced Activity did not return")
	}
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("supervisor-owned execution was not stopped")
	}
	snapshot, err := store.Snapshot(context.Background(), "logical-session-1")
	if err != nil {
		t.Fatalf("snapshot canceled Activity: %v", err)
	}
	if snapshot.Cancellation == nil || snapshot.Cancellation.RequestID != "temporal-cancel-operation-1" ||
		snapshot.Cancellation.Generation != 1 || snapshot.Executors[0].Status != workstore.ExecutorStatusCanceled {
		t.Fatalf("durable cancellation = %+v", snapshot)
	}
}

func TestFencedActivityDelegatesTurnToSupervisor(t *testing.T) {
	store := openSupervisorTestStore(t)
	supervisor := newTurnSupervisor(context.Background(), store,
		func(ctx context.Context, store *workstore.Store, lease workstore.Lease) (supervisedResult, error) {
			process := supervisorTestProcess(lease.Generation)
			if err := store.RegisterProcess(ctx, lease, process); err != nil {
				return supervisedResult{}, err
			}
			if err := store.CommitEffectOnce(ctx, lease, workstore.Effect{ID: "effect-1", Value: "controlled-edit"}); err != nil {
				return supervisedResult{}, err
			}
			return supervisedResult{
				VendorSessionID:   "01890f3e-7b5a-4c2d-8e1f-0123456789ab",
				PhysicalAttemptID: "supervisor-generation-1", ProcessIdentity: "pid:101:start:boot:1",
				Outcome: workstore.Outcome{Value: "EFFECT_COMPLETE"},
			}, nil
		}, sequentialCapabilities())
	server := httptest.NewServer(newSupervisorHandler(supervisor))
	t.Cleanup(server.Close)
	activities := Activities{
		Command: ClaudeCommand{
			Binary: "/opt/claude", WorkDir: t.TempDir(), Model: "haiku", MaxBudgetUSD: "0.25", MaxTurns: 2,
		},
		LauncherBinary: "/opt/launcher", FaultBoundary: FaultNone,
		EffectBinary: "/opt/controlled-effect", DestinationPath: t.TempDir() + "/destination.db",
		WorkspacePath: t.TempDir() + "/workspace.jsonl", EffectPayload: "controlled-edit",
		BarrierURL: "http://127.0.0.1:8080", BarrierPoint: committedEffectBarrier,
		RunRoot: t.TempDir(), WorkerID: "worker-one", SupervisorURL: server.URL,
	}
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestActivityEnvironment()
	environment.RegisterActivity(activities.RunClaude)
	encoded, err := environment.ExecuteActivity(activities.RunClaude, ClaudeActivityInput{
		LogicalSessionID: "logical-session-1", LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
		RecoveryMode:            RecoveryModeFenced,
		SelectedVendorSessionID: "01890f3e-7b5a-4c2d-8e1f-0123456789ab",
	})
	if err != nil {
		t.Fatalf("execute fenced Activity: %v", err)
	}
	var result ClaudeActivityResult
	if err := encoded.Get(&result); err != nil {
		t.Fatalf("decode fenced Activity: %v", err)
	}
	if result.TemporalAttempt != 1 || result.PhysicalAttemptID != "supervisor-generation-1" ||
		result.VendorSessionID != "01890f3e-7b5a-4c2d-8e1f-0123456789ab" ||
		result.Result != "EFFECT_COMPLETE" || result.ProcessIdentity != "pid:101:start:boot:1" {
		t.Fatalf("fenced Activity result = %+v", result)
	}
	snapshot, err := store.Snapshot(context.Background(), "logical-session-1")
	if err != nil {
		t.Fatalf("fenced snapshot: %v", err)
	}
	if snapshot.ActiveGeneration != 1 || len(snapshot.Effects) != 1 || snapshot.Outcome == nil {
		t.Fatalf("fenced snapshot = %+v", snapshot)
	}
}

func TestRunClaudeActivityRejectsIncompleteStableIdentity(t *testing.T) {
	t.Parallel()

	activities := Activities{}
	if _, err := activities.RunClaude(context.Background(), ClaudeActivityInput{}); err == nil {
		t.Fatal("incomplete Activity returned nil error")
	}
}

func TestPreRegistrationInvocationUsesLauncherWithoutRecoveryControls(t *testing.T) {
	t.Parallel()
	activities := Activities{
		Command: ClaudeCommand{Binary: "/opt/claude"}, LauncherBinary: "/opt/claude-direct-launcher",
		BarrierURL: "http://127.0.0.1:8080",
	}
	originalEnv := make([]string, 1, 8)
	originalEnv[0] = "EXISTING=value"
	original := Invocation{Binary: "/opt/claude", Args: []string{"-p"}, Env: originalEnv}
	got := activities.preRegistrationInvocation(original, "logical-session-1", "physical-attempt-1", "actor-1")
	if got.Binary != activities.LauncherBinary || original.Binary != "/opt/claude" {
		t.Fatalf("launcher invocation = %+v; original = %+v", got, original)
	}
	if &got.Env[0] == &original.Env[0] {
		t.Fatal("launcher invocation shares mutable environment storage with its input")
	}
	joined := strings.Join(got.Env, "\n")
	for _, want := range []string{
		"CLAUDE_DIRECT_REAL_BINARY=/opt/claude",
		"CLAUDE_DIRECT_PHYSICAL_ATTEMPT_ID=physical-attempt-1",
		"CLAUDE_DIRECT_LOGICAL_SESSION_ID=logical-session-1",
		"CLAUDE_DIRECT_ACTOR_ID=actor-1",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("launcher environment %q lacks %q", joined, want)
		}
	}
	if strings.Contains(strings.Join(got.Args, " "), "--resume") {
		t.Fatalf("launcher args contain recovery controls: %v", got.Args)
	}
}

func TestResumeOnlyActivityRejectsObservedSessionIdentityMismatch(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	activities := Activities{
		Command: ClaudeCommand{
			Binary: "/opt/claude", WorkDir: t.TempDir(), Model: "haiku", MaxBudgetUSD: "0.25", MaxTurns: 2,
		},
		LauncherBinary: "/opt/launcher", FaultBoundary: FaultNone,
		EffectBinary: "/opt/controlled-effect", DestinationPath: filepath.Join(root, "destination.db"),
		WorkspacePath: filepath.Join(root, "workspace", "effects.jsonl"), EffectPayload: "controlled-edit",
		BarrierURL: "http://127.0.0.1:8080", BarrierPoint: committedEffectBarrier,
		RunRoot: root, WorkerID: "worker-one",
		Invoke: func(_ context.Context, _ Invocation, input RunInvocationInput) (InvocationResult, error) {
			return InvocationResult{
				Claude: ClaudeStreamResult{SessionID: "11890f3e-7b5a-4c2d-8e1f-0123456789ab", Result: "EFFECT_COMPLETE"},
				Process: ProcessRecord{AttemptID: input.AttemptID, ActorID: input.ActorID, PID: 101,
					StartIdentity: "start-one", Identity: "pid:101:start:start-one"},
			}, nil
		},
	}
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestActivityEnvironment()
	environment.RegisterActivity(activities.RunClaude)
	_, err := environment.ExecuteActivity(activities.RunClaude, ClaudeActivityInput{
		LogicalSessionID: "logical-session-1", LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
		RecoveryMode:            RecoveryModeResumeOnly,
		SelectedVendorSessionID: "01890f3e-7b5a-4c2d-8e1f-0123456789ab",
	})
	if err == nil || !strings.Contains(err.Error(), "selected Claude session") {
		t.Fatalf("session mismatch error = %v", err)
	}
}
