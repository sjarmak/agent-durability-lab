package provider

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	sandboxworkflow "github.com/temporal-community/sandbox-orchestration-harness/sdk/workflow"
	"go.temporal.io/sdk/testsuite"
)

func TestPinnedSandboxWorkflowDeduplicatesOuterUpdateButNotProviderEffect(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name        string
		mode        Mode
		wantEffects []string
		wantApplied []bool
	}{
		{name: "unsafe", mode: ModeUnsafe, wantEffects: []string{"effect-1", "effect-1"}, wantApplied: []bool{true, true}},
		{name: "idempotent", mode: ModeIdempotent, wantEffects: []string{"effect-1"}, wantApplied: []bool{true, false}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			store := createStore(t, test.mode)
			barrier := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				response.WriteHeader(http.StatusNoContent)
			}))
			defer barrier.Close()
			config := Config{
				DatabasePath: store.path, Mode: test.mode, BarrierURL: barrier.URL,
				FaultOperation: OperationCommand, SessionID: "sandbox-1", WorkerIdentity: "worker-1",
			}
			command, err := EncodeCommand(CommandEnvelope{LogicalEffectID: "effect-1", Payload: "fixture"})
			if err != nil {
				t.Fatalf("EncodeCommand() error = %v", err)
			}

			var suite testsuite.WorkflowTestSuite
			environment := suite.NewTestWorkflowEnvironment()
			registerPinnedActivities(environment)
			updateErrors := make([]error, 0, 3)
			completedCommands := 0
			environment.RegisterDelayedCallback(func() {
				environment.UpdateWorkflow(
					sandboxworkflow.SandboxInitUpdate,
					"init-1",
					&testsuite.TestUpdateCallback{
						OnReject: func(updateErr error) { updateErrors = append(updateErrors, updateErr) },
						OnComplete: func(_ any, updateErr error) {
							updateErrors = append(updateErrors, updateErr)
						},
					},
					sandboxworkflow.SandboxInitInput{
						ComputeProvider: config.ProviderDetails(), IdleTimeout: compute.NoIdleTimeout,
					},
				)
			}, 0)
			environment.RegisterDelayedCallback(func() {
				for range 2 {
					environment.UpdateWorkflow(
						sandboxworkflow.SandboxExecuteCommandUpdate,
						"command-update-1",
						&testsuite.TestUpdateCallback{
							OnReject: func(updateErr error) { updateErrors = append(updateErrors, updateErr) },
							OnComplete: func(_ any, updateErr error) {
								updateErrors = append(updateErrors, updateErr)
								completedCommands++
								if completedCommands == 2 {
									environment.SignalWorkflow(sandboxworkflow.SandboxStopSignal, nil)
								}
							},
						},
						sandboxworkflow.SandboxExecuteCommandInput{Command: command},
					)
				}
			}, time.Second)

			environment.ExecuteWorkflow(sandboxworkflow.SandboxWorkflow, sandboxworkflow.SandboxLocalState{})
			if err := environment.GetWorkflowError(); err != nil {
				t.Fatalf("SandboxWorkflow failed: %v", err)
			}
			for index, updateErr := range updateErrors {
				if updateErr != nil {
					t.Fatalf("update error %d = %v", index, updateErr)
				}
			}
			if completedCommands != 2 {
				t.Fatalf("completed duplicate Update callbacks = %d, want 2", completedCommands)
			}

			state, err := store.Snapshot(context.Background())
			if err != nil {
				t.Fatalf("Snapshot() error = %v", err)
			}
			if len(state.Instances) != 1 {
				t.Fatalf("instances = %d, want 1", len(state.Instances))
			}
			if got := state.Instances[0].Effects; !equalStrings(got, test.wantEffects) {
				t.Fatalf("effects = %v, want %v", got, test.wantEffects)
			}
			commandAttempts := attemptsFor(state.Attempts, OperationCommand)
			if len(commandAttempts) != 2 {
				t.Fatalf("command attempts = %d, want 2", len(commandAttempts))
			}
			if commandAttempts[0].OperationID != commandAttempts[1].OperationID {
				t.Fatalf("inner operation IDs differ across Activity retry: %q != %q", commandAttempts[0].OperationID, commandAttempts[1].OperationID)
			}
			gotApplied := []bool{commandAttempts[0].Applied, commandAttempts[1].Applied}
			if !equalBools(gotApplied, test.wantApplied) {
				t.Fatalf("applied = %v, want %v", gotApplied, test.wantApplied)
			}
		})
	}
}

func registerPinnedActivities(environment *testsuite.TestWorkflowEnvironment) {
	environment.RegisterActivity(sandboxworkflow.StartSandbox)
	environment.RegisterActivity(sandboxworkflow.ExecuteCommand)
	environment.RegisterActivity(sandboxworkflow.StopSandbox)
}

func attemptsFor(attempts []Attempt, operation Operation) []Attempt {
	selected := make([]Attempt, 0, len(attempts))
	for _, attempt := range attempts {
		if attempt.Kind == operation {
			selected = append(selected, attempt)
		}
	}
	return selected
}
