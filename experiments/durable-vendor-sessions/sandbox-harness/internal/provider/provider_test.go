package provider

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
	sandboxworkflow "github.com/temporal-community/sandbox-orchestration-harness/sdk/workflow"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestPinnedUpstreamStartActivitySeparatesUpdateFromProviderIdempotency(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name            string
		mode            Mode
		wantInstances   int
		wantApplied     []bool
		wantFinalSuffix string
	}{
		{name: "unsafe", mode: ModeUnsafe, wantInstances: 2, wantApplied: []bool{true, true}, wantFinalSuffix: "000002"},
		{name: "idempotent", mode: ModeIdempotent, wantInstances: 1, wantApplied: []bool{true, false}, wantFinalSuffix: "000001"},
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
				FaultOperation: OperationStart, SessionID: "sandbox-1", WorkerIdentity: "worker-1",
			}

			var suite testsuite.WorkflowTestSuite
			environment := suite.NewTestWorkflowEnvironment()
			environment.RegisterActivity(sandboxworkflow.StartSandbox)
			environment.ExecuteWorkflow(startActivityWorkflow, config.ProviderDetails())
			if err := environment.GetWorkflowError(); err != nil {
				t.Fatalf("workflow failed: %v", err)
			}
			var instanceID string
			if err := environment.GetWorkflowResult(&instanceID); err != nil {
				t.Fatalf("workflow result: %v", err)
			}
			if len(instanceID) < len(test.wantFinalSuffix) || instanceID[len(instanceID)-len(test.wantFinalSuffix):] != test.wantFinalSuffix {
				t.Fatalf("instance ID = %q, want suffix %q", instanceID, test.wantFinalSuffix)
			}
			state, err := store.Snapshot(context.Background())
			if err != nil {
				t.Fatalf("Snapshot() error = %v", err)
			}
			if len(state.Instances) != test.wantInstances || len(state.Attempts) != 2 {
				t.Fatalf("instances/attempts = %d/%d, want %d/2", len(state.Instances), len(state.Attempts), test.wantInstances)
			}
			if state.Attempts[0].OperationID != state.Attempts[1].OperationID {
				t.Fatalf("operation IDs differ across Activity retry")
			}
			if state.Attempts[0].PhysicalAttemptID == state.Attempts[1].PhysicalAttemptID {
				t.Fatalf("physical attempt IDs were reused")
			}
			gotApplied := []bool{state.Attempts[0].Applied, state.Attempts[1].Applied}
			if !equalBools(gotApplied, test.wantApplied) {
				t.Fatalf("applied = %v, want %v", gotApplied, test.wantApplied)
			}
		})
	}
}

func startActivityWorkflow(ctx workflow.Context, details compute.ProviderDetails) (string, error) {
	options := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Millisecond, MaximumInterval: time.Millisecond, MaximumAttempts: 2,
		},
	}
	var output sandboxworkflow.StartSandboxOutput
	err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, options),
		sandboxworkflow.StartSandbox,
		sandboxworkflow.StartSandboxInput{Provider: details, TaskQueueName: "sandbox-queue"},
	).Get(ctx, &output)
	if err != nil {
		return "", err
	}
	return output.Status.InstanceID, nil
}
