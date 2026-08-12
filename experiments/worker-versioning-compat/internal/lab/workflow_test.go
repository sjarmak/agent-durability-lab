package lab

import (
	"testing"

	"go.temporal.io/sdk/testsuite"
)

func TestVersionedWorkflowSchedulesStableActivityAndWaitsForNextPhase(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	environment.RegisterActivityWithOptions(func(input ActivityInput) (ActivityReceipt, error) {
		return ActivityReceipt{SessionID: input.SessionID, WorkerBuild: "worker-v1", AgentBuild: "agent-v1"}, nil
	}, activityRegistrationOptions())
	environment.RegisterDelayedCallback(func() {
		environment.SignalWorkflow(continueSignal, struct{}{})
	}, 0)
	environment.ExecuteWorkflow(CompatibleWorkflow, WorkflowInput{SessionID: "session-1", RegistryPath: "/registry", Phases: 2})
	if err := environment.GetWorkflowError(); err != nil {
		t.Fatalf("workflow: %v", err)
	}
	var result WorkflowResult
	if err := environment.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if len(result.Receipts) != 2 || result.Receipts[0].SessionID != "session-1" {
		t.Fatalf("result = %+v", result)
	}
}
