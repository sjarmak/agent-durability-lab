package temporaladapter

import (
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/systemplan"
	"go.temporal.io/sdk/testsuite"
)

func TestWorkflowDurablyExecutesEveryStepAndRetriesExactFault(t *testing.T) {
	plan, err := systemplan.Build(protocol.CaseOutageBacklogRecovery, protocol.ProbeProtected, 1)
	if err != nil {
		t.Fatal(err)
	}
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	environment.RegisterActivity(RecordStep)
	environment.ExecuteWorkflow(Workflow, plan)
	if err := environment.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var result Result
	if err := environment.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if len(result.Receipts) != len(plan.Steps) {
		t.Fatalf("receipts=%d, want %d", len(result.Receipts), len(plan.Steps))
	}
	for index, receipt := range result.Receipts {
		if receipt.StepID != plan.Steps[index].ID || receipt.Attempt != map[bool]int32{true: 2, false: 1}[plan.Steps[index].FailureOnce] {
			t.Errorf("receipt %d = %+v", index, receipt)
		}
	}
}

func TestWorkflowRejectsInvalidPlan(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	environment.RegisterActivity(RecordStep)
	environment.ExecuteWorkflow(Workflow, systemplan.Plan{})
	if err := environment.GetWorkflowError(); err == nil {
		t.Fatal("invalid plan succeeded")
	}
}
