package lab

import (
	"context"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestCodexWorkflowReturnsOneAcceptedActivityResult(t *testing.T) {
	input := CodexActivityInput{LogicalSessionID: "session-1", LogicalTurnID: "turn-1", LogicalEffectID: "effect-1"}
	want := CodexActivityResult{
		TemporalAttempt: 2, PhysicalAttemptID: "attempt-2", ThreadID: testThreadID,
		Result: "EFFECT_COMPLETE", ProcessIdentity: "pid:2:start:two",
	}
	var heartbeatTimeout time.Duration
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	environment.RegisterActivityWithOptions(func(ctx context.Context, _ CodexActivityInput) (CodexActivityResult, error) {
		heartbeatTimeout = activity.GetInfo(ctx).HeartbeatTimeout
		return want, nil
	}, activity.RegisterOptions{Name: RunCodexActivityName})
	environment.ExecuteWorkflow(CodexWorkflow, input)
	if err := environment.GetWorkflowError(); err != nil {
		t.Fatalf("Workflow: %v", err)
	}
	var got CodexActivityResult
	if err := environment.GetWorkflowResult(&got); err != nil || got != want {
		t.Fatalf("result = %+v err=%v", got, err)
	}
	if heartbeatTimeout < 15*time.Second {
		t.Fatalf("heartbeat timeout = %s", heartbeatTimeout)
	}
}

func TestFencedWorkflowWaitsForCancellation(t *testing.T) {
	fenced := codexActivityOptions(CodexActivityInput{RecoveryMode: RecoveryModeFenced})
	resume := codexActivityOptions(CodexActivityInput{RecoveryMode: RecoveryModeResumeOnly})
	if !fenced.WaitForCancellation || resume.WaitForCancellation {
		t.Fatalf("WaitForCancellation fenced=%t resume=%t", fenced.WaitForCancellation, resume.WaitForCancellation)
	}
}
