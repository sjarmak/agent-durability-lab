package lab

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestDirectClaudeWorkflowReturnsTheSingleAcceptedActivityResult(t *testing.T) {
	t.Parallel()

	input := ClaudeActivityInput{
		LogicalSessionID: "logical-session-1", LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
	}
	want := ClaudeActivityResult{
		TemporalAttempt: 2, PhysicalAttemptID: "physical-attempt-2",
		VendorSessionID: "vendor-session-2", Result: "EFFECT_COMPLETE",
	}
	var heartbeatTimeout time.Duration
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	calls := 0
	environment.RegisterActivityWithOptions(
		func(ctx context.Context, _ ClaudeActivityInput) (ClaudeActivityResult, error) {
			calls++
			heartbeatTimeout = activity.GetInfo(ctx).HeartbeatTimeout
			return want, nil
		},
		activity.RegisterOptions{Name: RunClaudeActivityName},
	)
	environment.ExecuteWorkflow(DirectClaudeWorkflow, input)
	if !environment.IsWorkflowCompleted() || environment.GetWorkflowError() != nil {
		t.Fatalf("Workflow error = %v", environment.GetWorkflowError())
	}
	var got ClaudeActivityResult
	if err := environment.GetWorkflowResult(&got); err != nil {
		t.Fatalf("Workflow result: %v", err)
	}
	if got != want {
		t.Fatalf("result = %+v, want %+v", got, want)
	}
	if calls != 1 {
		t.Fatalf("Activity calls = %d, want 1", calls)
	}
	if heartbeatTimeout < 15*time.Second {
		t.Fatalf("Activity heartbeat timeout = %s, want at least 15s dispatch margin", heartbeatTimeout)
	}
}

func TestReplayWorkflowHistoryRejectsMalformedExport(t *testing.T) {
	t.Parallel()

	if err := replayWorkflowHistory([]byte(`{"events":[`)); err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("malformed history replay error = %v", err)
	}
}
