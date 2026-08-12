package lab

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	CodexWorkflowName      = "CodexDurabilityWorkflow"
	RunCodexActivityName   = "RunCodexDurabilityActivity"
	codexActivityID        = "codex-logical-turn"
	codexHeartbeatTimeout  = 20 * time.Second
	codexActivityStartTime = 5 * time.Minute
)

func CodexWorkflow(ctx workflow.Context, input CodexActivityInput) (CodexActivityResult, error) {
	activityContext := workflow.WithActivityOptions(ctx, codexActivityOptions(input))
	waitContext := activityContext
	if input.RecoveryMode.normalized() == RecoveryModeFenced {
		waitContext, _ = workflow.NewDisconnectedContext(ctx)
	}
	var result CodexActivityResult
	if err := workflow.ExecuteActivity(activityContext, RunCodexActivityName, input).Get(waitContext, &result); err != nil {
		return CodexActivityResult{}, fmt.Errorf("run Codex logical turn: %w", err)
	}
	return result, nil
}

func codexActivityOptions(input CodexActivityInput) workflow.ActivityOptions {
	return workflow.ActivityOptions{
		ActivityID: codexActivityID, ScheduleToCloseTimeout: 10 * time.Minute,
		StartToCloseTimeout: codexActivityStartTime, HeartbeatTimeout: codexHeartbeatTimeout,
		WaitForCancellation: input.RecoveryMode.normalized() == RecoveryModeFenced,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 100 * time.Millisecond, MaximumInterval: 100 * time.Millisecond,
			BackoffCoefficient: 1, MaximumAttempts: 2,
		},
	}
}
