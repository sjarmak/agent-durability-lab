package lab

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	DirectClaudeWorkflowName     = "DirectClaudeUnsafeWorkflow"
	RunClaudeActivityName        = "RunClaudeUnsafeActivity"
	directClaudeActivityID       = "direct-claude-turn"
	directClaudeHeartbeatTimeout = 15 * time.Second
)

func DirectClaudeWorkflow(ctx workflow.Context, input ClaudeActivityInput) (ClaudeActivityResult, error) {
	activityContext := workflow.WithActivityOptions(ctx, directClaudeActivityOptions(input))
	var result ClaudeActivityResult
	future := workflow.ExecuteActivity(activityContext, RunClaudeActivityName, input)
	waitContext := activityContext
	if input.RecoveryMode.normalized() == RecoveryModeFenced {
		waitContext, _ = workflow.NewDisconnectedContext(ctx)
	}
	err := future.Get(waitContext, &result)
	if err != nil {
		return ClaudeActivityResult{}, fmt.Errorf("run direct Claude turn: %w", err)
	}
	return result, nil
}

func directClaudeActivityOptions(input ClaudeActivityInput) workflow.ActivityOptions {
	return workflow.ActivityOptions{
		ActivityID:             directClaudeActivityID,
		ScheduleToCloseTimeout: 10 * time.Minute,
		StartToCloseTimeout:    5 * time.Minute,
		HeartbeatTimeout:       directClaudeHeartbeatTimeout,
		WaitForCancellation:    input.RecoveryMode.normalized() == RecoveryModeFenced,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 100 * time.Millisecond, MaximumInterval: 100 * time.Millisecond,
			BackoffCoefficient: 1, MaximumAttempts: 2,
		},
	}
}
