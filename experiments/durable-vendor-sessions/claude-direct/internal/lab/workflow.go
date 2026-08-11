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
	activityContext := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ActivityID:             directClaudeActivityID,
		ScheduleToCloseTimeout: 10 * time.Minute,
		StartToCloseTimeout:    5 * time.Minute,
		HeartbeatTimeout:       directClaudeHeartbeatTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 100 * time.Millisecond, MaximumInterval: 100 * time.Millisecond,
			BackoffCoefficient: 1, MaximumAttempts: 2,
		},
	})
	var result ClaudeActivityResult
	if err := workflow.ExecuteActivity(activityContext, RunClaudeActivityName, input).Get(activityContext, &result); err != nil {
		return ClaudeActivityResult{}, fmt.Errorf("run unsafe direct Claude turn: %w", err)
	}
	return result, nil
}
