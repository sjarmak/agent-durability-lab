package lab

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	workflowName = "ActivityCompletionIdentityWorkflow"
	activityName = "PendingCompletionActivity"
	activityID   = "completion-target"
)

func completionIdentityWorkflow(ctx workflow.Context) (string, error) {
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ActivityID:             activityID,
		ScheduleToCloseTimeout: 10 * time.Second,
		StartToCloseTimeout:    time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    100 * time.Millisecond,
			MaximumInterval:    100 * time.Millisecond,
			BackoffCoefficient: 1,
			MaximumAttempts:    2,
		},
	})
	var result string
	if err := workflow.ExecuteActivity(activityCtx, activityName).Get(activityCtx, &result); err != nil {
		return "", fmt.Errorf("await asynchronous Activity completion: %w", err)
	}
	return result, nil
}
