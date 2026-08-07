package lab

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	workflowName                = "ExternalEffectAmbiguityWorkflow"
	activityName                = "ApplyExternalEffect"
	activityID                  = "external-effect"
	activityStartToCloseTimeout = 5 * time.Second
)

func externalEffectWorkflow(ctx workflow.Context, input WorkflowInput) (string, error) {
	activityContext := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ActivityID:             activityID,
		ScheduleToCloseTimeout: 10 * time.Second,
		StartToCloseTimeout:    activityStartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    100 * time.Millisecond,
			MaximumInterval:    100 * time.Millisecond,
			BackoffCoefficient: 1,
			MaximumAttempts:    2,
		},
	})
	var receipt string
	if err := workflow.ExecuteActivity(activityContext, activityName, input).Get(activityContext, &receipt); err != nil {
		return "", fmt.Errorf("apply external effect: %w", err)
	}
	return receipt, nil
}

type workerRegistrar interface {
	RegisterWorkflowWithOptions(interface{}, workflow.RegisterOptions)
	RegisterActivityWithOptions(interface{}, activity.RegisterOptions)
}

func RegisterWorker(temporalWorker workerRegistrar, workerID string) {
	temporalWorker.RegisterWorkflowWithOptions(
		externalEffectWorkflow,
		workflow.RegisterOptions{Name: workflowName},
	)
	temporalWorker.RegisterActivityWithOptions(
		Activities{WorkerID: workerID}.Apply,
		activity.RegisterOptions{Name: activityName},
	)
}
