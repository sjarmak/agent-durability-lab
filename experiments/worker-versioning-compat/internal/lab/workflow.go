package lab

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

func activityRegistrationOptions() activity.RegisterOptions {
	return activity.RegisterOptions{Name: ActivityName}
}

func CompatibleWorkflow(ctx workflow.Context, input WorkflowInput) (WorkflowResult, error) {
	result := WorkflowResult{}
	options := workflow.ActivityOptions{
		ActivityID:          "attach-agent",
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	}
	ctx = workflow.WithActivityOptions(ctx, options)
	for phase := 1; phase <= input.Phases; phase++ {
		if phase > 1 {
			workflow.GetSignalChannel(ctx, continueSignal).Receive(ctx, nil)
		}
		phaseContext := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID: fmt.Sprintf("attach-agent-phase-%d", phase), StartToCloseTimeout: options.StartToCloseTimeout,
			RetryPolicy: options.RetryPolicy,
		})
		var receipt ActivityReceipt
		if err := workflow.ExecuteActivity(phaseContext, ActivityName, ActivityInput{
			SessionID: input.SessionID, RegistryPath: input.RegistryPath, Phase: phase,
		}).Get(phaseContext, &receipt); err != nil {
			return WorkflowResult{}, err
		}
		result.WorkflowBuilds = append(result.WorkflowBuilds, workflow.GetInfo(ctx).GetCurrentBuildID())
		result.Receipts = append(result.Receipts, receipt)
	}
	return result, nil
}
