package temporaladapter

import (
	"context"
	"fmt"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/systemplan"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const WorkflowName = "AgentDurabilityBenchmarkV2"

type Receipt struct {
	StepID  string `json:"step_id"`
	Kind    string `json:"kind"`
	Attempt int32  `json:"attempt"`
}

type Result struct {
	Receipts []Receipt `json:"receipts"`
}

func Workflow(ctx workflow.Context, plan systemplan.Plan) (Result, error) {
	if err := plan.Validate(); err != nil {
		return Result{}, err
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Millisecond, BackoffCoefficient: 1, MaximumAttempts: 2,
		},
	})
	result := Result{Receipts: make([]Receipt, 0, len(plan.Steps))}
	for _, step := range plan.Steps {
		var receipt Receipt
		if err := workflow.ExecuteActivity(ctx, RecordStep, step).Get(ctx, &receipt); err != nil {
			return Result{}, fmt.Errorf("execute durable step %s: %w", step.ID, err)
		}
		result.Receipts = append(result.Receipts, receipt)
	}
	return result, nil
}

func RecordStep(ctx context.Context, step systemplan.Step) (Receipt, error) {
	info := activity.GetInfo(ctx)
	if step.FailureOnce && info.Attempt == 1 {
		return Receipt{}, temporal.NewApplicationError("injected exact-boundary delivery failure", "InjectedBoundaryFailure")
	}
	return Receipt{StepID: step.ID, Kind: step.Kind, Attempt: info.Attempt}, nil
}
