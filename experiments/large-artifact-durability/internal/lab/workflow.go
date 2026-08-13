package lab

import (
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	workflowName            = "LargeArtifactDurabilityWorkflow"
	produceActivityName     = "ProduceLargeArtifact"
	acknowledgeActivityName = "AcknowledgeLargeArtifact"
	produceActivityID       = "produce-artifact"
	acknowledgeActivityID   = "acknowledge-artifact"
)

func artifactWorkflow(ctx workflow.Context, input WorkflowInput) (WorkflowResult, error) {
	if err := validateWorkflowInput(input); err != nil {
		return WorkflowResult{}, err
	}
	options := workflow.ActivityOptions{
		ScheduleToCloseTimeout: 30 * time.Second,
		StartToCloseTimeout:    10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    100 * time.Millisecond,
			MaximumInterval:    100 * time.Millisecond,
			BackoffCoefficient: 1,
			MaximumAttempts:    2,
		},
	}
	produceContext := workflow.WithActivityOptions(ctx, options)
	produceContext = workflow.WithActivityOptions(produceContext, withActivityID(options, produceActivityID))
	var reference ArtifactReference
	if err := workflow.ExecuteActivity(produceContext, produceActivityName, input).Get(produceContext, &reference); err != nil {
		return WorkflowResult{}, fmt.Errorf("produce large artifact: %w", err)
	}

	acknowledgeContext := workflow.WithActivityOptions(ctx, withActivityID(options, acknowledgeActivityID))
	var acknowledgement Acknowledgement
	if err := workflow.ExecuteActivity(acknowledgeContext, acknowledgeActivityName, ConsumeInput{
		StoreRoot:       input.StoreRoot,
		Reference:       reference,
		ConsumerID:      input.ConsumerID,
		Mode:            input.Mode,
		FailureBoundary: input.FailureBoundary,
	}).Get(acknowledgeContext, &acknowledgement); err != nil {
		return WorkflowResult{}, fmt.Errorf("acknowledge large artifact: %w", err)
	}
	return WorkflowResult{Reference: reference, Acknowledgement: acknowledgement}, nil
}

func withActivityID(options workflow.ActivityOptions, activityID string) workflow.ActivityOptions {
	options.ActivityID = activityID
	return options
}

func validateWorkflowInput(input WorkflowInput) error {
	if !input.Mode.valid() || !safeComponent(input.LogicalID) || !safeComponent(input.ConsumerID) ||
		!filepath.IsAbs(input.StoreRoot) || !filepath.IsAbs(input.SourcePath) {
		return fmt.Errorf("%w: workflow requires absolute store/source paths and valid identities", ErrInvalidArtifact)
	}
	if input.FailureBoundary != "" && !input.FailureBoundary.valid() {
		return errors.New("workflow requires a supported failure boundary")
	}
	return nil
}

func (b Boundary) valid() bool {
	switch b {
	case BoundaryBlobPublished, BoundaryReferenceCreated, BoundaryReferencePublished,
		BoundaryActivityCompleted, BoundaryAcknowledgementPublished:
		return true
	default:
		return false
	}
}

func (b Boundary) Valid() bool {
	return b.valid() || b == BoundaryExternalStorageStored
}
