package lab

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	externalWorkflowName        = "LargeArtifactExternalStorageWorkflow"
	externalPayloadActivityName = "ReturnLargeArtifactPayload"
	externalPayloadActivityID   = "return-large-payload"
)

func externalStorageWorkflow(ctx workflow.Context, input ExternalWorkflowInput) (ExternalWorkflowResult, error) {
	if !filepath.IsAbs(input.SourcePath) {
		return ExternalWorkflowResult{}, errors.New("external-storage Workflow requires an absolute source path")
	}
	activityContext := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ActivityID:             externalPayloadActivityID,
		ScheduleToCloseTimeout: 20 * time.Second,
		StartToCloseTimeout:    5 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    100 * time.Millisecond,
			MaximumInterval:    100 * time.Millisecond,
			BackoffCoefficient: 1,
			MaximumAttempts:    2,
		},
	})
	var content []byte
	if err := workflow.ExecuteActivity(activityContext, externalPayloadActivityName, input).Get(activityContext, &content); err != nil {
		return ExternalWorkflowResult{}, fmt.Errorf("retrieve large Activity payload: %w", err)
	}
	digest := sha256.Sum256(content)
	return ExternalWorkflowResult{Digest: hex.EncodeToString(digest[:]), Size: int64(len(content))}, nil
}

type ExternalActivities struct{}

func (ExternalActivities) ReturnPayload(_ context.Context, input ExternalWorkflowInput) ([]byte, error) {
	if !filepath.IsAbs(input.SourcePath) {
		return nil, errors.New("large-payload Activity requires an absolute source path")
	}
	return readSourceArtifact(input.SourcePath)
}
