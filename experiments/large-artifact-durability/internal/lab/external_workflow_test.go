package lab

import (
	"context"
	"errors"
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestExternalStorageWorkflowReturnsDigestOfLargeActivityResult(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	content := testArtifactRequest(ModeProtected, 1).Content
	environment.RegisterActivityWithOptions(
		func(context.Context, ExternalWorkflowInput) ([]byte, error) { return content, nil },
		activity.RegisterOptions{Name: externalPayloadActivityName},
	)

	environment.ExecuteWorkflow(externalStorageWorkflow, ExternalWorkflowInput{SourcePath: "/sealed/input/large-output.bin"})
	if err := environment.GetWorkflowError(); err != nil {
		t.Fatalf("Workflow failed: %v", err)
	}
	var result ExternalWorkflowResult
	if err := environment.GetWorkflowResult(&result); err != nil {
		t.Fatalf("Workflow result: %v", err)
	}
	if result.Digest != digestBytes(content) || result.Size != int64(len(content)) {
		t.Fatalf("result = %+v", result)
	}
}

func TestExternalStorageWorkflowPropagatesActivityFailure(t *testing.T) {
	t.Parallel()

	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	environment.RegisterActivityWithOptions(
		func(context.Context, ExternalWorkflowInput) ([]byte, error) { return nil, errors.New("payload failed") },
		activity.RegisterOptions{Name: externalPayloadActivityName},
	)
	environment.ExecuteWorkflow(externalStorageWorkflow, ExternalWorkflowInput{SourcePath: "/sealed/input.bin"})
	if err := environment.GetWorkflowError(); err == nil {
		t.Fatal("external payload Activity failure was swallowed")
	}
}
