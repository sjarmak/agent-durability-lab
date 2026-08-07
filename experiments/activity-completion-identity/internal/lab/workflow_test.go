package lab

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func TestWorkflowConfiguresStableActivityIdentityAndTimeout(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	infoChannel := make(chan activity.Info, 1)
	environment.RegisterActivityWithOptions(
		func(ctx context.Context) (string, error) {
			infoChannel <- activity.GetInfo(ctx)
			return "done", nil
		},
		activity.RegisterOptions{Name: activityName},
	)
	environment.ExecuteWorkflow(completionIdentityWorkflow)
	if err := environment.GetWorkflowError(); err != nil {
		t.Fatalf("workflow failed: %v", err)
	}
	var result string
	if err := environment.GetWorkflowResult(&result); err != nil {
		t.Fatalf("workflow result: %v", err)
	}
	if result != "done" {
		t.Fatalf("result = %q; want done", result)
	}
	info := <-infoChannel
	if info.ActivityID != activityID {
		t.Fatalf("ActivityID = %q; want %q", info.ActivityID, activityID)
	}
	if info.StartToCloseTimeout != time.Second {
		t.Fatalf("StartToCloseTimeout = %s; want 1s", info.StartToCloseTimeout)
	}
}

func TestWorkflowReplaysCapturedCompletionIdentityHistory(t *testing.T) {
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(
		completionIdentityWorkflow,
		workflow.RegisterOptions{Name: workflowName},
	)
	if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, completionIdentityHistoryPath(t)); err != nil {
		t.Fatalf("replay completion identity Workflow: %v", err)
	}
}

func completionIdentityHistoryPath(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate replay test source")
	}
	return filepath.Join(
		filepath.Dir(file), "..", "..", "evidence",
		"completion-identity-20260806-v2-stale-by-id-trial-1", "temporal-history.json",
	)
}
