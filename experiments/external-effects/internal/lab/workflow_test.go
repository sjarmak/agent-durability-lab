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

func TestWorkflowUsesStableActivityIDAndRetryTimeout(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	infoChannel := make(chan activity.Info, 1)
	environment.RegisterActivityWithOptions(func(ctx context.Context, input WorkflowInput) (string, error) {
		infoChannel <- activity.GetInfo(ctx)
		return "receipt", nil
	}, activity.RegisterOptions{Name: activityName})

	environment.ExecuteWorkflow(externalEffectWorkflow, WorkflowInput{})
	if err := environment.GetWorkflowError(); err != nil {
		t.Fatalf("workflow failed: %v", err)
	}
	info := <-infoChannel
	if info.ActivityID != activityID {
		t.Fatalf("ActivityID = %q, want %q", info.ActivityID, activityID)
	}
	if info.StartToCloseTimeout != 5*time.Second {
		t.Fatalf("StartToCloseTimeout = %s, want 5s", info.StartToCloseTimeout)
	}
}

func TestWorkflowReplaysCapturedExternalEffectHistory(t *testing.T) {
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(
		externalEffectWorkflow,
		workflow.RegisterOptions{Name: workflowName},
	)
	if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, externalEffectHistoryPath(t)); err != nil {
		t.Fatalf("replay external-effect Workflow: %v", err)
	}
}

func TestRegisterWorkerUsesStableWorkflowAndActivityNames(t *testing.T) {
	t.Parallel()
	registry := &recordingRegistry{}
	RegisterWorker(registry, "worker-17")
	if registry.workflowName != workflowName || registry.activityName != activityName {
		t.Fatalf("registered names = %q, %q", registry.workflowName, registry.activityName)
	}
	if registry.workflow == nil || registry.activity == nil {
		t.Fatalf("registered values = %#v, %#v", registry.workflow, registry.activity)
	}
}

type recordingRegistry struct {
	workflow     interface{}
	workflowName string
	activity     interface{}
	activityName string
}

func (r *recordingRegistry) RegisterWorkflowWithOptions(value interface{}, options workflow.RegisterOptions) {
	r.workflow = value
	r.workflowName = options.Name
}

func (r *recordingRegistry) RegisterActivityWithOptions(value interface{}, options activity.RegisterOptions) {
	r.activity = value
	r.activityName = options.Name
}

func externalEffectHistoryPath(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate replay test source")
	}
	return filepath.Join(
		filepath.Dir(file), "..", "..", "evidence",
		"external-effects-20260806-v2-git-protected-trial-1", "temporal-history.json",
	)
}
