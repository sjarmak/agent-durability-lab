package lab

import (
	"path/filepath"
	"runtime"
	"testing"

	sandboxworkflow "github.com/temporal-community/sandbox-orchestration-harness/sdk/workflow"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func TestCurrentWorkflowsReplayCapturedParentCloseHistories(t *testing.T) {
	t.Parallel()
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(parentSandboxWorkflow, workflow.RegisterOptions{Name: parentWorkflow})
	replayer.RegisterWorkflowWithOptions(
		sandboxworkflow.SandboxWorkflow,
		workflow.RegisterOptions{Name: sandboxworkflow.SandboxWorkflowType},
	)
	for _, name := range []string{"parent-history.json", "child-history.json"} {
		if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, parentCloseHistoryPath(t, name)); err != nil {
			t.Fatalf("replay %s: %v", name, err)
		}
	}
}

func parentCloseHistoryPath(t *testing.T, name string) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate replay test source")
	}
	return filepath.Join(
		filepath.Dir(file), "..", "..", "evidence", "sandbox-harness-20260808-v7",
		"sandbox-harness-parent-close-protected-trial-1", name,
	)
}
