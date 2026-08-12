package lab

import (
	"path/filepath"
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

type codexRegistrarCapture struct {
	workflows  []workflow.RegisterOptions
	activities []activity.RegisterOptions
}

func (r *codexRegistrarCapture) RegisterWorkflowWithOptions(_ any, options workflow.RegisterOptions) {
	r.workflows = append(r.workflows, options)
}

func (r *codexRegistrarCapture) RegisterActivityWithOptions(_ any, options activity.RegisterOptions) {
	r.activities = append(r.activities, options)
}

func TestRegisterWorkerBindsStableWorkflowAndActivityNames(t *testing.T) {
	root := t.TempDir()
	activities := testActivities(root)
	config := WorkerConfig{
		Command: activities.Command, LauncherBinary: "/opt/launcher", FaultBoundary: FaultNone,
		EffectBinary: activities.EffectBinary, DestinationPath: activities.DestinationPath,
		WorkspacePath: activities.WorkspacePath, EffectPayload: activities.EffectPayload,
		BarrierURL: activities.BarrierURL, BarrierPoint: activities.BarrierPoint,
		RunRoot: filepath.Join(root, "attempts"), WorkerID: "worker-one",
	}
	registrar := &codexRegistrarCapture{}
	if err := RegisterWorker(registrar, config); err != nil {
		t.Fatalf("register: %v", err)
	}
	if len(registrar.workflows) != 1 || registrar.workflows[0].Name != CodexWorkflowName ||
		len(registrar.activities) != 1 || registrar.activities[0].Name != RunCodexActivityName {
		t.Fatalf("registrations = %+v / %+v", registrar.workflows, registrar.activities)
	}
}
