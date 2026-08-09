package lab

import (
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

func TestRegisterWorkerUsesStableNamesAndCompleteConfiguration(t *testing.T) {
	t.Parallel()

	registry := &recordingWorkerRegistry{}
	config := WorkerConfig{
		Command: ClaudeCommand{
			Binary: "/opt/claude", WorkDir: "/tmp/fixture", Model: "haiku",
			MaxBudgetUSD: "0.25", MaxTurns: 2,
		},
		LauncherBinary: "/opt/launcher", FaultBoundary: FaultAfterToolEffect,
		EffectBinary: "/opt/effect", DestinationPath: "/tmp/destination.db",
		WorkspacePath: "/tmp/fixture/effects.jsonl", EffectPayload: "controlled-edit",
		BarrierURL: "http://127.0.0.1:8080", BarrierPoint: "effect-committed",
		RunRoot: "/tmp/runs", WorkerID: "worker-one",
	}
	if err := RegisterWorker(registry, config); err != nil {
		t.Fatalf("register Worker: %v", err)
	}
	if registry.workflowName != DirectClaudeWorkflowName || registry.activityName != RunClaudeActivityName {
		t.Fatalf("registered names = %q, %q", registry.workflowName, registry.activityName)
	}
	if registry.workflowValue == nil || registry.activityValue == nil {
		t.Fatalf("registered values = %#v, %#v", registry.workflowValue, registry.activityValue)
	}
	if err := RegisterWorker(&recordingWorkerRegistry{}, WorkerConfig{}); err == nil {
		t.Fatal("incomplete Worker configuration returned nil error")
	}
}

type recordingWorkerRegistry struct {
	workflowName  string
	activityName  string
	workflowValue any
	activityValue any
}

func (r *recordingWorkerRegistry) RegisterWorkflowWithOptions(value any, options workflow.RegisterOptions) {
	r.workflowValue = value
	r.workflowName = options.Name
}

func (r *recordingWorkerRegistry) RegisterActivityWithOptions(value any, options activity.RegisterOptions) {
	r.activityValue = value
	r.activityName = options.Name
}
