package lab

import (
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

func TestRegisterWorkerPinsBothWorkflowAndActivityProtocols(t *testing.T) {
	t.Parallel()

	registry := &recordingRegistry{}
	RegisterWorker(registry, Activities{WorkerID: "worker-1"}, ExternalActivities{})
	wantWorkflows := []string{workflowName, externalWorkflowName}
	wantActivities := []string{produceActivityName, acknowledgeActivityName, externalPayloadActivityName}
	if !equalStrings(registry.workflows, wantWorkflows) || !equalStrings(registry.activities, wantActivities) {
		t.Fatalf("registered workflows=%v activities=%v", registry.workflows, registry.activities)
	}
}

type recordingRegistry struct {
	workflows  []string
	activities []string
}

func (r *recordingRegistry) RegisterWorkflowWithOptions(_ interface{}, options workflow.RegisterOptions) {
	r.workflows = append(r.workflows, options.Name)
}

func (r *recordingRegistry) RegisterActivityWithOptions(_ interface{}, options activity.RegisterOptions) {
	r.activities = append(r.activities, options.Name)
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
