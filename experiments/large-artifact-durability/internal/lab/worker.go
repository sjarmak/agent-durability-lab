package lab

import (
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

type WorkerRegistry interface {
	RegisterWorkflowWithOptions(interface{}, workflow.RegisterOptions)
	RegisterActivityWithOptions(interface{}, activity.RegisterOptions)
}

func RegisterWorker(registry WorkerRegistry, activities Activities, external ExternalActivities) {
	registry.RegisterWorkflowWithOptions(artifactWorkflow, workflow.RegisterOptions{Name: workflowName})
	registry.RegisterWorkflowWithOptions(externalStorageWorkflow, workflow.RegisterOptions{Name: externalWorkflowName})
	registry.RegisterActivityWithOptions(activities.Produce, activity.RegisterOptions{Name: produceActivityName})
	registry.RegisterActivityWithOptions(activities.Acknowledge, activity.RegisterOptions{Name: acknowledgeActivityName})
	registry.RegisterActivityWithOptions(external.ReturnPayload, activity.RegisterOptions{Name: externalPayloadActivityName})
}
