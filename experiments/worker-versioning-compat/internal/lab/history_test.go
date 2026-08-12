package lab

import (
	"testing"

	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/failure/v1"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
)

func TestInspectActivityHistoryBindsScheduleStartAndReceipt(t *testing.T) {
	historyValue := syntheticActivityHistory(t, false)
	receipts, err := inspectActivityHistory(historyValue, ScenarioAutoCompatible, "workflow-1", "run-1", "/registry", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(receipts) != 2 || receipts[1].WorkerBuild != "worker-v2" {
		t.Fatalf("receipts = %+v", receipts)
	}
	for name, mutate := range map[string]func(*history.History){
		"wrong-workflow": func(value *history.History) {
			value.Events[2].GetActivityTaskCompletedEventAttributes().Result = payloads(t, ActivityReceipt{SessionID: "session-1", WorkflowID: "forged", RunID: "run-1", WorkerBuild: "worker-v1", AgentBuild: "agent-v1", Action: ActionStarted, TemporalAttempt: 1, Phase: 1})
		},
		"wrong-worker": func(value *history.History) {
			value.Events[1].GetActivityTaskStartedEventAttributes().Identity = "forged"
		},
		"wrong-activity": func(value *history.History) {
			value.Events[0].GetActivityTaskScheduledEventAttributes().ActivityId = "forged"
		},
	} {
		t.Run(name, func(t *testing.T) {
			copy := syntheticActivityHistory(t, false)
			mutate(copy)
			if _, err := inspectActivityHistory(copy, ScenarioAutoCompatible, "workflow-1", "run-1", "/registry", false); err == nil {
				t.Fatal("history mutation accepted")
			}
		})
	}
}

func TestInspectActivityHistoryAcceptsOnlyExactIncompatibleFailure(t *testing.T) {
	historyValue := syntheticActivityHistory(t, true)
	if _, err := inspectActivityHistory(historyValue, ScenarioAutoIncompatible, "workflow-1", "run-1", "/registry", true); err != nil {
		t.Fatal(err)
	}
	historyValue.Events[5].GetActivityTaskFailedEventAttributes().Failure.GetApplicationFailureInfo().Type = "wrong"
	if _, err := inspectActivityHistory(historyValue, ScenarioAutoIncompatible, "workflow-1", "run-1", "/registry", true); err == nil {
		t.Fatal("wrong incompatible failure accepted")
	}
}

func TestInspectWorkflowStartBindsExecutionAndConfinedRegistry(t *testing.T) {
	makeHistory := func() (*history.History, *history.WorkflowExecutionStartedEventAttributes) {
		input := WorkflowInput{SessionID: "session-1", RegistryPath: "evidence/auto-compatible-trial-1-registry.db", Phases: 2}
		started := &history.WorkflowExecutionStartedEventAttributes{
			WorkflowId: "population-auto-compatible-trial-1", OriginalExecutionRunId: "run-1", FirstExecutionRunId: "run-1", Input: payloads(t, input),
		}
		value := &history.History{Events: []*history.HistoryEvent{{EventId: 1, EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, Attributes: &history.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: started}}}}
		return value, started
	}
	value, started := makeHistory()
	if _, err := inspectWorkflowStart(value, ScenarioAutoCompatible, 1, "population", started.WorkflowId, "run-1"); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*history.WorkflowExecutionStartedEventAttributes){
		"workflow": func(attributes *history.WorkflowExecutionStartedEventAttributes) { attributes.WorkflowId = "forged" },
		"first-run": func(attributes *history.WorkflowExecutionStartedEventAttributes) {
			attributes.FirstExecutionRunId = "forged"
		},
		"absolute-registry": func(attributes *history.WorkflowExecutionStartedEventAttributes) {
			attributes.Input = payloads(t, WorkflowInput{SessionID: "session-1", RegistryPath: "/tmp/auto-compatible-trial-1-registry.db", Phases: 2})
		},
	} {
		t.Run(name, func(t *testing.T) {
			mutatedHistory, mutatedStart := makeHistory()
			mutate(mutatedStart)
			if _, err := inspectWorkflowStart(mutatedHistory, ScenarioAutoCompatible, 1, "population", "population-auto-compatible-trial-1", "run-1"); err == nil {
				t.Fatal("workflow start mutation accepted")
			}
		})
	}
}

func syntheticActivityHistory(t *testing.T, incompatible bool) *history.History {
	t.Helper()
	firstInput := ActivityInput{SessionID: "session-1", RegistryPath: "/registry", Phase: 1}
	secondInput := ActivityInput{SessionID: "session-1", RegistryPath: "/registry", Phase: 2}
	firstReceipt := ActivityReceipt{SessionID: "session-1", WorkflowID: "workflow-1", RunID: "run-1", WorkerBuild: "worker-v1", AgentBuild: "agent-v1", Action: ActionStarted, TemporalAttempt: 1, Phase: 1}
	secondReceipt := ActivityReceipt{SessionID: "session-1", WorkflowID: "workflow-1", RunID: "run-1", WorkerBuild: "worker-v2", AgentBuild: "agent-v1", Action: ActionAttached, TemporalAttempt: 1, Phase: 2}
	events := []*history.HistoryEvent{
		{EventId: 1, EventType: enums.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, Attributes: &history.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: scheduled("attach-agent-phase-1", firstInput, t)}},
		{EventId: 2, EventType: enums.EVENT_TYPE_ACTIVITY_TASK_STARTED, Attributes: &history.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &history.ActivityTaskStartedEventAttributes{ScheduledEventId: 1, Identity: "worker-v1", Attempt: 1}}},
		{EventId: 3, EventType: enums.EVENT_TYPE_ACTIVITY_TASK_COMPLETED, Attributes: &history.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &history.ActivityTaskCompletedEventAttributes{ScheduledEventId: 1, StartedEventId: 2, Result: payloads(t, firstReceipt)}}},
		{EventId: 4, EventType: enums.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, Attributes: &history.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: scheduled("attach-agent-phase-2", secondInput, t)}},
		{EventId: 5, EventType: enums.EVENT_TYPE_ACTIVITY_TASK_STARTED, Attributes: &history.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &history.ActivityTaskStartedEventAttributes{ScheduledEventId: 4, Identity: "worker-v2", Attempt: 1}}},
	}
	if incompatible {
		events[4].GetActivityTaskStartedEventAttributes().Identity = "worker-v3"
		events = append(events, &history.HistoryEvent{EventId: 6, EventType: enums.EVENT_TYPE_ACTIVITY_TASK_FAILED, Attributes: &history.HistoryEvent_ActivityTaskFailedEventAttributes{ActivityTaskFailedEventAttributes: &history.ActivityTaskFailedEventAttributes{ScheduledEventId: 4, StartedEventId: 5, Failure: &failure.Failure{FailureInfo: &failure.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failure.ApplicationFailureInfo{Type: "incompatible-agent-build"}}}}}})
	} else {
		events = append(events, &history.HistoryEvent{EventId: 6, EventType: enums.EVENT_TYPE_ACTIVITY_TASK_COMPLETED, Attributes: &history.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &history.ActivityTaskCompletedEventAttributes{ScheduledEventId: 4, StartedEventId: 5, Result: payloads(t, secondReceipt)}}})
	}
	return &history.History{Events: events}
}

func scheduled(id string, input ActivityInput, t *testing.T) *history.ActivityTaskScheduledEventAttributes {
	return &history.ActivityTaskScheduledEventAttributes{ActivityId: id, ActivityType: &common.ActivityType{Name: ActivityName}, TaskQueue: &taskqueue.TaskQueue{Name: "queue"}, Input: payloads(t, input)}
}

func payloads(t *testing.T, value any) *common.Payloads {
	t.Helper()
	result, err := converter.GetDefaultDataConverter().ToPayloads(value)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

var _ = workflowservice.GetSystemInfoRequest{}
