package lab

import (
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
)

func VerifyHistory(scenario Scenario, waitForCancellation bool, history *historypb.History) (HistoryObservation, []string) {
	var observation HistoryObservation
	for _, event := range history.GetEvents() {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
			observation.WorkflowCancelRequested++
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
			observation.WorkflowCanceled++
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED:
			observation.ActivityCancelRequested++
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
			observation.ActivityCanceled++
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
			observation.ActivityScheduled++
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
			observation.ActivityCompleted++
		}
	}
	failures := make([]string, 0)
	expectCount := func(name string, got, want int) {
		if got != want {
			failures = append(failures, fmt.Sprintf("Temporal history %s = %d; want %d", name, got, want))
		}
	}
	expectCount("Workflow cancel requests", observation.WorkflowCancelRequested, 1)
	expectCount("Workflow canceled events", observation.WorkflowCanceled, 1)
	expectCount("Activity cancel requests", observation.ActivityCancelRequested, 1)
	expectedActivities := 1
	expectedCompletions := 0
	if scenario.Safe() {
		expectedActivities = 2
		expectedCompletions = 1
	}
	expectCount("Activity schedules", observation.ActivityScheduled, expectedActivities)
	expectCount("Activity completions", observation.ActivityCompleted, expectedCompletions)
	expectedCanceled := 0
	if waitForCancellation {
		expectedCanceled = 1
	}
	expectCount("Activity canceled events", observation.ActivityCanceled, expectedCanceled)
	return observation, failures
}
