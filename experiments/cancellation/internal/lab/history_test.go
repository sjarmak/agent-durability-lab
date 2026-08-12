package lab

import (
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
)

func TestVerifyHistoryDistinguishesWaitPolicyAndCleanup(t *testing.T) {
	base := []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
	}
	control := historyWithTypes(base...)
	observation, failures := VerifyHistory(ScenarioTemporalControl, false, control)
	if len(failures) != 0 || observation.ActivityCanceled != 0 {
		t.Fatalf("control history = %+v failures=%v", observation, failures)
	}

	safeTypes := append(append([]enumspb.EventType(nil), base...),
		enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
	)
	observation, failures = VerifyHistory(ScenarioHealthySafe, true, historyWithTypes(safeTypes...))
	if len(failures) != 0 || observation.ActivityCanceled != 1 || observation.ActivityScheduled != 2 {
		t.Fatalf("safe history = %+v failures=%v", observation, failures)
	}

	observation, failures = VerifyHistory(ScenarioHealthySafe, false, historyWithTypes(safeTypes...))
	if len(failures) != 0 || observation.ActivityCanceled != 1 {
		t.Fatalf("non-waiting safe history with prompt cancellation = %+v failures=%v", observation, failures)
	}
}

func TestVerifyHistoryRejectsMissingCancellationEvidence(t *testing.T) {
	_, failures := VerifyHistory(ScenarioHealthySafe, true, historyWithTypes(
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
	))
	if len(failures) == 0 {
		t.Fatal("incomplete history passed verification")
	}
}

func TestVerifyHistoryAcceptsWorkerDeathTimeoutAfterCancellationRequest(t *testing.T) {
	history := workerDeathTimeoutHistory(
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
	)
	observation, failures := VerifyHistory(ScenarioWorkerDeathSafe, true, history)
	if len(failures) != 0 || observation.ActivityTimedOut != 1 || observation.ActivityCanceled != 0 {
		t.Fatalf("worker-death timeout history = %+v failures=%v", observation, failures)
	}
	if _, failures := VerifyHistory(ScenarioHealthySafe, true, history); len(failures) == 0 {
		t.Fatal("healthy cancellation accepted an Activity timeout")
	}
}

func TestVerifyHistoryRejectsActivityTimeoutWithoutAwaitingCancellation(t *testing.T) {
	history := workerDeathTimeoutHistory(
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
	)
	for _, scenario := range []Scenario{ScenarioTemporalControl, ScenarioHealthySafe, ScenarioWorkerDeathSafe, ScenarioFrozenSafe} {
		if _, failures := VerifyHistory(scenario, false, history); len(failures) == 0 {
			t.Fatalf("%s accepted an Activity timeout without awaiting cancellation", scenario)
		}
	}
}

func TestVerifyHistoryRejectsWorkerDeathTimeoutBeforeCancellationRequest(t *testing.T) {
	history := workerDeathTimeoutHistory(
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
	)
	if _, failures := VerifyHistory(ScenarioWorkerDeathSafe, true, history); len(failures) == 0 {
		t.Fatal("worker-death cancellation accepted an Activity timeout before the cancellation request")
	}
}

func TestVerifyHistoryRejectsWorkerDeathTimeoutForCleanupActivity(t *testing.T) {
	history := workerDeathTimeoutHistory(
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
	)
	var scheduled []*historypb.ActivityTaskScheduledEventAttributes
	for _, event := range history.GetEvents() {
		if attributes := event.GetActivityTaskScheduledEventAttributes(); attributes != nil {
			scheduled = append(scheduled, attributes)
		}
	}
	scheduled[0].ActivityId, scheduled[1].ActivityId = scheduled[1].ActivityId, scheduled[0].ActivityId
	scheduled[0].ActivityType, scheduled[1].ActivityType = scheduled[1].ActivityType, scheduled[0].ActivityType
	if _, failures := VerifyHistory(ScenarioWorkerDeathSafe, true, history); len(failures) == 0 {
		t.Fatal("worker-death cancellation accepted a timeout targeting the cleanup Activity")
	}
}

func historyWithTypes(types ...enumspb.EventType) *historypb.History {
	history := &historypb.History{Events: make([]*historypb.HistoryEvent, 0, len(types))}
	for _, eventType := range types {
		history.Events = append(history.Events, &historypb.HistoryEvent{EventType: eventType})
	}
	return history
}

func workerDeathTimeoutHistory(types ...enumspb.EventType) *historypb.History {
	history := historyWithTypes(types...)
	var firstScheduledEventID int64
	scheduledCount := 0
	for index, event := range history.Events {
		event.EventId = int64(index + 1)
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
			scheduledCount++
			activityID := "agent-session/session-1"
			activityType := "RunDetachedAgent"
			if scheduledCount == 2 {
				activityID = "cancel-agent-session/session-1"
				activityType = "CancelDetachedAgent"
			}
			event.Attributes = &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
				ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
					ActivityId:   activityID,
					ActivityType: &commonpb.ActivityType{Name: activityType},
				},
			}
			if firstScheduledEventID == 0 {
				firstScheduledEventID = event.GetEventId()
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
			event.Attributes = &historypb.HistoryEvent_ActivityTaskTimedOutEventAttributes{
				ActivityTaskTimedOutEventAttributes: &historypb.ActivityTaskTimedOutEventAttributes{
					ScheduledEventId: firstScheduledEventID,
					Failure: &failurepb.Failure{FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
						TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{TimeoutType: enumspb.TIMEOUT_TYPE_HEARTBEAT},
					}},
				},
			}
		}
	}
	return history
}
