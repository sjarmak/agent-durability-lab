package lab

import (
	"testing"

	enumspb "go.temporal.io/api/enums/v1"
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
}

func TestVerifyHistoryRejectsMissingCancellationEvidence(t *testing.T) {
	_, failures := VerifyHistory(ScenarioHealthySafe, true, historyWithTypes(
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
	))
	if len(failures) == 0 {
		t.Fatal("incomplete history passed verification")
	}
}

func historyWithTypes(types ...enumspb.EventType) *historypb.History {
	history := &historypb.History{Events: make([]*historypb.HistoryEvent, 0, len(types))}
	for _, eventType := range types {
		history.Events = append(history.Events, &historypb.HistoryEvent{EventType: eventType})
	}
	return history
}
