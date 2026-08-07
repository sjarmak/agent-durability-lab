package lab

import (
	"reflect"
	"testing"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
)

func TestSummarizeHistoryReadsCompactedRetryFailure(t *testing.T) {
	history := &historypb.History{Events: []*historypb.HistoryEvent{
		{
			EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
			Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{
				ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
					Attempt: 2,
					LastFailure: &failurepb.Failure{
						FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
							TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
								TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
							},
						},
					},
				},
			},
		},
		{
			EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
			Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{
				ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{},
			},
		},
	}}
	want := HistoryObservation{
		StartedAttempts: []int32{2},
		RetryFailures: []HistoryRetryObservation{{
			StartedAttempt: 2, TimeoutType: "StartToClose",
		}},
		CompletedCount: 1,
	}
	if got := summarizeHistory(history); !reflect.DeepEqual(got, want) {
		t.Fatalf("summarizeHistory() = %+v; want %+v", got, want)
	}
}
