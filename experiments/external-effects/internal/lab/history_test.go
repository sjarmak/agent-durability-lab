package lab

import (
	"testing"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
)

func TestSummarizeHistoryRecognizesCompactedRetry(t *testing.T) {
	t.Parallel()
	history := &historypb.History{Events: []*historypb.HistoryEvent{
		{
			EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
			Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{
				ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
					Attempt: 2,
					LastFailure: &failurepb.Failure{FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
						TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE},
					}},
				},
			},
		},
		{EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED},
	}}
	observation := summarizeHistory(history)
	if !observation.RetryTimedOut || observation.CompletedCount != 1 || observation.CompletedAttempt != 2 {
		t.Fatalf("observation = %+v", observation)
	}
}
