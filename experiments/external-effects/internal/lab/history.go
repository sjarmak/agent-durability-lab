package lab

import (
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
)

func summarizeHistory(history *historypb.History) HistoryObservation {
	var observation HistoryObservation
	for _, event := range history.GetEvents() {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED:
			attributes := event.GetActivityTaskStartedEventAttributes()
			observation.CompletedAttempt = attributes.GetAttempt()
			if timeout := attributes.GetLastFailure().GetTimeoutFailureInfo(); timeout != nil &&
				timeout.GetTimeoutType() == enumspb.TIMEOUT_TYPE_START_TO_CLOSE {
				observation.RetryTimedOut = true
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
			observation.CompletedCount++
		}
	}
	return observation
}
