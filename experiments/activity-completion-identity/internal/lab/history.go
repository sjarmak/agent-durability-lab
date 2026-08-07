package lab

import (
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
)

func summarizeHistory(history *historypb.History) HistoryObservation {
	observation := HistoryObservation{
		StartedAttempts: make([]int32, 0, 2),
		RetryFailures:   make([]HistoryRetryObservation, 0, 1),
	}
	for _, event := range history.GetEvents() {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED:
			attributes := event.GetActivityTaskStartedEventAttributes()
			attempt := attributes.GetAttempt()
			observation.StartedAttempts = append(observation.StartedAttempts, attempt)
			if timeout := attributes.GetLastFailure().GetTimeoutFailureInfo(); timeout != nil {
				observation.RetryFailures = append(observation.RetryFailures, HistoryRetryObservation{
					StartedAttempt: attempt,
					TimeoutType:    timeout.GetTimeoutType().String(),
				})
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
			observation.CompletedCount++
		}
	}
	return observation
}
