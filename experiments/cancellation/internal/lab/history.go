package lab

import (
	"fmt"
	"strings"

	"github.com/sjarmak/temporal_projects/internal/temporalagent"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
)

func VerifyHistory(scenario Scenario, waitForCancellation bool, history *historypb.History) (HistoryObservation, []string) {
	facts := observeHistory(history)
	observation := facts.observation
	failures := verifyHistoryCounts(scenario, observation)
	if waitForCancellation {
		if scenario == ScenarioWorkerDeathSafe {
			failures = append(failures, verifyWorkerDeathTerminal(facts)...)
		} else {
			failures = append(failures, expectHistoryCount("Activity canceled events", observation.ActivityCanceled, 1)...)
			failures = append(failures, expectHistoryCount("Activity timed-out events", observation.ActivityTimedOut, 0)...)
		}
	} else {
		if observation.ActivityCanceled > 1 {
			failures = append(failures, fmt.Sprintf(
				"Temporal history Activity canceled events = %d; want at most 1 when cancellation acknowledgement is not awaited",
				observation.ActivityCanceled,
			))
		}
		failures = append(failures, expectHistoryCount("Activity timed-out events", observation.ActivityTimedOut, 0)...)
	}
	return observation, failures
}

type historyFacts struct {
	observation                 HistoryObservation
	workflowCancelRequestedID   int64
	activityCancelRequestedID   int64
	firstActivityScheduledID    int64
	secondActivityScheduledID   int64
	firstActivityID             string
	firstActivityType           string
	secondActivityID            string
	secondActivityType          string
	activityTimedOutID          int64
	timedOutActivityScheduledID int64
	timedOutActivityTimeoutType enumspb.TimeoutType
}

func observeHistory(history *historypb.History) historyFacts {
	var facts historyFacts
	for index, event := range history.GetEvents() {
		eventID := event.GetEventId()
		if eventID == 0 {
			eventID = int64(index + 1)
		}
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
			facts.observation.WorkflowCancelRequested++
			facts.workflowCancelRequestedID = eventID
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
			facts.observation.WorkflowCanceled++
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED:
			facts.observation.ActivityCancelRequested++
			facts.activityCancelRequestedID = eventID
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
			facts.observation.ActivityCanceled++
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
			facts.observation.ActivityTimedOut++
			facts.activityTimedOutID = eventID
			attributes := event.GetActivityTaskTimedOutEventAttributes()
			facts.timedOutActivityScheduledID = attributes.GetScheduledEventId()
			facts.timedOutActivityTimeoutType = failureTimeoutType(attributes.GetFailure())
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
			facts.observation.ActivityScheduled++
			attributes := event.GetActivityTaskScheduledEventAttributes()
			if facts.firstActivityScheduledID == 0 {
				facts.firstActivityScheduledID = eventID
				facts.firstActivityID = attributes.GetActivityId()
				facts.firstActivityType = attributes.GetActivityType().GetName()
			} else if facts.secondActivityScheduledID == 0 {
				facts.secondActivityScheduledID = eventID
				facts.secondActivityID = attributes.GetActivityId()
				facts.secondActivityType = attributes.GetActivityType().GetName()
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
			facts.observation.ActivityCompleted++
		}
	}
	return facts
}

func verifyHistoryCounts(scenario Scenario, observation HistoryObservation) []string {
	failures := make([]string, 0)
	failures = append(failures, expectHistoryCount("Workflow cancel requests", observation.WorkflowCancelRequested, 1)...)
	failures = append(failures, expectHistoryCount("Workflow canceled events", observation.WorkflowCanceled, 1)...)
	failures = append(failures, expectHistoryCount("Activity cancel requests", observation.ActivityCancelRequested, 1)...)
	expectedActivities := 1
	expectedCompletions := 0
	if scenario.Safe() {
		expectedActivities = 2
		expectedCompletions = 1
	}
	failures = append(failures, expectHistoryCount("Activity schedules", observation.ActivityScheduled, expectedActivities)...)
	failures = append(failures, expectHistoryCount("Activity completions", observation.ActivityCompleted, expectedCompletions)...)
	return failures
}

func verifyWorkerDeathTerminal(facts historyFacts) []string {
	failures := expectHistoryCount(
		"Activity canceled or timed-out terminal events",
		facts.observation.ActivityCanceled+facts.observation.ActivityTimedOut,
		1,
	)
	if facts.observation.ActivityTimedOut == 0 {
		return failures
	}
	if facts.activityTimedOutID <= facts.workflowCancelRequestedID || facts.activityTimedOutID <= facts.activityCancelRequestedID {
		failures = append(failures, "Temporal history Activity timeout did not follow both cancellation requests")
	}
	if facts.timedOutActivityScheduledID == 0 || facts.timedOutActivityScheduledID != facts.firstActivityScheduledID {
		failures = append(failures, "Temporal history Activity timeout does not target the primary agent Activity")
	}
	if !validWorkerDeathActivitySchedule(facts) {
		failures = append(failures, "Temporal history does not bind the primary and cleanup Activity identities")
	}
	if facts.timedOutActivityTimeoutType != enumspb.TIMEOUT_TYPE_HEARTBEAT {
		failures = append(failures, "Temporal history primary Activity terminal timeout is not a heartbeat timeout")
	}
	if facts.secondActivityScheduledID == 0 || facts.activityTimedOutID >= facts.secondActivityScheduledID {
		failures = append(failures, "Temporal history Activity timeout did not precede disconnected cleanup")
	}
	return failures
}

func validWorkerDeathActivitySchedule(facts historyFacts) bool {
	const primaryPrefix = "agent-session/"
	if facts.firstActivityType != temporalagent.ActivityName || facts.secondActivityType != temporalagent.CancelActivityName {
		return false
	}
	sessionID := strings.TrimPrefix(facts.firstActivityID, primaryPrefix)
	return sessionID != facts.firstActivityID && sessionID != "" &&
		facts.firstActivityID == temporalagent.ActivityID(sessionID) &&
		facts.secondActivityID == temporalagent.CancelActivityID(sessionID)
}

func expectHistoryCount(name string, got, want int) []string {
	if got == want {
		return nil
	}
	return []string{fmt.Sprintf("Temporal history %s = %d; want %d", name, got, want)}
}

func failureTimeoutType(failure *failurepb.Failure) enumspb.TimeoutType {
	for current := failure; current != nil; current = current.GetCause() {
		if info := current.GetTimeoutFailureInfo(); info != nil {
			return info.GetTimeoutType()
		}
	}
	return enumspb.TIMEOUT_TYPE_UNSPECIFIED
}
