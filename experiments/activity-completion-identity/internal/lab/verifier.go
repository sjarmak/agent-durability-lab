package lab

import "fmt"

func Verify(evidence Evidence) Verdict {
	validationFailures := validateEvidence(evidence)
	expectedFailures := expectedObservationFailures(evidence)
	return Verdict{
		RunValid:            len(validationFailures) == 0,
		ExpectedObservation: len(validationFailures) == 0 && len(expectedFailures) == 0,
		InvariantSatisfied:  len(validationFailures) == 0 && staleAttemptCannotSelectOutcome(evidence),
		Failures:            append(validationFailures, expectedFailures...),
	}
}

func validateEvidence(evidence Evidence) []string {
	var failures []string
	if !evidence.Arm.Valid() {
		failures = append(failures, fmt.Sprintf("invalid arm %q", evidence.Arm))
	}
	if !containsAttempt(evidence.Attempts, 1) || !containsAttempt(evidence.Attempts, 2) {
		failures = append(failures, "attempt observations do not contain attempts 1 and 2")
	}
	if !containsInt32(evidence.History.StartedAttempts, 2) {
		failures = append(failures, "history does not contain attempt 2 start")
	}
	if !containsRetryTimeout(evidence.History.RetryFailures, 2, "StartToClose") {
		failures = append(failures, "attempt 2 history does not identify attempt 1 Start-to-Close timeout")
	}
	if evidence.History.CompletedCount != 1 {
		failures = append(failures, fmt.Sprintf("history completion count = %d; want 1", evidence.History.CompletedCount))
	}
	accepted := acceptedCompletions(evidence.Completions)
	if len(accepted) != 1 {
		failures = append(failures, fmt.Sprintf("accepted completion count = %d; want 1", len(accepted)))
	} else if accepted[0].Result != evidence.WorkflowOutcome {
		failures = append(failures, fmt.Sprintf(
			"accepted result %q does not match Workflow outcome %q", accepted[0].Result, evidence.WorkflowOutcome,
		))
	}
	return failures
}

func expectedObservationFailures(evidence Evidence) []string {
	switch evidence.Arm {
	case ArmStaleTaskToken:
		if !hasCompletion(evidence.Completions, 1, CompletionTaskToken, false, "NotFound") {
			return []string{"stale task-token completion was not rejected with NotFound"}
		}
		if !hasAcceptedResult(evidence.Completions, 2, CompletionTaskToken, evidence.WorkflowOutcome) {
			return []string{"attempt 2 task-token completion did not select the Workflow outcome"}
		}
	case ArmStaleByID:
		if !hasAcceptedResult(evidence.Completions, 1, CompletionByID, evidence.WorkflowOutcome) {
			return []string{"stale by-ID completion did not select the Workflow outcome"}
		}
	case ArmFencedByID:
		if !hasCompletion(evidence.Completions, 1, CompletionApplicationFence, false, "stale_attempt") {
			return []string{"application fence did not reject attempt 1"}
		}
		if !hasAcceptedResult(evidence.Completions, 2, CompletionByID, evidence.WorkflowOutcome) {
			return []string{"attempt 2 by-ID completion did not select the Workflow outcome"}
		}
	}
	return nil
}

func staleAttemptCannotSelectOutcome(evidence Evidence) bool {
	for _, completion := range evidence.Completions {
		if completion.Accepted && completion.CallerAttempt < 2 {
			return false
		}
	}
	return true
}

func containsAttempt(attempts []AttemptObservation, wanted int32) bool {
	for _, attempt := range attempts {
		if attempt.Attempt == wanted {
			return true
		}
	}
	return false
}

func containsInt32(values []int32, wanted int32) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func containsRetryTimeout(values []HistoryRetryObservation, attempt int32, timeoutType string) bool {
	for _, value := range values {
		if value.StartedAttempt == attempt && value.TimeoutType == timeoutType {
			return true
		}
	}
	return false
}

func acceptedCompletions(completions []CompletionObservation) []CompletionObservation {
	accepted := make([]CompletionObservation, 0, 1)
	for _, completion := range completions {
		if completion.Accepted {
			accepted = append(accepted, completion)
		}
	}
	return accepted
}

func hasCompletion(
	completions []CompletionObservation,
	callerAttempt int32,
	mechanism CompletionMechanism,
	accepted bool,
	errorCode string,
) bool {
	for _, completion := range completions {
		if completion.CallerAttempt == callerAttempt && completion.Mechanism == mechanism &&
			completion.Accepted == accepted && completion.ErrorCode == errorCode {
			return true
		}
	}
	return false
}

func hasAcceptedResult(
	completions []CompletionObservation,
	callerAttempt int32,
	mechanism CompletionMechanism,
	result string,
) bool {
	for _, completion := range completions {
		if completion.CallerAttempt == callerAttempt && completion.Mechanism == mechanism &&
			completion.Accepted && completion.Result == result {
			return true
		}
	}
	return false
}
