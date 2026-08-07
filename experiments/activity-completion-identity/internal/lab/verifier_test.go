package lab

import (
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
)

func TestVerifyCompletionIdentityArms(t *testing.T) {
	tests := []struct {
		name          string
		evidence      Evidence
		wantInvariant bool
		wantExpected  bool
		wantValid     bool
	}{
		{
			name: "stale task token is attempt scoped",
			evidence: Evidence{
				Arm:      ArmStaleTaskToken,
				Attempts: []AttemptObservation{{Attempt: 1}, {Attempt: 2}},
				History: HistoryObservation{
					StartedAttempts: []int32{2},
					RetryFailures: []HistoryRetryObservation{{
						StartedAttempt: 2, TimeoutType: "StartToClose",
					}},
					CompletedCount: 1,
				},
				Completions: []CompletionObservation{
					{CallerAttempt: 1, Mechanism: CompletionTaskToken, Accepted: false, ErrorCode: "NotFound"},
					{CallerAttempt: 2, Mechanism: CompletionTaskToken, Accepted: true, Result: "current-attempt-2"},
				},
				WorkflowOutcome: "current-attempt-2",
			},
			wantInvariant: true,
			wantExpected:  true,
			wantValid:     true,
		},
		{
			name: "logical ID lets stale caller choose result",
			evidence: Evidence{
				Arm:      ArmStaleByID,
				Attempts: []AttemptObservation{{Attempt: 1}, {Attempt: 2}},
				History: HistoryObservation{
					StartedAttempts: []int32{2},
					RetryFailures: []HistoryRetryObservation{{
						StartedAttempt: 2, TimeoutType: "StartToClose",
					}},
					CompletedCount: 1,
				},
				Completions: []CompletionObservation{
					{CallerAttempt: 1, Mechanism: CompletionByID, Accepted: true, Result: "stale-attempt-1"},
				},
				WorkflowOutcome: "stale-attempt-1",
			},
			wantInvariant: false,
			wantExpected:  true,
			wantValid:     true,
		},
		{
			name: "application fence rejects stale caller",
			evidence: Evidence{
				Arm:      ArmFencedByID,
				Attempts: []AttemptObservation{{Attempt: 1}, {Attempt: 2}},
				History: HistoryObservation{
					StartedAttempts: []int32{2},
					RetryFailures: []HistoryRetryObservation{{
						StartedAttempt: 2, TimeoutType: "StartToClose",
					}},
					CompletedCount: 1,
				},
				Completions: []CompletionObservation{
					{CallerAttempt: 1, Mechanism: CompletionApplicationFence, Accepted: false, ErrorCode: "stale_attempt"},
					{CallerAttempt: 2, Mechanism: CompletionByID, Accepted: true, Result: "current-attempt-2"},
				},
				WorkflowOutcome: "current-attempt-2",
			},
			wantInvariant: true,
			wantExpected:  true,
			wantValid:     true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			verdict := Verify(test.evidence)
			if verdict.RunValid != test.wantValid {
				t.Errorf("RunValid = %v; want %v; failures: %v", verdict.RunValid, test.wantValid, verdict.Failures)
			}
			if verdict.ExpectedObservation != test.wantExpected {
				t.Errorf("ExpectedObservation = %v; want %v; failures: %v", verdict.ExpectedObservation, test.wantExpected, verdict.Failures)
			}
			if verdict.InvariantSatisfied != test.wantInvariant {
				t.Errorf("InvariantSatisfied = %v; want %v; failures: %v", verdict.InvariantSatisfied, test.wantInvariant, verdict.Failures)
			}
		})
	}
}

func TestVerifyRejectsIncompleteEvidence(t *testing.T) {
	verdict := Verify(Evidence{
		Arm:             ArmStaleByID,
		Attempts:        []AttemptObservation{{Attempt: 1}},
		WorkflowOutcome: "stale-attempt-1",
	})
	if verdict.RunValid {
		t.Fatalf("RunValid = true; want false")
	}
}

func TestObserveCompletionUsesTemporalServiceErrorCode(t *testing.T) {
	observation := observeCompletion(
		1, CompletionTaskToken, "stale", func() error {
			return serviceerror.NewNotFound("stale attempt")
		},
	)
	if observation.ErrorCode != "NotFound" {
		t.Fatalf("ErrorCode = %q; want NotFound", observation.ErrorCode)
	}
}

func TestObserveCompletionBracketsOperationWithRequestAndResponseTimes(t *testing.T) {
	var operationAt time.Time
	observation := observeCompletion(2, CompletionByID, "current", func() error {
		operationAt = time.Now().UTC()
		return nil
	})
	if observation.RequestedAt.After(operationAt) {
		t.Fatalf("RequestedAt = %s after operation at %s", observation.RequestedAt, operationAt)
	}
	if observation.RespondedAt.Before(operationAt) {
		t.Fatalf("RespondedAt = %s before operation at %s", observation.RespondedAt, operationAt)
	}
}
