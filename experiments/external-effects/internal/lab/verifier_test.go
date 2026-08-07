package lab

import (
	"testing"
	"time"
)

func TestVerifyRecognizesControlAndProtectedOutcomes(t *testing.T) {
	t.Parallel()
	for _, destination := range AllDestinations() {
		for _, mode := range []Mode{ModeUnsafe, ModeProtected} {
			evidence := validEvidence(destination, mode)
			verdict := Verify(evidence)
			if !verdict.RunValid || !verdict.ExpectedObservation {
				t.Errorf("%s/%s verdict = %+v", destination, mode, verdict)
			}
			if got := verdict.InvariantSatisfied; got != (mode == ModeProtected) {
				t.Errorf("%s/%s invariant = %v", destination, mode, got)
			}
		}
	}
}

func TestVerifyRejectsImpreciseFailureBoundary(t *testing.T) {
	t.Parallel()
	evidence := validEvidence(DestinationDatabase, ModeProtected)
	evidence.Kill.BarrierObservedAt = evidence.Attempts[0].EffectRespondedAt.Add(-time.Nanosecond)
	verdict := Verify(evidence)
	if verdict.RunValid {
		t.Fatalf("verdict = %+v, want invalid ordering", verdict)
	}
}

func TestVerifyRejectsDuplicateProtectedEffect(t *testing.T) {
	t.Parallel()
	evidence := validEvidence(DestinationMessage, ModeProtected)
	evidence.DestinationState.PhysicalEffects = append(
		evidence.DestinationState.PhysicalEffects,
		PhysicalEffect{PhysicalID: "message-2", LogicalID: "effect-1", Receipt: "receipt-2"},
	)
	verdict := Verify(evidence)
	if verdict.InvariantSatisfied || verdict.ExpectedObservation {
		t.Fatalf("verdict = %+v, want protected invariant failure", verdict)
	}
}

func TestVerifyRejectsKilledProcessIdentityMismatch(t *testing.T) {
	t.Parallel()
	evidence := validEvidence(DestinationGit, ModeProtected)
	evidence.Kill.PID++
	verdict := Verify(evidence)
	if verdict.RunValid {
		t.Fatalf("verdict = %+v, want invalid killed PID", verdict)
	}
}

func TestVerifyFailsClosedWithoutTwoAttempts(t *testing.T) {
	t.Parallel()
	for _, attempts := range [][]AttemptObservation{
		nil,
		{{Attempt: 1}},
		{{Attempt: 1}, {Attempt: 2}, {Attempt: 3}},
	} {
		evidence := validEvidence(DestinationDatabase, ModeProtected)
		evidence.Attempts = attempts
		verdict := Verify(evidence)
		if verdict.RunValid || verdict.ExpectedObservation || verdict.InvariantSatisfied {
			t.Errorf("attempts=%d verdict=%+v, want fail-closed", len(attempts), verdict)
		}
	}
}

func TestVerifyRejectsMalformedEvidenceFields(t *testing.T) {
	t.Parallel()
	tests := map[string]func(*Evidence){
		"identity": func(evidence *Evidence) {
			evidence.Destination = "unknown"
			evidence.Mode = "unknown"
			evidence.EffectID = ""
		},
		"attempt order": func(evidence *Evidence) {
			evidence.Attempts[0].Attempt = 2
		},
		"worker identities": func(evidence *Evidence) {
			evidence.Attempts[1].WorkerID = "worker-1"
		},
		"receipts": func(evidence *Evidence) {
			evidence.Attempts[0].Receipt = ""
		},
		"attempt one timestamps": func(evidence *Evidence) {
			evidence.Attempts[0].StartedAt = time.Time{}
		},
		"retry timestamps": func(evidence *Evidence) {
			evidence.Attempts[1].StartedAt = evidence.Kill.KilledAt.Add(-time.Nanosecond)
		},
		"history": func(evidence *Evidence) {
			evidence.History.CompletedCount = 2
		},
		"workflow outcome": func(evidence *Evidence) {
			evidence.WorkflowOutcome = "different"
		},
		"destination effect": func(evidence *Evidence) {
			evidence.DestinationState.PhysicalEffects[0].LogicalID = "different"
		},
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			evidence := validEvidence(DestinationDatabase, ModeProtected)
			mutate(&evidence)
			verdict := Verify(evidence)
			if verdict.RunValid || len(verdict.Failures) == 0 {
				t.Fatalf("verdict=%+v, want invalid evidence", verdict)
			}
		})
	}
}

func validEvidence(destination Destination, mode Mode) Evidence {
	base := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	firstReceipt := "receipt-1"
	secondReceipt := "receipt-2"
	secondOutcome := OutcomeApplied
	physical := []PhysicalEffect{
		{PhysicalID: "physical-1", LogicalID: "effect-1", Receipt: firstReceipt, AppliedAt: base.Add(time.Second)},
		{PhysicalID: "physical-2", LogicalID: "effect-1", Receipt: secondReceipt, AppliedAt: base.Add(5 * time.Second)},
	}
	if mode == ModeProtected {
		secondReceipt = firstReceipt
		secondOutcome = protectedOutcome(destination)
		physical = physical[:1]
	}
	return Evidence{
		Destination: destination,
		Mode:        mode,
		EffectID:    "effect-1",
		Attempts: []AttemptObservation{
			{Attempt: 1, WorkerID: "worker-1", PID: 100, StartedAt: base, EffectRequestedAt: base.Add(time.Second), EffectRespondedAt: base.Add(2 * time.Second), Outcome: OutcomeApplied, Receipt: firstReceipt},
			{Attempt: 2, WorkerID: "worker-2", PID: 101, StartedAt: base.Add(4 * time.Second), EffectRequestedAt: base.Add(5 * time.Second), EffectRespondedAt: base.Add(6 * time.Second), Outcome: secondOutcome, Receipt: secondReceipt},
		},
		Kill:             KillObservation{BarrierObservedAt: base.Add(2500 * time.Millisecond), KilledAt: base.Add(3 * time.Second), WorkerID: "worker-1", PID: 100},
		DestinationState: DestinationState{PhysicalEffects: physical},
		History:          HistoryObservation{RetryTimedOut: true, CompletedCount: 1, CompletedAttempt: 2},
		WorkflowOutcome:  secondReceipt,
	}
}
