package lab

import "fmt"

func Verify(evidence Evidence) Verdict {
	failures := validateEvidence(evidence)
	invariantSatisfied := len(evidence.DestinationState.PhysicalEffects) == 1 &&
		len(evidence.Attempts) == 2 && evidence.Attempts[0].Receipt != "" &&
		evidence.Attempts[0].Receipt == evidence.Attempts[1].Receipt
	expected := len(failures) == 0
	if evidence.Mode == ModeProtected {
		expected = expected && invariantSatisfied &&
			evidence.Attempts[1].Outcome == protectedOutcome(evidence.Destination)
	} else {
		expected = expected && !invariantSatisfied && len(evidence.DestinationState.PhysicalEffects) == 2 &&
			evidence.Attempts[1].Outcome == OutcomeApplied
	}
	return Verdict{
		RunValid: len(failures) == 0, ExpectedObservation: expected,
		InvariantSatisfied: invariantSatisfied, Failures: failures,
	}
}

func validateEvidence(evidence Evidence) []string {
	var failures []string
	if !evidence.Destination.Valid() || !evidence.Mode.Valid() || evidence.EffectID == "" {
		failures = append(failures, "destination, mode, and effect ID must be valid")
	}
	if len(evidence.Attempts) != 2 {
		return append(failures, fmt.Sprintf("observed %d attempts; want 2", len(evidence.Attempts)))
	}
	first, second := evidence.Attempts[0], evidence.Attempts[1]
	if first.Attempt != 1 || second.Attempt != 2 {
		failures = append(failures, "attempt observations must be ordered 1 then 2")
	}
	if first.WorkerID != "worker-1" || second.WorkerID != "worker-2" {
		failures = append(failures, "attempts did not run on the expected Workers")
	}
	if first.Outcome != OutcomeApplied || first.Receipt == "" || second.Receipt == "" {
		failures = append(failures, "both attempts need receipts and attempt 1 must apply the effect")
	}
	if first.StartedAt.IsZero() || first.EffectRequestedAt.Before(first.StartedAt) ||
		first.EffectRespondedAt.Before(first.EffectRequestedAt) {
		failures = append(failures, "attempt 1 timestamps are incomplete or out of order")
	}
	if evidence.Kill.WorkerID != "worker-1" || evidence.Kill.PID <= 0 ||
		first.PID != evidence.Kill.PID || second.PID <= 0 || second.PID == evidence.Kill.PID ||
		evidence.Kill.BarrierObservedAt.Before(first.EffectRespondedAt) ||
		evidence.Kill.KilledAt.Before(evidence.Kill.BarrierObservedAt) {
		failures = append(failures, "effect/barrier/Worker-kill ordering is invalid")
	}
	if second.StartedAt.Before(evidence.Kill.KilledAt) || second.EffectRequestedAt.Before(second.StartedAt) ||
		second.EffectRespondedAt.Before(second.EffectRequestedAt) {
		failures = append(failures, "retry started before the kill or has invalid timestamps")
	}
	if !evidence.History.RetryTimedOut || evidence.History.CompletedCount != 1 ||
		evidence.History.CompletedAttempt != 2 {
		failures = append(failures, "Temporal history does not show timed-out attempt 1 and one completion by attempt 2")
	}
	if evidence.WorkflowOutcome != second.Receipt {
		failures = append(failures, "Workflow outcome is not attempt 2's receipt")
	}
	for _, effect := range evidence.DestinationState.PhysicalEffects {
		if effect.LogicalID != evidence.EffectID || effect.PhysicalID == "" || effect.Receipt == "" {
			failures = append(failures, "destination snapshot contains an invalid physical effect")
			break
		}
	}
	return failures
}
