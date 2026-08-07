package lab

import (
	"testing"

	"github.com/sjarmak/temporal_projects/internal/workstore"
)

func TestVerifierUnsafeControlRequiresObservedDuplication(t *testing.T) {
	snapshot := workstore.Snapshot{
		SessionID: "session-1", Mode: workstore.ModeUnsafe,
		Executors: []workstore.Executor{{Generation: 1}, {Generation: 2}},
		Effects:   []workstore.AcceptedEffect{{Generation: 1}, {Generation: 2}},
		Outcome:   &workstore.Outcome{Value: "one terminal result"},
		Events: []workstore.Event{
			{Kind: "child_alive_after_worker_kill"},
			{Kind: "executor_launch_decided", Attempt: 2},
		},
	}
	verdict := Verify(workstore.ModeUnsafe, snapshot, *snapshot.Outcome)
	if !verdict.RunValid || !verdict.ExpectedObservation {
		t.Fatalf("control verdict = %+v; want a valid reproduced violation", verdict)
	}
	if verdict.InvariantSatisfied {
		t.Fatalf("control verdict = %+v; unsafe duplicate must violate safety", verdict)
	}
}

func TestVerifierRejectsUnsafeControlThatCannotFailLoudly(t *testing.T) {
	snapshot := workstore.Snapshot{
		SessionID: "session-1", Mode: workstore.ModeUnsafe,
		Executors: []workstore.Executor{{Generation: 1}},
		Effects:   []workstore.AcceptedEffect{{Generation: 1}},
		Outcome:   &workstore.Outcome{Value: "done"},
	}
	verdict := Verify(workstore.ModeUnsafe, snapshot, *snapshot.Outcome)
	if verdict.RunValid || verdict.ExpectedObservation {
		t.Fatalf("control verdict = %+v; want invalid negative control", verdict)
	}
}

func TestVerifierReattachmentRequiresSameSessionAcrossAttempts(t *testing.T) {
	snapshot := workstore.Snapshot{
		SessionID: "session-1", Mode: workstore.ModeReattach,
		Executors: []workstore.Executor{{Generation: 1}},
		Effects:   []workstore.AcceptedEffect{{Generation: 1}},
		Outcome:   &workstore.Outcome{Value: "done"},
		Events: []workstore.Event{
			{Kind: "executor_launch_decided", Attempt: 1, Generation: 1},
			{Kind: "child_alive_after_worker_kill", Generation: 1},
			{Kind: "activity_reattached", Attempt: 2, Generation: 1},
			{Kind: "outcome_accepted", Generation: 1},
		},
	}
	verdict := Verify(workstore.ModeReattach, snapshot, *snapshot.Outcome)
	if !verdict.RunValid || !verdict.InvariantSatisfied {
		t.Fatalf("reattachment verdict = %+v; want valid satisfied invariant", verdict)
	}
}

func TestVerifierFencedReplacementRequiresDelayedStaleRejection(t *testing.T) {
	snapshot := workstore.Snapshot{
		SessionID: "session-1", Mode: workstore.ModeFenced, ActiveGeneration: 2,
		Executors: []workstore.Executor{{Generation: 1, Status: "superseded"}, {Generation: 2, Status: "completed"}},
		Effects:   []workstore.AcceptedEffect{{Generation: 2}},
		Outcome:   &workstore.Outcome{Value: "replacement"},
		Events: []workstore.Event{
			{Sequence: 1, Kind: "child_alive_after_worker_kill", Generation: 1},
			{Sequence: 2, Kind: "owner_replaced", Attempt: 2, Generation: 2},
			{Sequence: 3, Kind: "outcome_accepted", Generation: 2},
			{Sequence: 4, Kind: "effect_rejected_stale", Generation: 1},
			{Sequence: 5, Kind: "completion_rejected_stale", Generation: 1},
		},
	}
	verdict := Verify(workstore.ModeFenced, snapshot, *snapshot.Outcome)
	if !verdict.RunValid || !verdict.InvariantSatisfied {
		t.Fatalf("fenced verdict = %+v; want valid satisfied invariant", verdict)
	}
}

func TestVerifierRejectsStaleCompletionBeforeReplacementOutcome(t *testing.T) {
	snapshot := workstore.Snapshot{
		SessionID: "session-1", Mode: workstore.ModeFenced, ActiveGeneration: 2,
		Executors: []workstore.Executor{{Generation: 1}, {Generation: 2}},
		Effects:   []workstore.AcceptedEffect{{Generation: 2}},
		Outcome:   &workstore.Outcome{Value: "replacement"},
		Events: []workstore.Event{
			{Sequence: 1, Kind: "completion_rejected_stale", Generation: 1},
			{Sequence: 2, Kind: "outcome_accepted", Generation: 2},
		},
	}
	verdict := Verify(workstore.ModeFenced, snapshot, *snapshot.Outcome)
	if verdict.RunValid {
		t.Fatalf("fenced verdict = %+v; want ordering failure", verdict)
	}
}

func TestVerifierRejectsWorkflowOutcomeThatDiffersFromApplicationOutcome(t *testing.T) {
	snapshot := workstore.Snapshot{
		SessionID: "session-1", Mode: workstore.ModeReattach,
		Executors: []workstore.Executor{{Generation: 1}},
		Effects:   []workstore.AcceptedEffect{{Generation: 1}},
		Outcome:   &workstore.Outcome{Value: "application-outcome"},
		Events: []workstore.Event{
			{Kind: "child_alive_after_worker_kill", Generation: 1},
			{Kind: "activity_reattached", Attempt: 2, Generation: 1},
		},
	}
	verdict := Verify(workstore.ModeReattach, snapshot, workstore.Outcome{Value: "workflow-outcome"})
	if verdict.RunValid || verdict.WorkflowOutcomeMatchesApplication {
		t.Fatalf("mismatch verdict = %+v; want invalid run", verdict)
	}
}
