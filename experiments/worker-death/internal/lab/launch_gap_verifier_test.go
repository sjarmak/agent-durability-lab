package lab

import (
	"testing"

	"github.com/sjarmak/temporal_projects/internal/workstore"
)

func TestVerifyLaunchGapControlRequiresRecordedPhantomAndCanceledWorkflow(t *testing.T) {
	snapshot := workstore.Snapshot{
		SessionID: "session-1", Mode: workstore.ModeFenced, ActiveGeneration: 1,
		Executors: []workstore.Executor{{Generation: 1, Status: workstore.ExecutorStatusLaunchPending}},
		Events: []workstore.Event{
			{Sequence: 1, Kind: "executor_launch_decided", Generation: 1, Attempt: 1},
			{Sequence: 2, Kind: "activity_after_launch_decision", Generation: 1, Attempt: 1},
			{Sequence: 3, Kind: "worker_killed", Generation: 1, Attempt: 1},
			{Sequence: 4, Kind: "activity_reattached", Generation: 1, Attempt: 2},
			{Sequence: 5, Kind: "phantom_launch_observed", Generation: 1, Attempt: 2},
		},
	}

	verdict := VerifyLaunchGap(LaunchGapControl, snapshot, nil, true)
	if !verdict.RunValid || !verdict.ExpectedObservation || verdict.InvariantSatisfied {
		t.Fatalf("control verdict = %+v", verdict)
	}

	withoutCancellation := VerifyLaunchGap(LaunchGapControl, snapshot, nil, false)
	if withoutCancellation.RunValid {
		t.Fatalf("uncanceled control verdict = %+v; want invalid", withoutCancellation)
	}
}

func TestVerifyLaunchGapFencedRecoveryRequiresOneGenerationTwoOutcome(t *testing.T) {
	want := workstore.Outcome{Value: "outcome/session-1/g2"}
	snapshot := workstore.Snapshot{
		SessionID: "session-1", Mode: workstore.ModeFenced, ActiveGeneration: 2, Outcome: &want,
		Executors: []workstore.Executor{
			{Generation: 1, Status: workstore.ExecutorStatusSuperseded},
			{Generation: 2, Status: workstore.ExecutorStatusCompleted, PID: 42, ProcessStart: "boot:42"},
		},
		Effects: []workstore.AcceptedEffect{{Generation: 2}},
		Events: []workstore.Event{
			{Sequence: 1, Kind: "executor_launch_decided", Generation: 1, Attempt: 1},
			{Sequence: 2, Kind: "activity_after_launch_decision", Generation: 1, Attempt: 1},
			{Sequence: 3, Kind: "worker_killed", Generation: 1, Attempt: 1},
			{Sequence: 4, Kind: "pending_launch_replaced", Generation: 2, Attempt: 2},
			{Sequence: 5, Kind: "process_registered", Generation: 2, Attempt: 2},
			{Sequence: 6, Kind: "effect_accepted", Generation: 2, Attempt: 2},
			{Sequence: 7, Kind: "outcome_accepted", Generation: 2, Attempt: 2},
		},
	}

	verdict := VerifyLaunchGap(LaunchGapFencedRecovery, snapshot, &want, false)
	if !verdict.RunValid || !verdict.ExpectedObservation || !verdict.InvariantSatisfied ||
		!verdict.WorkflowOutcomeMatchesApplication {
		t.Fatalf("recovery verdict = %+v", verdict)
	}

	wrong := workstore.Outcome{Value: "wrong"}
	mismatch := VerifyLaunchGap(LaunchGapFencedRecovery, snapshot, &wrong, false)
	if mismatch.RunValid || mismatch.WorkflowOutcomeMatchesApplication {
		t.Fatalf("mismatched verdict = %+v; want invalid", mismatch)
	}
}

func TestVerifyLaunchGapRejectsOutOfOrderOrRegisteredControlEvidence(t *testing.T) {
	snapshot := workstore.Snapshot{
		SessionID: "session-1", Mode: workstore.ModeFenced, ActiveGeneration: 1,
		Executors: []workstore.Executor{{
			Generation: 1, Status: workstore.ExecutorStatusRunning, PID: 42, ProcessStart: "boot:42",
		}},
		Events: []workstore.Event{
			{Sequence: 2, Kind: "executor_launch_decided", Generation: 1, Attempt: 1},
			{Sequence: 1, Kind: "activity_after_launch_decision", Generation: 1, Attempt: 1},
			{Sequence: 3, Kind: "worker_killed", Generation: 1, Attempt: 1},
			{Sequence: 4, Kind: "activity_reattached", Generation: 1, Attempt: 2},
			{Sequence: 5, Kind: "phantom_launch_observed", Generation: 1, Attempt: 2},
		},
	}
	verdict := VerifyLaunchGap(LaunchGapControl, snapshot, nil, true)
	if verdict.RunValid || len(verdict.Failures) == 0 {
		t.Fatalf("invalid evidence verdict = %+v", verdict)
	}
}
