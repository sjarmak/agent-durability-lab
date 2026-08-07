package lab

import (
	"testing"

	"github.com/temporalio-labs/agent-durability-lab/internal/workstore"
)

func TestVerifyPostExecAttachControlRequiresOneReusedChild(t *testing.T) {
	t.Parallel()
	outcome := workstore.Outcome{Value: "outcome/session-1/g1"}
	snapshot := postExecAttachFixture(outcome)
	verdict := VerifyPostExecGap(PostExecAttachControl, snapshot, &outcome)
	if !verdict.RunValid || !verdict.ExpectedObservation || !verdict.InvariantSatisfied ||
		!verdict.WorkflowOutcomeMatchesApplication {
		t.Fatalf("attach verdict = %+v", verdict)
	}

	snapshot.Executors = append(snapshot.Executors, workstore.Executor{Generation: 2})
	invalid := VerifyPostExecGap(PostExecAttachControl, snapshot, &outcome)
	if invalid.RunValid {
		t.Fatalf("competing attach verdict = %+v, want invalid", invalid)
	}
}

func TestVerifyPostExecFencedReplacementRequiresRevocationAndExit(t *testing.T) {
	t.Parallel()
	outcome := workstore.Outcome{Value: "outcome/session-1/g2"}
	snapshot := postExecFencedFixture(outcome)
	verdict := VerifyPostExecGap(PostExecFencedReplacement, snapshot, &outcome)
	if !verdict.RunValid || !verdict.ExpectedObservation || !verdict.InvariantSatisfied ||
		!verdict.StaleAuthorityRejected || !verdict.StaleProcessExited {
		t.Fatalf("fenced verdict = %+v", verdict)
	}

	snapshot.Events = snapshot.Events[:len(snapshot.Events)-1]
	invalid := VerifyPostExecGap(PostExecFencedReplacement, snapshot, &outcome)
	if invalid.RunValid || invalid.StaleProcessExited {
		t.Fatalf("missing cleanup verdict = %+v, want invalid", invalid)
	}
}

func TestVerifyPostExecGapRejectsOutcomeMismatchAndUnknownArm(t *testing.T) {
	t.Parallel()
	outcome := workstore.Outcome{Value: "outcome/session-1/g1"}
	snapshot := postExecAttachFixture(outcome)
	wrong := workstore.Outcome{Value: "wrong"}
	if verdict := VerifyPostExecGap(PostExecAttachControl, snapshot, &wrong); verdict.RunValid {
		t.Fatalf("mismatched outcome verdict = %+v, want invalid", verdict)
	}
	if verdict := VerifyPostExecGap("unknown", snapshot, &outcome); verdict.RunValid {
		t.Fatalf("unknown arm verdict = %+v, want invalid", verdict)
	}
}

func postExecAttachFixture(outcome workstore.Outcome) workstore.Snapshot {
	return workstore.Snapshot{
		SessionID: "session-1", Mode: workstore.ModeFenced, ActiveGeneration: 1, Outcome: &outcome,
		Executors: []workstore.Executor{{
			Generation: 1, Status: workstore.ExecutorStatusCompleted, PID: 101, ProcessStart: "boot:101",
		}},
		Effects: []workstore.AcceptedEffect{{Generation: 1}},
		Events: []workstore.Event{
			{Sequence: 1, Kind: "executor_launch_decided", Generation: 1, Attempt: 1},
			{Sequence: 2, Kind: "unregistered_child_discovered", Generation: 1, Attempt: 1, PID: 101, Details: map[string]string{"process_start": "boot:101"}},
			{Sequence: 3, Kind: "worker_killed", Generation: 1, Attempt: 1, PID: 100},
			{Sequence: 4, Kind: "child_alive_after_worker_kill", Generation: 1, Attempt: 1, PID: 101, Details: map[string]string{"process_start": "boot:101"}},
			{Sequence: 5, Kind: "activity_reattached", Generation: 1, Attempt: 2},
			{Sequence: 6, Kind: "process_registered", Generation: 1, Attempt: 1, PID: 101, Details: map[string]string{"process_start": "boot:101"}},
			{Sequence: 7, Kind: "effect_accepted", Generation: 1, Attempt: 1},
			{Sequence: 8, Kind: "outcome_accepted", Generation: 1, Attempt: 1},
			{Sequence: 9, Kind: "child_identity_gone", Generation: 1, Attempt: 1, PID: 101, Details: map[string]string{"process_start": "boot:101"}},
		},
	}
}

func postExecFencedFixture(outcome workstore.Outcome) workstore.Snapshot {
	return workstore.Snapshot{
		SessionID: "session-1", Mode: workstore.ModeFenced, ActiveGeneration: 2, Outcome: &outcome,
		Executors: []workstore.Executor{
			{Generation: 1, Status: workstore.ExecutorStatusSuperseded},
			{Generation: 2, Status: workstore.ExecutorStatusCompleted, PID: 202, ProcessStart: "boot:202"},
		},
		Effects: []workstore.AcceptedEffect{{Generation: 2}},
		Events: []workstore.Event{
			{Sequence: 1, Kind: "executor_launch_decided", Generation: 1, Attempt: 1},
			{Sequence: 2, Kind: "unregistered_child_discovered", Generation: 1, Attempt: 1, PID: 101, Details: map[string]string{"process_start": "boot:101"}},
			{Sequence: 3, Kind: "worker_killed", Generation: 1, Attempt: 1, PID: 100},
			{Sequence: 4, Kind: "child_alive_after_worker_kill", Generation: 1, Attempt: 1, PID: 101, Details: map[string]string{"process_start": "boot:101"}},
			{Sequence: 5, Kind: "pending_launch_replaced", Generation: 2, Attempt: 2},
			{Sequence: 6, Kind: "stale_child_alive_after_replacement", Generation: 1, Attempt: 1, PID: 101, Details: map[string]string{"process_start": "boot:101"}},
			{Sequence: 7, Kind: "process_registered", Generation: 2, Attempt: 2, PID: 202},
			{Sequence: 8, Kind: "effect_accepted", Generation: 2, Attempt: 2},
			{Sequence: 9, Kind: "outcome_accepted", Generation: 2, Attempt: 2},
			{Sequence: 10, Kind: "process_registration_rejected_stale", Generation: 1, Attempt: 1, PID: 101, Details: map[string]string{"process_start": "boot:101"}},
			{Sequence: 11, Kind: "stale_child_identity_gone", Generation: 1, Attempt: 1, PID: 101, Details: map[string]string{"process_start": "boot:101"}},
		},
	}
}
