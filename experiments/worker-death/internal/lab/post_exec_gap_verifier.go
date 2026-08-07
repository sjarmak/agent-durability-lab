package lab

import (
	"fmt"

	"github.com/temporalio-labs/agent-durability-lab/internal/workstore"
)

type PostExecGapArm string

const (
	PostExecAttachControl     PostExecGapArm = "attach-control"
	PostExecFencedReplacement PostExecGapArm = "fenced-replacement"
)

func (a PostExecGapArm) Valid() bool {
	return a == PostExecAttachControl || a == PostExecFencedReplacement
}

type PostExecGapVerdict struct {
	Arm                               PostExecGapArm     `json:"arm"`
	RunValid                          bool               `json:"run_valid"`
	InvariantSatisfied                bool               `json:"invariant_satisfied"`
	ExpectedObservation               bool               `json:"expected_observation"`
	WorkflowOutcomeMatchesApplication bool               `json:"workflow_outcome_matches_application"`
	StaleAuthorityRejected            bool               `json:"stale_authority_rejected"`
	StaleProcessExited                bool               `json:"stale_process_exited"`
	WorkflowOutcome                   *workstore.Outcome `json:"workflow_outcome,omitempty"`
	ApplicationOutcome                *workstore.Outcome `json:"application_outcome,omitempty"`
	Failures                          []string           `json:"failures,omitempty"`
}

func VerifyPostExecGap(
	arm PostExecGapArm,
	snapshot workstore.Snapshot,
	workflowOutcome *workstore.Outcome,
) PostExecGapVerdict {
	verdict := PostExecGapVerdict{Arm: arm, WorkflowOutcome: workflowOutcome}
	if snapshot.SessionID == "" || snapshot.Mode != workstore.ModeFenced {
		verdict.Failures = append(verdict.Failures, "snapshot requires a fenced session identity")
	}
	if snapshot.Outcome == nil || workflowOutcome == nil {
		verdict.Failures = append(verdict.Failures, "application and Workflow outcomes are required")
	} else {
		outcome := *snapshot.Outcome
		verdict.ApplicationOutcome = &outcome
		verdict.WorkflowOutcomeMatchesApplication = outcome == *workflowOutcome
		if !verdict.WorkflowOutcomeMatchesApplication {
			verdict.Failures = append(verdict.Failures, "Workflow outcome does not match the application outcome")
		}
	}
	discovered, found := eventByKindGeneration(snapshot.Events, "unregistered_child_discovered", 1)
	if !found || discovered.PID <= 0 || discovered.Details["process_start"] == "" {
		verdict.Failures = append(verdict.Failures, "missing unregistered child process identity")
	}
	alive, aliveFound := eventByKindGeneration(snapshot.Events, "child_alive_after_worker_kill", 1)
	if !aliveFound || alive.PID != discovered.PID || alive.Details["process_start"] != discovered.Details["process_start"] {
		verdict.Failures = append(verdict.Failures, "child survival identity does not match discovery")
	}
	if !eventsAppearInOrder(snapshot.Events, []eventMatch{
		{kind: "executor_launch_decided", attempt: 1, generation: 1},
		{kind: "unregistered_child_discovered", attempt: 1, generation: 1},
		{kind: "worker_killed", attempt: 1, generation: 1},
		{kind: "child_alive_after_worker_kill", attempt: 1, generation: 1},
	}) {
		verdict.Failures = append(verdict.Failures, "post-exec discovery, Worker kill, and child survival evidence are missing or out of order")
	}
	if len(snapshot.Effects) != 1 {
		verdict.Failures = append(verdict.Failures, fmt.Sprintf("accepted effects = %d; want 1", len(snapshot.Effects)))
	}

	switch arm {
	case PostExecAttachControl:
		verifyPostExecAttach(snapshot, discovered, &verdict)
	case PostExecFencedReplacement:
		verifyPostExecFenced(snapshot, discovered, &verdict)
	default:
		verdict.Failures = append(verdict.Failures, fmt.Sprintf("unknown post-exec arm %q", arm))
	}
	verdict.RunValid = len(verdict.Failures) == 0
	verdict.InvariantSatisfied = verdict.RunValid
	verdict.ExpectedObservation = verdict.RunValid
	return verdict
}

func verifyPostExecAttach(
	snapshot workstore.Snapshot,
	discovered workstore.Event,
	verdict *PostExecGapVerdict,
) {
	if snapshot.ActiveGeneration != 1 || len(snapshot.Executors) != 1 {
		verdict.Failures = append(verdict.Failures, "attach control did not preserve one active generation")
	} else {
		executor := snapshot.Executors[0]
		if executor.Generation != 1 || executor.Status != workstore.ExecutorStatusCompleted ||
			executor.PID != discovered.PID || executor.ProcessStart != discovered.Details["process_start"] {
			verdict.Failures = append(verdict.Failures, "attach control did not register and complete the discovered child")
		}
	}
	if len(snapshot.Effects) == 1 && snapshot.Effects[0].Generation != 1 {
		verdict.Failures = append(verdict.Failures, "attach control effect did not come from generation 1")
	}
	if !eventsAppearInOrder(snapshot.Events, []eventMatch{
		{kind: "activity_reattached", attempt: 2, generation: 1},
		{kind: "process_registered", attempt: 1, generation: 1},
		{kind: "effect_accepted", attempt: 1, generation: 1},
		{kind: "outcome_accepted", attempt: 1, generation: 1},
		{kind: "child_identity_gone", attempt: 1, generation: 1},
	}) {
		verdict.Failures = append(verdict.Failures, "attach, registration, outcome, and process-exit evidence are missing or out of order")
	}
}

func verifyPostExecFenced(
	snapshot workstore.Snapshot,
	discovered workstore.Event,
	verdict *PostExecGapVerdict,
) {
	if snapshot.ActiveGeneration != 2 || len(snapshot.Executors) != 2 {
		verdict.Failures = append(verdict.Failures, "fenced replacement did not preserve two generations with generation 2 active")
	} else {
		old, replacement := snapshot.Executors[0], snapshot.Executors[1]
		if old.Generation != 1 || old.Status != workstore.ExecutorStatusSuperseded || old.PID != 0 || old.ProcessStart != "" {
			verdict.Failures = append(verdict.Failures, "obsolete child registered in the authoritative executor record")
		}
		if replacement.Generation != 2 || replacement.Status != workstore.ExecutorStatusCompleted ||
			replacement.PID <= 0 || replacement.ProcessStart == "" {
			verdict.Failures = append(verdict.Failures, "replacement child did not register and complete")
		}
	}
	if len(snapshot.Effects) == 1 && snapshot.Effects[0].Generation != 2 {
		verdict.Failures = append(verdict.Failures, "fenced effect did not come from generation 2")
	}
	stale, staleFound := eventByKindGeneration(snapshot.Events, "process_registration_rejected_stale", 1)
	verdict.StaleAuthorityRejected = staleFound && stale.PID == discovered.PID &&
		stale.Details["process_start"] == discovered.Details["process_start"]
	if !verdict.StaleAuthorityRejected {
		verdict.Failures = append(verdict.Failures, "obsolete child's generation capability was not rejected at registration")
	}
	exited, exitFound := eventByKindGeneration(snapshot.Events, "stale_child_identity_gone", 1)
	verdict.StaleProcessExited = exitFound && exited.PID == discovered.PID &&
		exited.Details["process_start"] == discovered.Details["process_start"]
	if !verdict.StaleProcessExited {
		verdict.Failures = append(verdict.Failures, "obsolete child process identity did not disappear after rejection")
	}
	if !eventsAppearInOrder(snapshot.Events, []eventMatch{
		{kind: "pending_launch_replaced", attempt: 2, generation: 2},
		{kind: "process_registered", attempt: 2, generation: 2},
		{kind: "effect_accepted", attempt: 2, generation: 2},
		{kind: "outcome_accepted", attempt: 2, generation: 2},
		{kind: "process_registration_rejected_stale", attempt: 1, generation: 1},
		{kind: "stale_child_identity_gone", attempt: 1, generation: 1},
	}) {
		verdict.Failures = append(verdict.Failures, "replacement outcome, stale registration rejection, and process-exit evidence are missing or out of order")
	}
	if !eventsAppearInOrder(snapshot.Events, []eventMatch{
		{kind: "pending_launch_replaced", attempt: 2, generation: 2},
		{kind: "stale_child_alive_after_replacement", attempt: 1, generation: 1},
		{kind: "process_registration_rejected_stale", attempt: 1, generation: 1},
	}) {
		verdict.Failures = append(verdict.Failures, "obsolete child coexistence was not observed before stale registration")
	}
}

func eventByKindGeneration(events []workstore.Event, kind string, generation uint64) (workstore.Event, bool) {
	for _, event := range events {
		if event.Kind == kind && event.Generation == generation {
			return event, true
		}
	}
	return workstore.Event{}, false
}
