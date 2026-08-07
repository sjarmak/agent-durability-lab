package lab

import (
	"fmt"

	"github.com/sjarmak/temporal_projects/internal/workstore"
)

type LaunchGapArm string

const (
	LaunchGapControl        LaunchGapArm = "control"
	LaunchGapFencedRecovery LaunchGapArm = "fenced-recovery"
)

func (a LaunchGapArm) Valid() bool {
	return a == LaunchGapControl || a == LaunchGapFencedRecovery
}

type LaunchGapVerdict struct {
	Arm                               LaunchGapArm       `json:"arm"`
	RunValid                          bool               `json:"run_valid"`
	InvariantSatisfied                bool               `json:"invariant_satisfied"`
	ExpectedObservation               bool               `json:"expected_observation"`
	WorkflowCanceled                  bool               `json:"workflow_canceled"`
	WorkflowOutcomeMatchesApplication bool               `json:"workflow_outcome_matches_application"`
	WorkflowOutcome                   *workstore.Outcome `json:"workflow_outcome,omitempty"`
	ApplicationOutcome                *workstore.Outcome `json:"application_outcome,omitempty"`
	Failures                          []string           `json:"failures,omitempty"`
}

func VerifyLaunchGap(
	arm LaunchGapArm,
	snapshot workstore.Snapshot,
	workflowOutcome *workstore.Outcome,
	workflowCanceled bool,
) LaunchGapVerdict {
	verdict := LaunchGapVerdict{
		Arm: arm, WorkflowCanceled: workflowCanceled, WorkflowOutcome: workflowOutcome,
		Failures: make([]string, 0),
	}
	if snapshot.SessionID == "" {
		verdict.Failures = append(verdict.Failures, "missing session identity")
	}
	if snapshot.Mode != workstore.ModeFenced {
		verdict.Failures = append(verdict.Failures, fmt.Sprintf("snapshot mode %q; want fenced", snapshot.Mode))
	}
	if snapshot.Outcome != nil {
		outcome := *snapshot.Outcome
		verdict.ApplicationOutcome = &outcome
	}

	switch arm {
	case LaunchGapControl:
		verifyLaunchGapControl(snapshot, workflowOutcome, workflowCanceled, &verdict)
	case LaunchGapFencedRecovery:
		verifyLaunchGapRecovery(snapshot, workflowOutcome, workflowCanceled, &verdict)
	default:
		verdict.Failures = append(verdict.Failures, fmt.Sprintf("unknown launch-gap arm %q", arm))
	}
	verdict.RunValid = len(verdict.Failures) == 0
	verdict.ExpectedObservation = verdict.RunValid
	return verdict
}

func verifyLaunchGapControl(
	snapshot workstore.Snapshot,
	workflowOutcome *workstore.Outcome,
	workflowCanceled bool,
	verdict *LaunchGapVerdict,
) {
	if len(snapshot.Executors) != 1 {
		verdict.Failures = append(verdict.Failures, fmt.Sprintf("control executors = %d; want 1", len(snapshot.Executors)))
	} else {
		executor := snapshot.Executors[0]
		if executor.Generation != 1 || executor.Status != workstore.ExecutorStatusLaunchPending ||
			executor.PID != 0 || executor.ProcessStart != "" {
			verdict.Failures = append(verdict.Failures, "control did not preserve one unregistered pending generation")
		}
	}
	if len(snapshot.Effects) != 0 || snapshot.Outcome != nil || workflowOutcome != nil {
		verdict.Failures = append(verdict.Failures, "control unexpectedly produced an effect or outcome")
	}
	if !workflowCanceled {
		verdict.Failures = append(verdict.Failures, "control Workflow was not canceled after phantom evidence was recorded")
	}
	if hasEvent(snapshot.Events, "process_registered", 0, 0) {
		verdict.Failures = append(verdict.Failures, "control registered a process despite the pre-launch kill boundary")
	}
	if !eventsAppearInOrder(snapshot.Events, []eventMatch{
		{kind: "executor_launch_decided", attempt: 1, generation: 1},
		{kind: "activity_after_launch_decision", attempt: 1, generation: 1},
		{kind: "worker_killed", attempt: 1, generation: 1},
		{kind: "activity_reattached", attempt: 2, generation: 1},
		{kind: "phantom_launch_observed", attempt: 2, generation: 1},
	}) {
		verdict.Failures = append(verdict.Failures, "control boundary, kill, reattachment, and phantom evidence are missing or out of order")
	}
	verdict.InvariantSatisfied = false
}

func verifyLaunchGapRecovery(
	snapshot workstore.Snapshot,
	workflowOutcome *workstore.Outcome,
	workflowCanceled bool,
	verdict *LaunchGapVerdict,
) {
	if workflowCanceled {
		verdict.Failures = append(verdict.Failures, "recovery Workflow was canceled")
	}
	if len(snapshot.Executors) != 2 {
		verdict.Failures = append(verdict.Failures, fmt.Sprintf("recovery executors = %d; want 2 lifecycle records", len(snapshot.Executors)))
	} else {
		old, replacement := snapshot.Executors[0], snapshot.Executors[1]
		if old.Generation != 1 || old.Status != workstore.ExecutorStatusSuperseded || old.PID != 0 || old.ProcessStart != "" {
			verdict.Failures = append(verdict.Failures, "generation 1 was not preserved as an unregistered superseded launch")
		}
		if replacement.Generation != 2 || replacement.Status != workstore.ExecutorStatusCompleted ||
			replacement.PID <= 0 || replacement.ProcessStart == "" {
			verdict.Failures = append(verdict.Failures, "generation 2 did not register and complete")
		}
	}
	if snapshot.ActiveGeneration != 2 {
		verdict.Failures = append(verdict.Failures, fmt.Sprintf("active generation = %d; want 2", snapshot.ActiveGeneration))
	}
	if len(snapshot.Effects) != 1 || (len(snapshot.Effects) == 1 && snapshot.Effects[0].Generation != 2) {
		verdict.Failures = append(verdict.Failures, "recovery did not accept exactly one generation 2 effect")
	}
	if snapshot.Outcome == nil || workflowOutcome == nil {
		verdict.Failures = append(verdict.Failures, "recovery is missing the application or Workflow outcome")
	} else {
		verdict.WorkflowOutcomeMatchesApplication = *workflowOutcome == *snapshot.Outcome
		if !verdict.WorkflowOutcomeMatchesApplication {
			verdict.Failures = append(verdict.Failures, "Workflow result does not match the accepted application outcome")
		}
	}
	if hasEvent(snapshot.Events, "process_registered", 0, 1) {
		verdict.Failures = append(verdict.Failures, "superseded generation 1 registered a process")
	}
	if !eventsAppearInOrder(snapshot.Events, []eventMatch{
		{kind: "executor_launch_decided", attempt: 1, generation: 1},
		{kind: "activity_after_launch_decision", attempt: 1, generation: 1},
		{kind: "worker_killed", attempt: 1, generation: 1},
		{kind: "pending_launch_replaced", attempt: 2, generation: 2},
		{kind: "process_registered", attempt: 2, generation: 2},
		{kind: "effect_accepted", attempt: 2, generation: 2},
		{kind: "outcome_accepted", attempt: 2, generation: 2},
	}) {
		verdict.Failures = append(verdict.Failures, "recovery lifecycle evidence is missing or out of order")
	}
	verdict.InvariantSatisfied = len(verdict.Failures) == 0
}

type eventMatch struct {
	kind       string
	attempt    int32
	generation uint64
}

func eventsAppearInOrder(events []workstore.Event, matches []eventMatch) bool {
	previous := uint64(0)
	for _, match := range matches {
		sequence := uint64(0)
		for _, event := range events {
			if event.Kind == match.kind && event.Attempt == match.attempt && event.Generation == match.generation {
				sequence = event.Sequence
				break
			}
		}
		if sequence == 0 || sequence <= previous {
			return false
		}
		previous = sequence
	}
	return true
}
