package lab

import (
	"fmt"

	"github.com/sjarmak/temporal_projects/internal/workstore"
)

type Verdict struct {
	Mode                              workstore.Mode     `json:"mode"`
	RunValid                          bool               `json:"run_valid"`
	InvariantSatisfied                bool               `json:"invariant_satisfied"`
	ExpectedObservation               bool               `json:"expected_observation"`
	WorkflowOutcomeMatchesApplication bool               `json:"workflow_outcome_matches_application"`
	WorkflowOutcome                   workstore.Outcome  `json:"workflow_outcome"`
	ApplicationOutcome                *workstore.Outcome `json:"application_outcome,omitempty"`
	Failures                          []string           `json:"failures,omitempty"`
}

func Verify(mode workstore.Mode, snapshot workstore.Snapshot, workflowOutcome workstore.Outcome) Verdict {
	verdict := Verdict{Mode: mode, WorkflowOutcome: workflowOutcome, Failures: make([]string, 0)}
	if snapshot.SessionID == "" {
		verdict.Failures = append(verdict.Failures, "missing session identity")
	}
	if snapshot.Mode != mode {
		verdict.Failures = append(verdict.Failures, fmt.Sprintf("snapshot mode %q does not match %q", snapshot.Mode, mode))
	}
	if snapshot.Outcome == nil {
		verdict.Failures = append(verdict.Failures, "missing accepted terminal outcome")
	} else {
		applicationOutcome := *snapshot.Outcome
		verdict.ApplicationOutcome = &applicationOutcome
		verdict.WorkflowOutcomeMatchesApplication = workflowOutcome == applicationOutcome
		if !verdict.WorkflowOutcomeMatchesApplication {
			verdict.Failures = append(verdict.Failures, "Workflow result does not match the accepted application outcome")
		}
	}

	switch mode {
	case workstore.ModeUnsafe:
		verifyUnsafe(snapshot, &verdict)
	case workstore.ModeReattach:
		verifyReattachment(snapshot, &verdict)
	case workstore.ModeFenced:
		verifyFenced(snapshot, &verdict)
	default:
		verdict.Failures = append(verdict.Failures, "unknown experiment mode")
	}
	verdict.RunValid = len(verdict.Failures) == 0
	return verdict
}

func verifyUnsafe(snapshot workstore.Snapshot, verdict *Verdict) {
	duplicateObserved := len(snapshot.Executors) >= 2 && len(snapshot.Effects) >= 2
	if !duplicateObserved {
		verdict.Failures = append(verdict.Failures, "unsafe control did not produce competing executors and duplicate effects")
	}
	if !hasEvent(snapshot.Events, "child_alive_after_worker_kill", 0, 0) {
		verdict.Failures = append(verdict.Failures, "child survival after Worker death was not observed")
	}
	if !hasEvent(snapshot.Events, "executor_launch_decided", 2, 0) {
		verdict.Failures = append(verdict.Failures, "second Activity attempt did not launch the competing executor")
	}
	verdict.ExpectedObservation = duplicateObserved
	verdict.InvariantSatisfied = false
}

func verifyReattachment(snapshot workstore.Snapshot, verdict *Verdict) {
	if len(snapshot.Executors) != 1 {
		verdict.Failures = append(verdict.Failures, fmt.Sprintf("reattachment created %d executors; want 1", len(snapshot.Executors)))
	}
	if len(snapshot.Effects) != 1 {
		verdict.Failures = append(verdict.Failures, fmt.Sprintf("reattachment accepted %d effects; want 1", len(snapshot.Effects)))
	}
	if !hasEvent(snapshot.Events, "child_alive_after_worker_kill", 0, 1) {
		verdict.Failures = append(verdict.Failures, "generation 1 child survival was not observed")
	}
	if !hasEvent(snapshot.Events, "activity_reattached", 2, 1) {
		verdict.Failures = append(verdict.Failures, "attempt 2 did not reattach to generation 1")
	}
	verdict.InvariantSatisfied = len(verdict.Failures) == 0
	verdict.ExpectedObservation = verdict.InvariantSatisfied
}

func verifyFenced(snapshot workstore.Snapshot, verdict *Verdict) {
	if len(snapshot.Executors) != 2 {
		verdict.Failures = append(verdict.Failures, fmt.Sprintf("fenced replacement recorded %d executors; want 2", len(snapshot.Executors)))
	}
	if snapshot.ActiveGeneration != 2 {
		verdict.Failures = append(verdict.Failures, fmt.Sprintf("active generation = %d; want 2", snapshot.ActiveGeneration))
	}
	if len(snapshot.Effects) != 1 || (len(snapshot.Effects) == 1 && snapshot.Effects[0].Generation != 2) {
		verdict.Failures = append(verdict.Failures, "only generation 2 effect should be accepted")
	}
	if !hasEvent(snapshot.Events, "child_alive_after_worker_kill", 0, 1) {
		verdict.Failures = append(verdict.Failures, "generation 1 child survival was not observed")
	}
	if !hasEvent(snapshot.Events, "owner_replaced", 2, 2) {
		verdict.Failures = append(verdict.Failures, "attempt 2 did not explicitly replace owner generation")
	}
	accepted := eventSequence(snapshot.Events, "outcome_accepted", 2)
	staleEffect := eventSequence(snapshot.Events, "effect_rejected_stale", 1)
	staleCompletion := eventSequence(snapshot.Events, "completion_rejected_stale", 1)
	if accepted == 0 || staleEffect == 0 || staleCompletion == 0 {
		verdict.Failures = append(verdict.Failures, "missing replacement outcome or stale rejection evidence")
	} else if !(accepted < staleEffect && staleEffect < staleCompletion) {
		verdict.Failures = append(verdict.Failures, "stale attempts were not delayed until after replacement outcome acceptance")
	}
	verdict.InvariantSatisfied = len(verdict.Failures) == 0
	verdict.ExpectedObservation = verdict.InvariantSatisfied
}

func hasEvent(events []workstore.Event, kind string, attempt int32, generation uint64) bool {
	for _, event := range events {
		if event.Kind == kind && (attempt == 0 || event.Attempt == attempt) && (generation == 0 || event.Generation == generation) {
			return true
		}
	}
	return false
}

func eventSequence(events []workstore.Event, kind string, generation uint64) uint64 {
	for _, event := range events {
		if event.Kind == kind && event.Generation == generation {
			return event.Sequence
		}
	}
	return 0
}
