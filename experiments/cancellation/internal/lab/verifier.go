package lab

import (
	"fmt"

	"github.com/sjarmak/temporal_projects/internal/workstore"
)

func Verify(scenario Scenario, snapshot workstore.Snapshot) Verdict {
	verdict := Verdict{RunValid: true, ExpectedObservation: true, InvariantSatisfied: scenario.Safe()}
	if !scenario.Valid() {
		verdict.RunValid = false
		verdict.ExpectedObservation = false
		verdict.InvariantSatisfied = false
		verdict.Failures = append(verdict.Failures, fmt.Sprintf("unknown scenario %q", scenario))
		return verdict
	}
	if len(snapshot.Executors) != 1 {
		verdict.RunValid = false
		verdict.Failures = append(verdict.Failures, fmt.Sprintf("executors = %d; want 1", len(snapshot.Executors)))
	}
	if scenario == ScenarioTemporalControl {
		verifyTemporalControl(snapshot, &verdict)
		return verdict
	}
	verifySafeCancellation(scenario, snapshot, &verdict)
	return verdict
}

func verifyTemporalControl(snapshot workstore.Snapshot, verdict *Verdict) {
	verdict.InvariantSatisfied = false
	if snapshot.Cancellation != nil {
		verdict.RunValid = false
		verdict.ExpectedObservation = false
		verdict.Failures = append(verdict.Failures, "Temporal-only control unexpectedly committed application cancellation")
	}
	if len(snapshot.Effects) != 1 || snapshot.Outcome == nil {
		verdict.RunValid = false
		verdict.ExpectedObservation = false
		verdict.Failures = append(verdict.Failures, "detached child did not prove post-Workflow-cancel mutation authority")
	}
}

func verifySafeCancellation(scenario Scenario, snapshot workstore.Snapshot, verdict *Verdict) {
	if snapshot.Cancellation == nil {
		verdict.RunValid = false
		verdict.ExpectedObservation = false
		verdict.InvariantSatisfied = false
		verdict.Failures = append(verdict.Failures, "application cancellation is missing")
		return
	}
	if len(snapshot.Effects) != 0 || snapshot.Outcome != nil {
		verdict.ExpectedObservation = false
		verdict.InvariantSatisfied = false
		verdict.Failures = append(verdict.Failures, "post-cancel effect or outcome was accepted")
	}
	if snapshot.Cancellation.Acknowledgement == nil {
		verdict.ExpectedObservation = false
		verdict.InvariantSatisfied = false
		verdict.Failures = append(verdict.Failures, "cooperative cancellation acknowledgement is missing")
	}
	required := []string{
		"cancellation_committed", "executor_stop_delivery_attempted", "executor_stop_delivery_sent",
		"executor_stop_received", "cancellation_acknowledged", "tool_child_registered", "tool_child_stop_received",
	}
	if scenario == ScenarioWorkerDeathSafe {
		required = append(required, "worker_killed")
	}
	if scenario == ScenarioFrozenSafe {
		required = append(required, "process_tree_frozen", "process_tree_resumed")
	}
	for _, kind := range required {
		if !hasEvent(snapshot.Events, kind) {
			verdict.RunValid = false
			verdict.ExpectedObservation = false
			verdict.Failures = append(verdict.Failures, "missing event "+kind)
		}
	}
	for _, executor := range snapshot.Executors {
		if executor.Status != workstore.ExecutorStatusCanceled {
			verdict.ExpectedObservation = false
			verdict.InvariantSatisfied = false
			verdict.Failures = append(verdict.Failures, "executor is not durably canceled")
		}
	}
}

func hasEvent(events []workstore.Event, kind string) bool {
	for _, event := range events {
		if event.Kind == kind {
			return true
		}
	}
	return false
}
