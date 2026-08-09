// Package systemplan defines the adapter-neutral durable procedure that each
// required system must execute before its native record can be attached to a
// common v2 evidence run.
package systemplan

import (
	"fmt"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
)

type Step struct {
	Sequence    int    `json:"sequence"`
	ID          string `json:"id"`
	Kind        string `json:"kind"`
	FailureOnce bool   `json:"failure_once"`
}

type Plan struct {
	Case  protocol.CaseID `json:"case"`
	Probe protocol.Probe  `json:"probe"`
	Trial int             `json:"trial"`
	Steps []Step          `json:"steps"`
}

type Execution struct {
	SystemID       string                  `json:"system_id"`
	AdapterID      string                  `json:"adapter_id"`
	AdapterVersion string                  `json:"adapter_version"`
	ExecutionID    string                  `json:"execution_id"`
	Native         []protocol.NativeRecord `json:"native"`
	Settings       map[string]string       `json:"settings"`
	ReplayVerified bool                    `json:"replay_verified"`
}

func Build(benchmarkCase protocol.CaseID, probe protocol.Probe, trial int) (Plan, error) {
	if !benchmarkCase.Valid() || !probe.Valid() || trial < 1 {
		return Plan{}, fmt.Errorf("%w: valid case, probe, and trial are required", protocol.ErrInvalidEvidence)
	}
	kinds := caseSteps(benchmarkCase)
	steps := make([]Step, 0, len(kinds))
	for index, kind := range kinds {
		steps = append(steps, Step{
			Sequence: index + 1, ID: fmt.Sprintf("%s-%s-%02d", benchmarkCase, probe, index+1), Kind: kind,
			FailureOnce: probe != protocol.ProbeUnfaulted && kind == "fault-boundary",
		})
	}
	plan := Plan{Case: benchmarkCase, Probe: probe, Trial: trial, Steps: steps}
	return plan, plan.Validate()
}

func (p Plan) Validate() error {
	if !p.Case.Valid() || !p.Probe.Valid() || p.Trial < 1 || len(p.Steps) < 3 {
		return fmt.Errorf("%w: incomplete durable system plan", protocol.ErrInvalidEvidence)
	}
	seen := make(map[string]bool, len(p.Steps))
	faults := 0
	for index, step := range p.Steps {
		if step.Sequence != index+1 || step.ID == "" || step.Kind == "" || seen[step.ID] {
			return fmt.Errorf("%w: invalid durable system step", protocol.ErrInvalidEvidence)
		}
		seen[step.ID] = true
		if step.FailureOnce {
			faults++
		}
	}
	if p.Probe == protocol.ProbeUnfaulted && faults != 0 || p.Probe != protocol.ProbeUnfaulted && faults != 1 {
		return fmt.Errorf("%w: durable plan fault count differs from probe", protocol.ErrInvalidEvidence)
	}
	return nil
}

func caseSteps(benchmarkCase protocol.CaseID) []string {
	switch benchmarkCase {
	case protocol.CaseABAReacquisition:
		return []string{"operation-ready", "owner-a-generation-7", "fault-boundary", "owner-b-generation-8", "owner-b-complete", "owner-a-generation-9", "current-outcome", "release-stale-generation-7", "acknowledge"}
	case protocol.CaseLayeredRetryAmplification:
		return []string{"operation-ready", "first-request-accepted", "fault-boundary", "retry-budget", "dependency-recovered", "outcome", "acknowledge"}
	case protocol.CaseOutageBacklogRecovery:
		return []string{"steady-state", "outage-commit", "fault-boundary", "backlog-target", "restoration-commit", "bounded-drain", "outcome", "acknowledge"}
	case protocol.CaseBackpressureOverload:
		return []string{"workers-ready", "offered-load", "fault-boundary", "admission", "bounded-processing", "reconcile", "acknowledge"}
	case protocol.CasePoisonWorkIsolation:
		return []string{"mixed-cohort-ready", "poison-release", "fault-boundary", "retry-budget", "quarantine", "healthy-progress", "acknowledge"}
	case protocol.CaseSilentProgress:
		return []string{"executor-registered", "progress-accepted", "fault-boundary", "progress-deadline", "revoke", "replace", "stale-publish-check", "legitimate-wait-check", "acknowledge"}
	default:
		return nil
	}
}
