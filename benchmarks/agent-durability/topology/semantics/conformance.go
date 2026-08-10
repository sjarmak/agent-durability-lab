package semantics

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/evidence"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/oracle"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
)

type ConformanceSummary struct {
	Scenarios       int `json:"scenarios"`
	Trials          int `json:"trials"`
	PairsAttempted  int `json:"pairs_attempted"`
	RunsPreserved   int `json:"runs_preserved"`
	ValidPassRuns   int `json:"valid_pass_runs"`
	ValidFailRuns   int `json:"valid_fail_runs"`
	InvalidOrErrors int `json:"invalid_or_errors"`
}

// RunConformance executes every semantics boundary in both topology arms and
// writes each complete run through the append-only evidence writer. It keeps
// running the paired arm and later scenarios after a logical or apparatus
// failure so the failure cannot select its own evidence population.
func RunConformance(
	ctx context.Context,
	executor *TemporalExecutor,
	evidenceRoot string,
	fanout int,
	trials int,
) (ConformanceSummary, error) {
	return runConformanceScenarios(ctx, executor, evidenceRoot, fanout, trials, "semantics-v1", FrozenSemanticsScenarios())
}

// RunRecoveryConformance executes the complete canonical recovery mechanism
// matrix. Scale and publication repetitions remain the responsibility of the
// frozen 88-stratum runner.
func RunRecoveryConformance(
	ctx context.Context,
	executor *TemporalExecutor,
	evidenceRoot string,
	fanout int,
	trials int,
) (ConformanceSummary, error) {
	if fanout != 32 {
		return ConformanceSummary{Scenarios: len(FrozenRecoveryScenarios()), Trials: trials},
			fmt.Errorf("%w: recovery conformance requires canonical fanout 32", protocol.ErrInvalidEvidence)
	}
	return runConformanceScenarios(ctx, executor, evidenceRoot, fanout, trials, "recovery-v1", FrozenRecoveryScenarios())
}

func runConformanceScenarios(
	ctx context.Context,
	executor *TemporalExecutor,
	evidenceRoot string,
	fanout int,
	trials int,
	suite string,
	scenarios []Scenario,
) (ConformanceSummary, error) {
	summary := ConformanceSummary{Scenarios: len(scenarios), Trials: trials}
	if ctx == nil || executor == nil || evidenceRoot == "" || !slices.Contains([]int{8, 32, 128}, fanout) || trials < 1 {
		return summary, fmt.Errorf("%w: topology conformance configuration", protocol.ErrInvalidEvidence)
	}
	var failures error
	for _, scenario := range scenarios {
		for trial := 1; trial <= trials; trial++ {
			summary.PairsAttempted++
			pairID := fmt.Sprintf(
				"development/%s/%s/%s/%s/fanout-%03d/trial-%02d",
				suite, scenario.Case, shortDigest(scenario.Boundary), scenario.Probe, fanout, trial,
			)
			results := make(map[protocol.Topology]EpisodeResult, len(protocol.Topologies()))
			for _, topology := range protocol.Topologies() {
				result, err := executor.Run(ctx, RunRequest{
					PairID: pairID, ScheduleBlockID: "schedule-block/" + pairID, Topology: topology,
					Case: scenario.Case, Boundary: scenario.Boundary, Probe: scenario.Probe, Fanout: fanout,
				})
				if err != nil {
					summary.InvalidOrErrors++
					failures = errors.Join(failures, fmt.Errorf("%s %s trial %d %s: %w", scenario.Case, scenario.Boundary, trial, topology, err))
					continue
				}
				directory, err := evidence.WriteRun(evidenceRoot, result.Bundle)
				if err != nil {
					summary.InvalidOrErrors++
					failures = errors.Join(failures, fmt.Errorf("preserve %s: %w", result.Bundle.Manifest.RunID, err))
					continue
				}
				summary.RunsPreserved++
				_, storedVerdict, err := oracle.VerifyRun(evidenceRoot, directory)
				if err != nil {
					summary.InvalidOrErrors++
					failures = errors.Join(failures, fmt.Errorf("reload sealed run %s: %w", directory, err))
					continue
				}
				if !reflect.DeepEqual(storedVerdict, result.Bundle.Verdict) {
					summary.InvalidOrErrors++
					failures = errors.Join(failures, fmt.Errorf("reloaded verdict differs for %s", directory))
					continue
				}
				results[topology] = result
				verdict := result.Bundle.Verdict
				if verdict.Admission != protocol.AdmissionValid || !result.NativeHistory.ReplayCompatible {
					summary.InvalidOrErrors++
					failures = errors.Join(failures, fmt.Errorf("%s admitted/replay verdict from %s: %+v", topology, directory, verdict))
					continue
				}
				if scenario.Probe == protocol.ProbeUnsafe {
					if verdict.Safety != protocol.OutcomeFail || verdict.EfficiencyEligible {
						summary.InvalidOrErrors++
						failures = errors.Join(failures, fmt.Errorf("%s nondistinguishing unsafe control at %s", topology, directory))
						continue
					}
					summary.ValidFailRuns++
				} else {
					if !verdict.EfficiencyEligible {
						summary.InvalidOrErrors++
						failures = errors.Join(failures, fmt.Errorf("%s protected/unfaulted failure at %s: %+v", topology, directory, verdict))
						continue
					}
					summary.ValidPassRuns++
				}
			}
			direct, directOK := results[protocol.TopologyDirectActivity]
			child, childOK := results[protocol.TopologyChildWorkflow]
			if directOK && childOK && !direct.Bundle.EffectiveInput.MatchedWith(child.Bundle.EffectiveInput) {
				summary.InvalidOrErrors++
				failures = errors.Join(failures, fmt.Errorf("paired effective inputs differ for %s", pairID))
			}
		}
	}
	return summary, failures
}
