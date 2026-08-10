package matrix

import (
	"context"
	"errors"
	"fmt"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/evidence"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/runner"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/semantics"
)

type liveArmExecutor struct {
	topology     protocol.Topology
	executor     *semantics.TemporalExecutor
	evidenceRoot string
}

func (e *liveArmExecutor) Topology() protocol.Topology { return e.topology }

func (e *liveArmExecutor) Ready(ctx context.Context) error {
	return e.executor.Ready(ctx)
}

func (e *liveArmExecutor) Execute(ctx context.Context, request runner.RunRequest) (runner.RunResult, error) {
	trackerBeadID := ""
	if request.Phase == protocol.PhasePilot {
		trackerBeadID = PilotTrackerBeadID
	}
	result, err := e.executor.Run(ctx, semantics.RunRequest{
		PairID: request.Block.PairID, ScheduleBlockID: request.Block.ScheduleBlockID,
		TrackerBeadID: trackerBeadID,
		Topology:      e.topology, Case: request.Block.Stratum.Case, Boundary: request.Block.Stratum.Boundary,
		Probe: request.Block.Stratum.Probe, Fanout: request.Block.Stratum.Fanout,
	})
	return persistLiveArmResult(e.evidenceRoot, request, result, err)
}

func persistLiveArmResult(
	evidenceRoot string,
	request runner.RunRequest,
	result semantics.EpisodeResult,
	runErr error,
) (runner.RunResult, error) {
	if result.Bundle.Manifest.RunID == "" {
		return runner.RunResult{RunID: request.RunID}, runErr
	}
	directory, evidenceErr := evidence.WriteRun(evidenceRoot, result.Bundle)
	return runner.RunResult{
		RunID: result.Bundle.Manifest.RunID, EvidenceDirectory: directory,
	}, errors.Join(runErr, evidenceErr)
}

func liveArmExecutors(executor *semantics.TemporalExecutor, root string) map[protocol.Topology]runner.Executor {
	return map[protocol.Topology]runner.Executor{
		protocol.TopologyDirectActivity: &liveArmExecutor{topology: protocol.TopologyDirectActivity, executor: executor, evidenceRoot: root},
		protocol.TopologyChildWorkflow:  &liveArmExecutor{topology: protocol.TopologyChildWorkflow, executor: executor, evidenceRoot: root},
	}
}

// SelectLiveSentinels chooses a predetermined publication-excluded block for
// every unsafe boundary plus protected and unfaulted baselines in both suites.
func SelectLiveSentinels(schedule protocol.Schedule) ([]protocol.PairBlock, error) {
	if schedule.Phase != protocol.PhasePublication {
		return nil, fmt.Errorf("%w: live sentinel schedule phase", protocol.ErrInvalidEvidence)
	}
	result := make([]protocol.PairBlock, 0, 23)
	seen := make(map[string]bool)
	for _, block := range schedule.Blocks {
		if block.Slot != 2 || block.Reserve {
			continue
		}
		selectBlock := block.Stratum.Probe == protocol.ProbeUnsafe ||
			(block.Stratum.Case == protocol.CaseJoinBarrier && block.Stratum.Fanout == 8 &&
				(block.Stratum.Probe == protocol.ProbeProtected || block.Stratum.Probe == protocol.ProbeUnfaulted)) ||
			(block.Stratum.Case == protocol.CaseCrashRecoveryBoundaries && block.Stratum.Fanout == 8 &&
				(block.Stratum.Probe == protocol.ProbeProtected || block.Stratum.Probe == protocol.ProbeUnfaulted))
		if !selectBlock {
			continue
		}
		if seen[block.Stratum.ID] {
			return nil, fmt.Errorf("%w: duplicate live sentinel stratum", protocol.ErrInvalidEvidence)
		}
		seen[block.Stratum.ID] = true
		result = append(result, block)
	}
	unsafe, passing := 0, 0
	for _, block := range result {
		if block.Stratum.Probe == protocol.ProbeUnsafe {
			unsafe++
		} else {
			passing++
		}
	}
	if len(result) != 23 || unsafe != 19 || passing != 4 {
		return nil, fmt.Errorf("%w: live sentinel coverage", protocol.ErrInvalidEvidence)
	}
	return result, nil
}

func accountLivePair(report *Report, execution runner.PairExecution) error {
	if execution.Admission != protocol.AdmissionValid || len(execution.Arms) != len(protocol.Topologies()) {
		return fmt.Errorf("%w: live sentinel pair admission %s: %v", protocol.ErrInvalidEvidence, execution.Block.PairID, execution.ReasonCodes)
	}
	report.LiveSentinelPairs++
	for _, arm := range execution.Arms {
		verdict := arm.Verdict
		if verdict.Admission != protocol.AdmissionValid || verdict.Liveness != protocol.OutcomePass || verdict.Diagnosability != protocol.OutcomePass {
			return fmt.Errorf("%w: live sentinel arm admission %s/%s", protocol.ErrInvalidEvidence, execution.Block.PairID, arm.Topology)
		}
		report.LiveSentinelArms++
		report.LiveHistoriesReplayed++
		if execution.Block.Stratum.Probe == protocol.ProbeUnsafe {
			report.LiveUnsafeArms++
			if verdict.Safety != protocol.OutcomeFail || verdict.EfficiencyEligible {
				return fmt.Errorf("%w: live unsafe sentinel did not independently fail safety", protocol.ErrInvalidEvidence)
			}
			report.LiveUnsafeArmsDistinguished++
			continue
		}
		report.LivePassingArms++
		if verdict.Correctness != protocol.OutcomePass || verdict.Safety != protocol.OutcomePass || !verdict.EfficiencyEligible {
			return fmt.Errorf("%w: live protected or unfaulted sentinel failed", protocol.ErrInvalidEvidence)
		}
		report.LivePassingArmsPassed++
	}
	return nil
}
