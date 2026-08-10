// Package runner executes one frozen matched topology pair sequentially and
// preserves pair-level admission evidence.
package runner

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/oracle"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
)

type RunRequest struct {
	Phase    protocol.Phase     `json:"phase"`
	Block    protocol.PairBlock `json:"block"`
	Topology protocol.Topology  `json:"topology"`
	RunID    string             `json:"run_id"`
}

type RunResult struct {
	RunID             string `json:"run_id"`
	EvidenceDirectory string `json:"evidence_directory"`
	ExclusionReason   string `json:"exclusion_reason,omitempty"`
}

type Executor interface {
	Topology() protocol.Topology
	Ready(context.Context) error
	Execute(context.Context, RunRequest) (RunResult, error)
}

type Config struct {
	Root         string
	EvidenceRoot string
	Phase        protocol.Phase
	Registration protocol.Preregistration
	Schedule     protocol.Schedule
	Executors    map[protocol.Topology]Executor
}

type ArmRun struct {
	Topology          protocol.Topology `json:"topology"`
	Order             int               `json:"order"`
	RunID             string            `json:"run_id"`
	EvidenceDirectory string            `json:"evidence_directory,omitempty"`
	ReadyAtUTC        string            `json:"ready_at_utc,omitempty"`
	StartedAtUTC      string            `json:"started_at_utc,omitempty"`
	FinishedAtUTC     string            `json:"finished_at_utc,omitempty"`
	DurationNS        int64             `json:"duration_ns"`
	ExclusionReason   string            `json:"exclusion_reason,omitempty"`
	Verdict           protocol.Verdict  `json:"verdict"`
	Error             string            `json:"error,omitempty"`
}

type PairExecution struct {
	ProtocolVersion    string             `json:"protocol_version"`
	Phase              protocol.Phase     `json:"phase"`
	Block              protocol.PairBlock `json:"block"`
	Admission          protocol.Admission `json:"admission"`
	ReasonCodes        []string           `json:"reason_codes,omitempty"`
	EfficiencyEligible bool               `json:"efficiency_eligible"`
	Arms               []ArmRun           `json:"arms"`
}

func RunPair(ctx context.Context, config Config, block protocol.PairBlock) (PairExecution, error) {
	if ctx == nil {
		return PairExecution{}, fmt.Errorf("%w: nil runner context", protocol.ErrInvalidEvidence)
	}
	if err := validateConfig(config, block); err != nil {
		return PairExecution{}, err
	}
	directory, err := createPairDirectory(config.Root, block.PairID)
	if err != nil {
		return PairExecution{}, err
	}
	execution := PairExecution{
		ProtocolVersion: protocol.PublicationProtocolVersion,
		Phase:           config.Phase,
		Block:           cloneBlock(block),
		Admission:       protocol.AdmissionValid,
	}
	bundles := make([]protocol.EvidenceBundle, 0, 2)
	for order, topology := range block.TopologyOrder {
		arm, bundle, reasons := executeArm(ctx, config, block, topology, order+1)
		execution.ReasonCodes = appendReasons(execution.ReasonCodes, reasons)
		execution.Arms = append(execution.Arms, arm)
		if bundle != nil {
			bundles = append(bundles, *bundle)
		}
	}
	finalizePair(&execution, bundles)
	if err := writePairEvidence(directory, execution); err != nil {
		return PairExecution{}, err
	}
	return execution, nil
}

func executeArm(ctx context.Context, config Config, block protocol.PairBlock, topology protocol.Topology, order int) (ArmRun, *protocol.EvidenceBundle, []string) {
	executor := config.Executors[topology]
	runID := block.PairID + "/" + string(topology)
	arm := ArmRun{Topology: topology, Order: order, RunID: runID}
	var reasons []string
	if err := executor.Ready(ctx); err != nil {
		arm.Error = err.Error()
		return arm, nil, appendReason(reasons, string(topology)+":readiness_failed")
	}
	arm.ReadyAtUTC = time.Now().UTC().Format(time.RFC3339Nano)
	started := time.Now()
	arm.StartedAtUTC = started.UTC().Format(time.RFC3339Nano)
	result, executeErr := executor.Execute(ctx, RunRequest{Phase: config.Phase, Block: cloneBlock(block), Topology: topology, RunID: runID})
	finished := time.Now()
	arm.FinishedAtUTC = finished.UTC().Format(time.RFC3339Nano)
	arm.DurationNS = finished.Sub(started).Nanoseconds()
	arm.EvidenceDirectory = result.EvidenceDirectory
	arm.ExclusionReason = result.ExclusionReason
	if executeErr != nil {
		arm.Error = executeErr.Error()
		reasons = appendReason(reasons, string(topology)+":execution_failed")
	}
	if result.RunID != runID {
		reasons = appendReason(reasons, string(topology)+":run_identity_mismatch")
	}
	if result.ExclusionReason != "" {
		code := string(topology) + ":infrastructure_exclusion:" + result.ExclusionReason
		if !allowedInfrastructureExclusion(result.ExclusionReason) {
			code = string(topology) + ":forbidden_outcome_exclusion:" + result.ExclusionReason
		}
		reasons = appendReason(reasons, code)
	}
	if result.EvidenceDirectory == "" {
		return arm, nil, appendReason(reasons, string(topology)+":evidence_missing")
	}
	bundle, verdict, verifyErr := oracle.VerifyRun(config.EvidenceRoot, result.EvidenceDirectory)
	arm.Verdict = verdict
	if verifyErr != nil {
		if arm.Error == "" {
			arm.Error = verifyErr.Error()
		}
		return arm, nil, appendReason(reasons, string(topology)+":evidence_invalid")
	}
	if !matchesRequest(bundle.Manifest, block, topology, runID) {
		reasons = appendReason(reasons, string(topology)+":arm_or_block_mismatch")
	}
	if verdict.Admission != protocol.AdmissionValid {
		reasons = appendReason(reasons, string(topology)+":run_invalid")
	}
	return arm, &bundle, reasons
}

func finalizePair(execution *PairExecution, bundles []protocol.EvidenceBundle) {
	if len(bundles) == 2 {
		if !bundles[0].EffectiveInput.MatchedWith(bundles[1].EffectiveInput) {
			execution.ReasonCodes = appendReason(execution.ReasonCodes, "paired_arm_input_mismatch")
		}
		if bundles[0].Manifest.LogicalOperationID != bundles[1].Manifest.LogicalOperationID ||
			bundles[0].Manifest.TrackerBeadID != bundles[1].Manifest.TrackerBeadID {
			execution.ReasonCodes = appendReason(execution.ReasonCodes, "paired_arm_identity_mismatch")
		}
	}
	if execution.Block.Stratum.Probe == protocol.ProbeUnsafe && len(execution.Arms) == 2 {
		for _, arm := range execution.Arms {
			verdict := arm.Verdict
			if verdict.Admission == protocol.AdmissionValid && verdict.Safety != protocol.OutcomeFail {
				execution.ReasonCodes = appendReason(execution.ReasonCodes, string(arm.Topology)+":unsafe_control_not_distinguishing")
			}
		}
	}
	if len(execution.ReasonCodes) > 0 {
		execution.Admission = protocol.AdmissionInvalid
	}
	execution.EfficiencyEligible = execution.Admission == protocol.AdmissionValid && len(execution.Arms) == 2
	for _, arm := range execution.Arms {
		execution.EfficiencyEligible = execution.EfficiencyEligible && arm.Verdict.EfficiencyEligible
	}
}

func validateConfig(config Config, block protocol.PairBlock) error {
	if config.Root == "" || config.EvidenceRoot == "" || !config.Phase.Valid() || config.Schedule.Phase != config.Phase {
		return fmt.Errorf("%w: runner roots or phase", protocol.ErrInvalidEvidence)
	}
	if err := protocol.ValidateSchedule(config.Registration, config.Phase, config.Schedule); err != nil {
		return err
	}
	if err := block.Validate(); err != nil {
		return err
	}
	if block.Index > len(config.Schedule.Blocks) || !reflect.DeepEqual(config.Schedule.Blocks[block.Index-1], block) {
		return fmt.Errorf("%w: pair block differs from frozen schedule", protocol.ErrInvalidEvidence)
	}
	if len(config.Executors) != 2 {
		return fmt.Errorf("%w: executor count", protocol.ErrInvalidEvidence)
	}
	for _, topology := range protocol.Topologies() {
		executor := config.Executors[topology]
		if executor == nil || executor.Topology() != topology {
			return fmt.Errorf("%w: executor identity", protocol.ErrInvalidEvidence)
		}
	}
	return nil
}

func matchesRequest(manifest protocol.Manifest, block protocol.PairBlock, topology protocol.Topology, runID string) bool {
	return manifest.RunID == runID && manifest.PairID == block.PairID && manifest.ScheduleBlockID == block.ScheduleBlockID &&
		manifest.Topology == topology && manifest.Case == block.Stratum.Case && manifest.Boundary == block.Stratum.Boundary &&
		manifest.Probe == block.Stratum.Probe && manifest.Fanout == block.Stratum.Fanout
}

func allowedInfrastructureExclusion(reason string) bool {
	return slices.Contains([]string{"setup_failed", "readiness_failed", "teardown_failed", "cleanup_failed", "history_capture_failed"}, reason)
}

func appendReason(reasons []string, reason string) []string {
	if !slices.Contains(reasons, reason) {
		return append(reasons, reason)
	}
	return reasons
}

func appendReasons(reasons, additions []string) []string {
	for _, addition := range additions {
		reasons = appendReason(reasons, addition)
	}
	return reasons
}

func cloneBlock(block protocol.PairBlock) protocol.PairBlock {
	clone := block
	clone.TopologyOrder = slices.Clone(block.TopologyOrder)
	return clone
}
