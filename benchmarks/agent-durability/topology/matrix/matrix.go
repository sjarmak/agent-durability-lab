package matrix

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/evidence"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/internal/sealedfs"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/internal/testfixture"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/oracle"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/runner"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/semantics"
)

const (
	FixtureConformanceKind = "topology-matrix-apparatus-fixture-v1"
	FullConformanceKind    = "topology-matrix-integrated-conformance-v1"
	ReportFile             = "matrix-report.json"
	ScheduleFile           = "frozen-publication-schedule.json"
	PreregistrationFile    = "frozen-preregistration.json"
	InventoryFile          = "matrix-inventory.json"
)

type Config struct {
	Root                string
	PreregistrationPath string
	TemporalPath        string
	WorkRoot            string
	AgentBinary         string
}

type Report struct {
	ProtocolVersion                string        `json:"protocol_version"`
	Kind                           string        `json:"kind"`
	CreatedAtUTC                   string        `json:"created_at_utc"`
	TrackerBeadID                  string        `json:"tracker_bead_id"`
	PublicationExcluded            bool          `json:"publication_excluded"`
	ExclusionReason                string        `json:"exclusion_reason"`
	Schedule                       ScheduleAudit `json:"schedule_audit"`
	SelectedStrata                 int           `json:"selected_strata"`
	ValidPairs                     int           `json:"valid_pairs"`
	ValidArms                      int           `json:"valid_arms"`
	UnsafeArms                     int           `json:"unsafe_arms"`
	UnsafeArmsDistinguished        int           `json:"unsafe_arms_distinguished"`
	ProtectedOrUnfaultedArms       int           `json:"protected_or_unfaulted_arms"`
	ProtectedOrUnfaultedArmsPassed int           `json:"protected_or_unfaulted_arms_passed"`
	InvalidControls                int           `json:"invalid_controls"`
	InvalidControlsRejected        int           `json:"invalid_controls_rejected"`
	LiveSentinelPairs              int           `json:"live_sentinel_pairs"`
	LiveSentinelArms               int           `json:"live_sentinel_arms"`
	LiveUnsafeArms                 int           `json:"live_unsafe_arms"`
	LiveUnsafeArmsDistinguished    int           `json:"live_unsafe_arms_distinguished"`
	LivePassingArms                int           `json:"live_passing_arms"`
	LivePassingArmsPassed          int           `json:"live_passing_arms_passed"`
	LiveHistoriesReplayed          int           `json:"live_histories_replayed"`
	HarnessBinarySHA256            string        `json:"harness_binary_sha256"`
	AgentBinarySHA256              string        `json:"agent_binary_sha256,omitempty"`
	TemporalBinarySHA256           string        `json:"temporal_binary_sha256,omitempty"`
	PreregistrationSHA256          string        `json:"preregistration_sha256"`
	ScheduleSHA256                 string        `json:"schedule_sha256"`
}

type fixtureExecutor struct {
	topology     protocol.Topology
	evidenceRoot string
	mutate       func(protocol.Topology, *protocol.EvidenceBundle)
}

func (e *fixtureExecutor) Topology() protocol.Topology { return e.topology }
func (e *fixtureExecutor) Ready(context.Context) error { return nil }

func (e *fixtureExecutor) Execute(_ context.Context, request runner.RunRequest) (runner.RunResult, error) {
	bundle := testfixture.Bundle(request.Block, e.topology)
	if e.mutate != nil {
		e.mutate(e.topology, &bundle)
	}
	bundle.Verdict = oracle.Evaluate(bundle)
	directory, err := evidence.WriteRun(e.evidenceRoot, bundle)
	return runner.RunResult{RunID: bundle.Manifest.RunID, EvidenceDirectory: directory}, err
}

func RunFixtureConformance(ctx context.Context, config Config) (Report, error) {
	return runConformance(ctx, config, false)
}

func RunConformance(ctx context.Context, config Config) (Report, error) {
	if config.TemporalPath == "" || config.WorkRoot == "" || config.AgentBinary == "" {
		return Report{}, fmt.Errorf("%w: live matrix config", protocol.ErrInvalidEvidence)
	}
	if err := ValidateDisjointPaths(config.Root, config.WorkRoot); err != nil {
		return Report{}, err
	}
	return runConformance(ctx, config, true)
}

func runConformance(ctx context.Context, config Config, live bool) (Report, error) {
	if ctx == nil || config.Root == "" || config.PreregistrationPath == "" {
		return Report{}, fmt.Errorf("%w: matrix config", protocol.ErrInvalidEvidence)
	}
	root, err := createRoot(config.Root)
	if err != nil {
		return Report{}, err
	}
	registrationData, err := os.ReadFile(config.PreregistrationPath)
	if err != nil {
		return Report{}, err
	}
	if err := writeBytesExclusive(filepath.Join(root, PreregistrationFile), registrationData); err != nil {
		return Report{}, err
	}
	registration, err := protocol.LoadPreregistration(filepath.Join(root, PreregistrationFile))
	if err != nil {
		return Report{}, err
	}
	schedule, err := protocol.BuildSchedule(registration, protocol.PhasePublication)
	if err != nil {
		return Report{}, err
	}
	audit, selected, err := AuditSchedule(registration, schedule)
	if err != nil {
		return Report{}, err
	}
	if err := sealedfs.WriteJSONExclusive(filepath.Join(root, ScheduleFile), schedule); err != nil {
		return Report{}, err
	}
	kind := FixtureConformanceKind
	if live {
		kind = FullConformanceKind
	}
	report := Report{
		ProtocolVersion: protocol.PublicationProtocolVersion,
		Kind:            kind, CreatedAtUTC: time.Now().UTC().Format(time.RFC3339Nano),
		TrackerBeadID: "temporal_projects-4ic.4", PublicationExcluded: true,
		ExclusionReason: "deterministic apparatus fixtures and sentinels are not independent paired episodes",
		Schedule:        audit, SelectedStrata: len(selected),
	}
	executable, err := os.Executable()
	if err != nil {
		return Report{}, err
	}
	report.HarnessBinarySHA256, err = sealedfs.HashRegularFile(executable)
	if err != nil {
		return Report{}, err
	}
	if live {
		report.AgentBinarySHA256, err = sealedfs.HashRegularFile(config.AgentBinary)
		if err != nil {
			return Report{}, err
		}
		report.TemporalBinarySHA256, err = sealedfs.HashRegularFile(config.TemporalPath)
		if err != nil {
			return Report{}, err
		}
	}
	fixtureRoot := filepath.Join(root, "fixtures")
	pairRoot := filepath.Join(fixtureRoot, "pairs")
	runRoot := filepath.Join(fixtureRoot, "runs")
	executors := fixtureExecutors(runRoot, nil)
	for _, block := range selected {
		execution, runErr := runner.RunPair(ctx, runner.Config{
			Root: pairRoot, EvidenceRoot: runRoot, Phase: protocol.PhasePublication,
			Registration: registration, Schedule: schedule, Executors: executors,
		}, block)
		if runErr != nil {
			return Report{}, runErr
		}
		if err := accountFixturePair(&report, execution); err != nil {
			return Report{}, err
		}
	}
	invalidSpecs, err := invalidControlSpecs(selected)
	if err != nil {
		return Report{}, err
	}
	report.InvalidControls = len(invalidSpecs)
	for _, spec := range invalidSpecs {
		controlRoot := filepath.Join(root, "invalid-controls", spec.name)
		execution, runErr := runner.RunPair(ctx, runner.Config{
			Root: filepath.Join(controlRoot, "pairs"), EvidenceRoot: filepath.Join(controlRoot, "runs"),
			Phase: protocol.PhasePublication, Registration: registration, Schedule: schedule,
			Executors: fixtureExecutors(filepath.Join(controlRoot, "runs"), spec.mutate),
		}, spec.block)
		if runErr != nil {
			return Report{}, runErr
		}
		if execution.Admission != protocol.AdmissionInvalid || len(execution.ReasonCodes) == 0 {
			return Report{}, fmt.Errorf("%w: invalid control %s was admitted", protocol.ErrInvalidEvidence, spec.name)
		}
		report.InvalidControlsRejected++
	}
	if live {
		liveBlocks, selectErr := SelectLiveSentinels(schedule)
		if selectErr != nil {
			return Report{}, selectErr
		}
		temporalExecutor, openErr := semantics.OpenTemporalExecutor(ctx, semantics.ExecutorConfig{
			TemporalPath: config.TemporalPath, WorkRoot: config.WorkRoot, AgentBinary: config.AgentBinary,
		})
		if openErr != nil {
			return Report{}, openErr
		}
		liveRoot := filepath.Join(root, "live-sentinels")
		liveExecutors := liveArmExecutors(temporalExecutor, filepath.Join(liveRoot, "runs"))
		for _, block := range liveBlocks {
			execution, runErr := runner.RunPair(ctx, runner.Config{
				Root: filepath.Join(liveRoot, "pairs"), EvidenceRoot: filepath.Join(liveRoot, "runs"),
				Phase: protocol.PhasePublication, Registration: registration, Schedule: schedule, Executors: liveExecutors,
			}, block)
			if runErr != nil {
				_ = temporalExecutor.Close()
				return Report{}, runErr
			}
			if err := accountLivePair(&report, execution); err != nil {
				_ = temporalExecutor.Close()
				return Report{}, err
			}
		}
		if closeErr := temporalExecutor.Close(); closeErr != nil {
			return Report{}, closeErr
		}
	}
	report.PreregistrationSHA256, err = sealedfs.HashRegularFile(filepath.Join(root, PreregistrationFile))
	if err != nil {
		return Report{}, err
	}
	report.ScheduleSHA256, err = sealedfs.HashRegularFile(filepath.Join(root, ScheduleFile))
	if err != nil {
		return Report{}, err
	}
	if err := sealedfs.WriteJSONExclusive(filepath.Join(root, ReportFile), report); err != nil {
		return Report{}, err
	}
	if err := writeInventory(root, kind); err != nil {
		return Report{}, err
	}
	return Audit(root)
}

func fixtureExecutors(root string, mutate func(protocol.Topology, *protocol.EvidenceBundle)) map[protocol.Topology]runner.Executor {
	return map[protocol.Topology]runner.Executor{
		protocol.TopologyDirectActivity: &fixtureExecutor{topology: protocol.TopologyDirectActivity, evidenceRoot: root, mutate: mutate},
		protocol.TopologyChildWorkflow:  &fixtureExecutor{topology: protocol.TopologyChildWorkflow, evidenceRoot: root, mutate: mutate},
	}
}

func accountFixturePair(report *Report, execution runner.PairExecution) error {
	if execution.Admission != protocol.AdmissionValid || len(execution.Arms) != len(protocol.Topologies()) {
		return fmt.Errorf("%w: fixture pair admission %s: %v", protocol.ErrInvalidEvidence, execution.Block.PairID, execution.ReasonCodes)
	}
	report.ValidPairs++
	for _, arm := range execution.Arms {
		verdict := arm.Verdict
		if verdict.Admission != protocol.AdmissionValid || verdict.Liveness != protocol.OutcomePass || verdict.Diagnosability != protocol.OutcomePass {
			return fmt.Errorf("%w: fixture arm admission %s/%s", protocol.ErrInvalidEvidence, execution.Block.PairID, arm.Topology)
		}
		report.ValidArms++
		if execution.Block.Stratum.Probe == protocol.ProbeUnsafe {
			report.UnsafeArms++
			if verdict.Safety != protocol.OutcomeFail || verdict.EfficiencyEligible {
				return fmt.Errorf("%w: unsafe fixture did not independently fail safety", protocol.ErrInvalidEvidence)
			}
			report.UnsafeArmsDistinguished++
			continue
		}
		report.ProtectedOrUnfaultedArms++
		if verdict.Correctness != protocol.OutcomePass || verdict.Safety != protocol.OutcomePass || !verdict.EfficiencyEligible {
			return fmt.Errorf("%w: protected or unfaulted fixture failed", protocol.ErrInvalidEvidence)
		}
		report.ProtectedOrUnfaultedArmsPassed++
	}
	return nil
}

type invalidControlSpec struct {
	name              string
	block             protocol.PairBlock
	expectInputMatch  bool
	expectInvalidArms bool
	mutate            func(protocol.Topology, *protocol.EvidenceBundle)
}

func invalidControlSpecs(selected []protocol.PairBlock) ([]invalidControlSpec, error) {
	var protected, recovery protocol.PairBlock
	for _, block := range selected {
		if protected.PairID == "" && block.Stratum.Probe == protocol.ProbeProtected {
			protected = block
		}
		if recovery.PairID == "" && block.Stratum.Case == protocol.CaseBackpressureOverload && block.Stratum.Probe == protocol.ProbeProtected {
			recovery = block
		}
	}
	if protected.PairID == "" || recovery.PairID == "" {
		return nil, fmt.Errorf("%w: invalid-control fixture selection", protocol.ErrInvalidEvidence)
	}
	return []invalidControlSpec{
		{name: "missing-lineage", block: protected, expectInputMatch: true, expectInvalidArms: true, mutate: func(_ protocol.Topology, bundle *protocol.EvidenceBundle) {
			bundle.Lineage.Edges = bundle.Lineage.Edges[1:]
		}},
		{name: "replay-failure", block: protected, expectInputMatch: true, expectInvalidArms: true, mutate: func(_ protocol.Topology, bundle *protocol.EvidenceBundle) {
			bundle.NativeHistory.ReplayCompatible = false
			bundle.NativeHistory.ReplayError = "fixture nondeterminism"
			bundle.Execution.ReplayVerified = false
		}},
		{name: "recovery-control-mismatch", block: recovery, expectInputMatch: true, expectInvalidArms: true, mutate: func(_ protocol.Topology, bundle *protocol.EvidenceBundle) {
			bundle.Workload.ProhibitedActionCount++
		}},
		{name: "paired-input-mismatch", block: protected, mutate: func(topology protocol.Topology, bundle *protocol.EvidenceBundle) {
			if topology == protocol.TopologyChildWorkflow {
				bundle.EffectiveInput.ActivityOptionsSHA256 = testfixture.Hash('9')
			}
		}},
	}, nil
}

func createRoot(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	parent := filepath.Dir(absolute)
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return "", err
	}
	resolvedParent, err := filepath.EvalSymlinks(parent)
	if err != nil {
		return "", err
	}
	root := filepath.Join(resolvedParent, filepath.Base(absolute))
	if err := os.Mkdir(root, 0o750); err != nil {
		if errors.Is(err, os.ErrExist) {
			return "", fmt.Errorf("%w: matrix root", protocol.ErrEvidenceExists)
		}
		return "", err
	}
	return root, nil
}

func writeBytesExclusive(path string, data []byte) (returnErr error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	if _, err := file.Write(data); err != nil {
		return err
	}
	return file.Sync()
}
