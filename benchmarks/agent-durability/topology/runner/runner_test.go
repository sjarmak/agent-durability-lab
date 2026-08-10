package runner

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/evidence"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/internal/testfixture"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/oracle"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
)

func TestRunPairExecutesFrozenArmOrderSequentiallyAndSealsPair(t *testing.T) {
	registration, schedule, block := frozenProtectedBlock(t)
	root := t.TempDir()
	evidenceRoot := filepath.Join(root, "runs")
	calls := []string{}
	executors := map[protocol.Topology]Executor{}
	for _, topology := range protocol.Topologies() {
		executors[topology] = &fakeExecutor{topology: topology, evidenceRoot: evidenceRoot, calls: &calls}
	}
	execution, err := RunPair(context.Background(), Config{
		Root: filepath.Join(root, "pairs"), EvidenceRoot: evidenceRoot, Phase: protocol.PhasePilot,
		Registration: registration, Schedule: schedule, Executors: executors,
	}, block)
	if err != nil {
		t.Fatal(err)
	}
	wantCalls := []string{
		"ready:" + string(block.TopologyOrder[0]), "execute:" + string(block.TopologyOrder[0]),
		"ready:" + string(block.TopologyOrder[1]), "execute:" + string(block.TopologyOrder[1]),
	}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("calls = %v, want %v", calls, wantCalls)
	}
	if execution.Admission != protocol.AdmissionValid || len(execution.Arms) != 2 || !execution.EfficiencyEligible {
		t.Fatalf("execution = %+v", execution)
	}
	loaded, err := LoadPair(filepath.Join(root, "pairs"), PairDirectoryName(block.PairID))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(loaded, execution) {
		t.Fatalf("loaded pair differs: got %+v want %+v", loaded, execution)
	}
	pairDirectory := filepath.Join(root, "pairs", PairDirectoryName(block.PairID))
	if err := os.WriteFile(filepath.Join(pairDirectory, "unsealed-extra.json"), []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPair(filepath.Join(root, "pairs"), pairDirectory); !errors.Is(err, protocol.ErrInvalidEvidence) {
		t.Fatalf("extra pair artifact error = %v", err)
	}
	if _, err := RunPair(context.Background(), Config{
		Root: filepath.Join(root, "pairs"), EvidenceRoot: evidenceRoot, Phase: protocol.PhasePilot,
		Registration: registration, Schedule: schedule, Executors: executors,
	}, block); !errors.Is(err, protocol.ErrEvidenceExists) {
		t.Fatalf("second pair error = %v", err)
	}
}

func TestRunPairRejectsArmMismatchInvalidEvidenceAndForbiddenExclusion(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*fakeExecutor, string)
	}{
		{name: "mismatched arm input", configure: func(executor *fakeExecutor, _ string) {
			executor.mutate = func(bundle *protocol.EvidenceBundle) {
				bundle.EffectiveInput.ActivityOptionsSHA256 = testfixture.Hash('9')
			}
		}},
		{name: "mismatched logical operation", configure: func(executor *fakeExecutor, _ string) {
			executor.mutate = func(bundle *protocol.EvidenceBundle) {
				bundle.Manifest.LogicalOperationID = "different-operation"
				for index := range bundle.CausalEvents {
					bundle.CausalEvents[index].LogicalOperationID = "different-operation"
				}
			}
		}},
		{name: "mismatched tracker identity", configure: func(executor *fakeExecutor, _ string) {
			executor.mutate = func(bundle *protocol.EvidenceBundle) {
				bundle.Manifest.TrackerBeadID = "temporal_projects-other"
				for index := range bundle.CausalEvents {
					bundle.CausalEvents[index].TrackerBeadID = "temporal_projects-other"
				}
			}
		}},
		{name: "missing lineage", configure: func(executor *fakeExecutor, _ string) {
			executor.mutate = func(bundle *protocol.EvidenceBundle) { bundle.Lineage.Edges = bundle.Lineage.Edges[1:] }
		}},
		{name: "replay failure", configure: func(executor *fakeExecutor, _ string) {
			executor.mutate = func(bundle *protocol.EvidenceBundle) {
				bundle.NativeHistory.ReplayCompatible = false
				bundle.NativeHistory.ReplayError = "nondeterminism"
				bundle.Execution.ReplayVerified = false
			}
		}},
		{name: "outside evidence root", configure: func(executor *fakeExecutor, outside string) { executor.writeRoot = outside }},
		{name: "outcome based exclusion", configure: func(executor *fakeExecutor, _ string) { executor.exclusion = "safety_failed" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registration, schedule, block := frozenProtectedBlock(t)
			root := t.TempDir()
			evidenceRoot := filepath.Join(root, "runs")
			executors := map[protocol.Topology]Executor{}
			for _, topology := range protocol.Topologies() {
				executor := &fakeExecutor{topology: topology, evidenceRoot: evidenceRoot}
				if topology == block.TopologyOrder[1] {
					test.configure(executor, t.TempDir())
				}
				executors[topology] = executor
			}
			execution, err := RunPair(context.Background(), Config{
				Root: filepath.Join(root, "pairs"), EvidenceRoot: evidenceRoot, Phase: protocol.PhasePilot,
				Registration: registration, Schedule: schedule, Executors: executors,
			}, block)
			if err != nil {
				t.Fatal(err)
			}
			if execution.Admission != protocol.AdmissionInvalid || execution.EfficiencyEligible || len(execution.ReasonCodes) == 0 || len(execution.Arms) != 2 {
				t.Fatalf("rejected execution = %+v", execution)
			}
		})
	}
}

func TestPairTimingRequiresUTCMonotonicSequence(t *testing.T) {
	valid := []PairTimingEvent{
		{Sequence: 1, Topology: protocol.TopologyDirectActivity, Kind: "arm_ready", TimestampUTC: "2026-08-09T16:00:00Z"},
		{Sequence: 2, Topology: protocol.TopologyDirectActivity, Kind: "arm_finished", TimestampUTC: "2026-08-09T16:00:01Z"},
	}
	if err := validatePairTiming(valid); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func([]PairTimingEvent){
		func(events []PairTimingEvent) { events[0].TimestampUTC = "2026-08-09T12:00:00-04:00" },
		func(events []PairTimingEvent) { events[1].TimestampUTC = "2026-08-09T15:59:59Z" },
		func(events []PairTimingEvent) { events[1].Kind = "invented" },
	} {
		candidate := append([]PairTimingEvent(nil), valid...)
		mutate(candidate)
		if err := validatePairTiming(candidate); !errors.Is(err, protocol.ErrInvalidEvidence) {
			t.Fatalf("timing error = %v", err)
		}
	}
}

func TestRunPairRetainsLogicalFailureWithoutOutcomeBasedExclusion(t *testing.T) {
	registration, schedule, block := frozenProtectedBlock(t)
	root := t.TempDir()
	evidenceRoot := filepath.Join(root, "runs")
	calls := []string{}
	executors := map[protocol.Topology]Executor{}
	for _, topology := range protocol.Topologies() {
		executor := &fakeExecutor{topology: topology, evidenceRoot: evidenceRoot, calls: &calls}
		if topology == block.TopologyOrder[0] {
			executor.mutate = func(bundle *protocol.EvidenceBundle) { bundle.Workload.ActualLogicalOutput = "failed" }
		}
		executors[topology] = executor
	}
	execution, err := RunPair(context.Background(), Config{
		Root: filepath.Join(root, "pairs"), EvidenceRoot: evidenceRoot, Phase: protocol.PhasePilot,
		Registration: registration, Schedule: schedule, Executors: executors,
	}, block)
	if err != nil {
		t.Fatal(err)
	}
	if execution.Admission != protocol.AdmissionValid || execution.EfficiencyEligible || len(execution.Arms) != 2 || len(calls) != 4 {
		t.Fatalf("logical failure was excluded or stopped pair: %+v calls=%v", execution, calls)
	}
	if execution.Arms[0].Verdict.Correctness != protocol.OutcomeFail {
		t.Fatalf("logical failure verdict = %+v", execution.Arms[0].Verdict)
	}
}

func TestRunPairRejectsScheduleDriftBeforeExecution(t *testing.T) {
	registration, schedule, block := frozenProtectedBlock(t)
	drifted := schedule.Clone()
	drifted.Blocks[0].PairID += "-drift"
	executors := map[protocol.Topology]Executor{
		protocol.TopologyDirectActivity: &fakeExecutor{topology: protocol.TopologyDirectActivity, evidenceRoot: t.TempDir()},
		protocol.TopologyChildWorkflow:  &fakeExecutor{topology: protocol.TopologyChildWorkflow, evidenceRoot: t.TempDir()},
	}
	_, err := RunPair(context.Background(), Config{
		Root: t.TempDir(), EvidenceRoot: t.TempDir(), Phase: protocol.PhasePilot,
		Registration: registration, Schedule: drifted, Executors: executors,
	}, block)
	if !errors.Is(err, protocol.ErrInvalidEvidence) {
		t.Fatalf("schedule drift error = %v", err)
	}
}

func TestUnsafeControlMustFailSafetyEvenWhenCorrectnessAlreadyFails(t *testing.T) {
	execution := PairExecution{
		ProtocolVersion: protocol.PublicationProtocolVersion,
		Admission:       protocol.AdmissionValid,
		Block:           protocol.PairBlock{Stratum: protocol.Stratum{Probe: protocol.ProbeUnsafe}},
		Arms: []ArmRun{
			{Topology: protocol.TopologyDirectActivity, Verdict: protocol.Verdict{Admission: protocol.AdmissionValid, Correctness: protocol.OutcomeFail, Safety: protocol.OutcomePass, Liveness: protocol.OutcomePass}},
			{Topology: protocol.TopologyChildWorkflow, Verdict: protocol.Verdict{Admission: protocol.AdmissionValid, Correctness: protocol.OutcomeFail, Safety: protocol.OutcomePass, Liveness: protocol.OutcomePass}},
		},
	}
	finalizePair(&execution, nil)
	if execution.Admission != protocol.AdmissionInvalid ||
		!slices.Contains(execution.ReasonCodes, "direct-activity:unsafe_control_not_distinguishing") ||
		!slices.Contains(execution.ReasonCodes, "child-workflow:unsafe_control_not_distinguishing") {
		t.Fatalf("unsafe correctness failure masked safety control: %+v", execution)
	}
}

type fakeExecutor struct {
	topology     protocol.Topology
	evidenceRoot string
	writeRoot    string
	exclusion    string
	mutate       func(*protocol.EvidenceBundle)
	calls        *[]string
}

func (e *fakeExecutor) Topology() protocol.Topology { return e.topology }

func (e *fakeExecutor) Ready(context.Context) error {
	e.record("ready:" + string(e.topology))
	return nil
}

func (e *fakeExecutor) Execute(_ context.Context, request RunRequest) (RunResult, error) {
	e.record("execute:" + string(e.topology))
	bundle := testfixture.Bundle(request.Block, e.topology)
	if e.mutate != nil {
		e.mutate(&bundle)
	}
	bundle.Verdict = oracle.Evaluate(bundle)
	bundle.Execution.ExclusionReason = e.exclusion
	if e.exclusion != "" {
		bundle.Verdict = oracle.Evaluate(bundle)
	}
	root := e.writeRoot
	if root == "" {
		root = e.evidenceRoot
	}
	directory, err := evidence.WriteRun(root, bundle)
	return RunResult{RunID: bundle.Manifest.RunID, EvidenceDirectory: directory, ExclusionReason: e.exclusion}, err
}

func (e *fakeExecutor) record(value string) {
	if e.calls != nil {
		*e.calls = append(*e.calls, value)
	}
}

func frozenProtectedBlock(t *testing.T) (protocol.Preregistration, protocol.Schedule, protocol.PairBlock) {
	t.Helper()
	registration, err := protocol.LoadPreregistration(filepath.Join("..", "..", "topology-preregistration-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	schedule, err := protocol.BuildSchedule(registration, protocol.PhasePilot)
	if err != nil {
		t.Fatal(err)
	}
	for _, block := range schedule.Blocks {
		if block.Stratum.Probe == protocol.ProbeProtected {
			return registration, schedule, block
		}
	}
	t.Fatal("pilot schedule lacks protected block")
	return protocol.Preregistration{}, protocol.Schedule{}, protocol.PairBlock{}
}
