package semantics

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/oracle"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
	historypb "go.temporal.io/api/history/v1"
	temporallog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestTemporalExecutorRecoversJoinAcrossBothTopologyArms(t *testing.T) {
	if testing.Short() {
		t.Skip("real Temporal service and hermetic processes are integration coverage")
	}
	temporalPath := temporalBinary(t)
	root := t.TempDir()
	agentBinary := filepath.Join(root, "agent-simulator")
	build := exec.Command("go", "build", "-o", agentBinary, "./cmd/agent-simulator")
	build.Dir = semanticsRepositoryRoot(t)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build hermetic agent: %v\n%s", err, output)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	executor, err := OpenTemporalExecutor(ctx, ExecutorConfig{
		TemporalPath: temporalPath, WorkRoot: filepath.Join(root, "work"), AgentBinary: agentBinary,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := executor.Close(); err != nil {
			t.Errorf("close Temporal executor: %v", err)
		}
	})
	results := make(map[protocol.Topology]EpisodeResult)
	for _, topology := range protocol.Topologies() {
		result, runErr := executor.Run(ctx, RunRequest{
			PairID: "integration/join/protected/pair-01", ScheduleBlockID: "schedule-block/integration-join-01",
			Topology: topology, Case: protocol.CaseJoinBarrier,
			Boundary: "designated-item-result-observed-before-activity-completion", Probe: protocol.ProbeProtected, Fanout: 8,
		})
		if runErr != nil {
			t.Fatalf("%s run: %v", topology, runErr)
		}
		if result.Bundle.Verdict.Admission != protocol.AdmissionValid || !result.Bundle.Verdict.EfficiencyEligible ||
			!result.NativeHistory.ReplayCompatible {
			t.Fatalf("%s result = %+v / history=%+v", topology, result.Bundle.Verdict, result.NativeHistory)
		}
		results[topology] = result
	}
	direct := results[protocol.TopologyDirectActivity]
	child := results[protocol.TopologyChildWorkflow]
	if !direct.Bundle.EffectiveInput.MatchedWith(child.Bundle.EffectiveInput) {
		t.Fatalf("paired effective inputs differ:\ndirect=%+v\nchild=%+v", direct.Bundle.EffectiveInput, child.Bundle.EffectiveInput)
	}
	var export struct {
		Children []json.RawMessage `json:"children"`
	}
	if err := json.Unmarshal(child.NativeHistory.Export, &export); err != nil || len(export.Children) != 8 {
		t.Fatalf("captured child histories = %d, error=%v", len(export.Children), err)
	}
}

func TestPreservedSemanticsEvidenceAuditsFromDisk(t *testing.T) {
	root := os.Getenv("TOPOLOGY_SEMANTICS_EVIDENCE_ROOT")
	if root == "" {
		t.Skip("set TOPOLOGY_SEMANTICS_EVIDENCE_ROOT to audit an append-only conformance root")
	}
	auditTopologyEvidenceRoot(t, root)
}

func TestPreservedRecoveryEvidenceAuditsFromDisk(t *testing.T) {
	root := os.Getenv("TOPOLOGY_RECOVERY_EVIDENCE_ROOT")
	if root == "" {
		t.Skip("set TOPOLOGY_RECOVERY_EVIDENCE_ROOT to audit an append-only conformance root")
	}
	auditTopologyEvidenceRoot(t, root)
}

func auditTopologyEvidenceRoot(t *testing.T, root string) {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	verified := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			t.Fatalf("unexpected evidence-root entry %s", entry.Name())
		}
		_, verdict, err := oracle.VerifyRun(root, filepath.Join(root, entry.Name()))
		if err != nil {
			t.Fatalf("verify %s: %v", entry.Name(), err)
		}
		if verdict.Admission != protocol.AdmissionValid {
			t.Fatalf("invalid stored verdict for %s: %+v", entry.Name(), verdict)
		}
		if err := replayStoredNativeHistory(filepath.Join(root, entry.Name(), protocol.NativeHistoryFile)); err != nil {
			t.Fatalf("replay %s: %v", entry.Name(), err)
		}
		verified++
	}
	if verified == 0 {
		t.Fatal("evidence root contained no run directories")
	}
}

func replayStoredNativeHistory(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var native struct {
		Export json.RawMessage `json:"export"`
	}
	if err := json.Unmarshal(data, &native); err != nil {
		return err
	}
	type capturedHistory struct {
		History json.RawMessage `json:"history"`
	}
	var export struct {
		Parent   capturedHistory   `json:"parent"`
		Children []capturedHistory `json:"children"`
	}
	if err := json.Unmarshal(native.Export, &export); err != nil {
		return err
	}
	logger := temporallog.NewStructuredLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
	parent := &historypb.History{}
	if err := protojson.Unmarshal(export.Parent.History, parent); err != nil {
		return err
	}
	parentReplayer := worker.NewWorkflowReplayer()
	parentReplayer.RegisterWorkflowWithOptions(ParentWorkflow, workflow.RegisterOptions{Name: ParentWorkflowName})
	if err := parentReplayer.ReplayWorkflowHistory(logger, parent); err != nil {
		return err
	}
	childReplayer := worker.NewWorkflowReplayer()
	childReplayer.RegisterWorkflowWithOptions(ItemWorkflow, workflow.RegisterOptions{Name: ItemWorkflowName})
	childReplayer.RegisterWorkflowWithOptions(RecoveryItemWorkflow, workflow.RegisterOptions{Name: RecoveryItemWorkflowName})
	for _, captured := range export.Children {
		history := &historypb.History{}
		if err := protojson.Unmarshal(captured.History, history); err != nil {
			return err
		}
		if err := childReplayer.ReplayWorkflowHistory(logger, history); err != nil {
			return err
		}
	}
	return nil
}

func TestTemporalExecutorCoversFrozenSemanticsCasesAndBoundaries(t *testing.T) {
	if testing.Short() {
		t.Skip("real Temporal service and hermetic processes are integration coverage")
	}
	temporalPath := temporalBinary(t)
	root := t.TempDir()
	agentBinary := filepath.Join(root, "agent-simulator")
	build := exec.Command("go", "build", "-o", agentBinary, "./cmd/agent-simulator")
	build.Dir = semanticsRepositoryRoot(t)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build hermetic agent: %v\n%s", err, output)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	executor, err := OpenTemporalExecutor(ctx, ExecutorConfig{
		TemporalPath: temporalPath, WorkRoot: filepath.Join(root, "work"), AgentBinary: agentBinary,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := executor.Close(); err != nil {
			t.Errorf("close Temporal executor: %v", err)
		}
	})
	for _, test := range FrozenSemanticsScenarios() {
		name := string(test.Case) + "/" + test.Boundary + "/" + string(test.Probe)
		t.Run(name, func(t *testing.T) {
			pairID := "integration/semantics/" + shortDigest(name)
			results := make(map[protocol.Topology]EpisodeResult)
			for _, topology := range protocol.Topologies() {
				result, runErr := executor.Run(ctx, RunRequest{
					PairID: pairID, ScheduleBlockID: "schedule-block/" + shortDigest(name), Topology: topology,
					Case: test.Case, Boundary: test.Boundary, Probe: test.Probe, Fanout: 8,
				})
				if runErr != nil {
					t.Fatalf("%s run: %v", topology, runErr)
				}
				if result.Bundle.Verdict.Admission != protocol.AdmissionValid || !result.NativeHistory.ReplayCompatible {
					t.Fatalf("%s admission/history = %+v / events=%d hash=%s replay=%t\nrecovery=%+v\nevents=%+v", topology,
						result.Bundle.Verdict, result.NativeHistory.EventCount, result.NativeHistory.HistorySHA256,
						result.NativeHistory.ReplayCompatible, result.Bundle.Workload.Recovery, result.Bundle.CausalEvents)
				}
				if test.Probe == protocol.ProbeUnsafe {
					if result.Bundle.Verdict.Safety != protocol.OutcomeFail || result.Bundle.Verdict.EfficiencyEligible {
						t.Fatalf("%s unsafe control did not distinguish: %+v", topology, result.Bundle.Verdict)
					}
				} else if !result.Bundle.Verdict.EfficiencyEligible {
					t.Fatalf("%s protected/unfaulted result = %+v\nworkload=%+v", topology,
						result.Bundle.Verdict, result.Bundle.Workload)
				}
				results[topology] = result
			}
			if !results[protocol.TopologyDirectActivity].Bundle.EffectiveInput.MatchedWith(
				results[protocol.TopologyChildWorkflow].Bundle.EffectiveInput,
			) {
				t.Fatal("paired effective inputs differ")
			}
		})
	}
}

func TestTemporalExecutorCoversFrozenRecoveryCasesAndBoundaries(t *testing.T) {
	if testing.Short() {
		t.Skip("real Temporal service, dependency service, Worker restart, and hermetic processes are integration coverage")
	}
	temporalPath := temporalBinary(t)
	root := t.TempDir()
	agentBinary := filepath.Join(root, "agent-simulator")
	build := exec.Command("go", "build", "-o", agentBinary, "./cmd/agent-simulator")
	build.Dir = semanticsRepositoryRoot(t)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build hermetic agent: %v\n%s", err, output)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	executor, err := OpenTemporalExecutor(ctx, ExecutorConfig{
		TemporalPath: temporalPath, WorkRoot: filepath.Join(root, "work"), AgentBinary: agentBinary,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := executor.Close(); err != nil {
			t.Errorf("close Temporal executor: %v", err)
		}
	})
	for _, scenario := range FrozenRecoveryScenarios() {
		name := string(scenario.Case) + "/" + scenario.Boundary + "/" + string(scenario.Probe)
		t.Run(name, func(t *testing.T) {
			pairID := "integration/recovery/" + shortDigest(name)
			fanout := 8
			if scenario.Case == protocol.CaseBackpressureOverload && scenario.Probe == protocol.ProbeUnsafe {
				fanout = 32
			}
			results := make(map[protocol.Topology]EpisodeResult)
			for _, topology := range protocol.Topologies() {
				result, runErr := executor.Run(ctx, RunRequest{
					PairID: pairID, ScheduleBlockID: "schedule-block/" + shortDigest(name), Topology: topology,
					Case: scenario.Case, Boundary: scenario.Boundary, Probe: scenario.Probe, Fanout: fanout,
				})
				if runErr != nil {
					t.Fatalf("%s run: %v", topology, runErr)
				}
				if result.Bundle.Verdict.Admission != protocol.AdmissionValid || !result.NativeHistory.ReplayCompatible {
					t.Fatalf("%s admission/history = %+v / events=%d hash=%s replay=%t\nrecovery=%+v\nevents=%+v",
						topology, result.Bundle.Verdict, result.NativeHistory.EventCount, result.NativeHistory.HistorySHA256,
						result.NativeHistory.ReplayCompatible, result.Bundle.Workload.Recovery, result.Bundle.CausalEvents)
				}
				if scenario.Probe == protocol.ProbeUnsafe {
					if result.Bundle.Verdict.Safety != protocol.OutcomeFail || result.Bundle.Verdict.EfficiencyEligible {
						t.Fatalf("%s unsafe recovery control did not distinguish: %+v", topology, result.Bundle.Verdict)
					}
				} else if !result.Bundle.Verdict.EfficiencyEligible {
					t.Fatalf("%s protected/unfaulted recovery result = %+v\nworkload=%+v\nrecovery=%+v", topology,
						result.Bundle.Verdict, result.Bundle.Workload, result.Bundle.Workload.Recovery)
				}
				if scenario.Case == protocol.CaseSilentProgress && result.Bundle.Workload.Recovery != nil {
					t.Logf("%s silent-progress recovery metrics: %+v", topology, result.Bundle.Workload.Recovery.Metrics)
				}
				results[topology] = result
			}
			if !results[protocol.TopologyDirectActivity].Bundle.EffectiveInput.MatchedWith(
				results[protocol.TopologyChildWorkflow].Bundle.EffectiveInput,
			) {
				t.Fatal("paired recovery effective inputs differ")
			}
		})
	}
}

func TestTemporalExecutorRecoveryScaleDoesNotDeadlockAdmission(t *testing.T) {
	if testing.Short() {
		t.Skip("canonical recovery admission is real service/process integration coverage")
	}
	temporalPath := temporalBinary(t)
	root := t.TempDir()
	agentBinary := filepath.Join(root, "agent-simulator")
	build := exec.Command("go", "build", "-o", agentBinary, "./cmd/agent-simulator")
	build.Dir = semanticsRepositoryRoot(t)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build hermetic agent: %v\n%s", err, output)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	executor, err := OpenTemporalExecutor(ctx, ExecutorConfig{
		TemporalPath: temporalPath, WorkRoot: filepath.Join(root, "work"), AgentBinary: agentBinary,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := executor.Close(); err != nil {
			t.Errorf("close Temporal executor: %v", err)
		}
	})
	for _, scenario := range []Scenario{
		{Case: protocol.CaseLayeredRetryAmplification, Boundary: protocol.UnfaultedBoundary, Probe: protocol.ProbeUnfaulted},
		{Case: protocol.CaseOutageBacklogHerdRecovery, Boundary: "outage-backlog-restoration-and-catchup-worker-crash", Probe: protocol.ProbeProtected},
		{Case: protocol.CasePoisonWorkIsolation, Boundary: "mixed-cohort-admitted-before-poison-failure-release", Probe: protocol.ProbeProtected},
	} {
		t.Run(string(scenario.Case), func(t *testing.T) {
			name := scenarioKey(scenario)
			result, runErr := executor.Run(ctx, RunRequest{
				PairID:          "integration/recovery-scale/" + shortDigest(name),
				ScheduleBlockID: "schedule-block/recovery-scale/" + shortDigest(name),
				Topology:        protocol.TopologyDirectActivity, Case: scenario.Case,
				Boundary: scenario.Boundary, Probe: scenario.Probe, Fanout: 32,
			})
			if runErr != nil {
				t.Fatal(runErr)
			}
			if !result.Bundle.Verdict.EfficiencyEligible {
				t.Fatalf("canonical protected recovery did not pass: %+v\nworkload=%+v\ndependency=%+v",
					result.Bundle.Verdict, result.Bundle.Workload, result.Bundle.Dependency)
			}
		})
	}
	t.Run("child-workflow-unsafe-outage-after-warmup", func(t *testing.T) {
		scenario := Scenario{
			Case:     protocol.CaseOutageBacklogHerdRecovery,
			Boundary: "outage-backlog-restoration-and-catchup-worker-crash",
			Probe:    protocol.ProbeUnsafe,
		}
		name := scenarioKey(scenario) + "/child-workflow"
		result, runErr := executor.Run(ctx, RunRequest{
			PairID:          "integration/recovery-scale/" + shortDigest(name),
			ScheduleBlockID: "schedule-block/recovery-scale/" + shortDigest(name),
			Topology:        protocol.TopologyChildWorkflow,
			Case:            scenario.Case,
			Boundary:        scenario.Boundary,
			Probe:           scenario.Probe,
			Fanout:          32,
		})
		if runErr != nil {
			t.Fatal(runErr)
		}
		if result.Bundle.Verdict.Admission != protocol.AdmissionValid ||
			result.Bundle.Verdict.Safety != protocol.OutcomeFail ||
			result.Bundle.Verdict.Liveness != protocol.OutcomePass {
			t.Fatalf("canonical unsafe Child-Workflow outage did not distinguish and recover: %+v", result.Bundle.Verdict)
		}
	})
}

func TestTemporalExecutorPoisonChildScalePreservesBoundaryAcrossRetries(t *testing.T) {
	if testing.Short() {
		t.Skip("fanout-128 child-Workflow poison recovery is real service/process integration coverage")
	}
	if os.Getenv("TOPOLOGY_POISON_SCALE_REGRESSION") != "1" {
		t.Skip("set TOPOLOGY_POISON_SCALE_REGRESSION=1 to run the repeated fanout-128 poison profile")
	}
	temporalPath := temporalBinary(t)
	root := t.TempDir()
	agentBinary := filepath.Join(root, "agent-simulator")
	build := exec.Command("go", "build", "-o", agentBinary, "./cmd/agent-simulator")
	build.Dir = semanticsRepositoryRoot(t)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build hermetic agent: %v\n%s", err, output)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	executor, err := OpenTemporalExecutor(ctx, ExecutorConfig{
		TemporalPath: temporalPath, WorkRoot: filepath.Join(root, "work"), AgentBinary: agentBinary,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := executor.Close(); err != nil {
			t.Errorf("close Temporal executor: %v", err)
		}
	})
	for trial := 1; trial <= 3; trial++ {
		t.Run(fmt.Sprintf("trial-%02d", trial), func(t *testing.T) {
			pairID := fmt.Sprintf("integration/poison-child-scale/trial-%02d", trial)
			result, runErr := executor.Run(ctx, RunRequest{
				PairID: pairID, ScheduleBlockID: "schedule-block/" + pairID,
				Topology: protocol.TopologyChildWorkflow, Case: protocol.CasePoisonWorkIsolation,
				Boundary: "mixed-cohort-admitted-before-poison-failure-release",
				Probe:    protocol.ProbeProtected, Fanout: 128,
			})
			if runErr != nil {
				t.Fatal(runErr)
			}
			if !result.Bundle.FaultBoundary.Injected || result.Bundle.Verdict.Admission != protocol.AdmissionValid ||
				result.Bundle.Verdict.Correctness != protocol.OutcomePass || result.Bundle.Verdict.Safety != protocol.OutcomePass ||
				result.Bundle.Verdict.Liveness != protocol.OutcomePass || !result.Bundle.NativeHistory.ReplayCompatible {
				t.Fatalf("protected child-Workflow poison scale did not preserve the boundary: fault=%+v verdict=%+v history=%+v",
					result.Bundle.FaultBoundary, result.Bundle.Verdict, result.Bundle.NativeHistory)
			}
		})
	}
}

func TestTemporalExecutorUnsafeQueuedChildScaleClosesHeldBarriers(t *testing.T) {
	if testing.Short() {
		t.Skip("fanout-32 unsafe queued child-Workflow teardown is real service/process integration coverage")
	}
	temporalPath := temporalBinary(t)
	root := t.TempDir()
	agentBinary := filepath.Join(root, "agent-simulator")
	build := exec.Command("go", "build", "-o", agentBinary, "./cmd/agent-simulator")
	build.Dir = semanticsRepositoryRoot(t)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build hermetic agent: %v\n%s", err, output)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	executor, err := OpenTemporalExecutor(ctx, ExecutorConfig{
		TemporalPath: temporalPath, WorkRoot: filepath.Join(root, "work"), AgentBinary: agentBinary,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := executor.Close(); err != nil {
			t.Errorf("close Temporal executor: %v", err)
		}
	})
	for trial := 1; trial <= 3; trial++ {
		t.Run(fmt.Sprintf("trial-%02d", trial), func(t *testing.T) {
			pairID := fmt.Sprintf("integration/unsafe-queued-child-scale/trial-%02d", trial)
			result, runErr := executor.Run(ctx, RunRequest{
				PairID: pairID, ScheduleBlockID: "schedule-block/" + pairID,
				Topology: protocol.TopologyChildWorkflow, Case: protocol.CaseQueuedExecutingSupersession,
				Boundary: "queued-before-activity-start", Probe: protocol.ProbeUnsafe, Fanout: 32,
			})
			if runErr != nil {
				t.Fatal(runErr)
			}
			if result.Bundle.Verdict.Admission != protocol.AdmissionValid ||
				result.Bundle.Verdict.Safety != protocol.OutcomeFail || result.Bundle.Verdict.Liveness != protocol.OutcomePass ||
				!result.Bundle.NativeHistory.ReplayCompatible {
				t.Fatalf("unsafe queued child-Workflow scale did not distinguish and tear down cleanly: verdict=%+v history=%+v",
					result.Bundle.Verdict, result.Bundle.NativeHistory)
			}
		})
	}
}

func TestTemporalExecutorProtectedSupersessionChildScaleKeepsHeldArrivalLive(t *testing.T) {
	if testing.Short() {
		t.Skip("fanout-128 protected child-Workflow supersession is real service/process integration coverage")
	}
	if os.Getenv("TOPOLOGY_SUPERSESSION_SCALE_REGRESSION") != "1" {
		t.Skip("set TOPOLOGY_SUPERSESSION_SCALE_REGRESSION=1 to run the repeated fanout-128 supersession profile")
	}
	temporalPath := temporalBinary(t)
	root := t.TempDir()
	agentBinary := filepath.Join(root, "agent-simulator")
	build := exec.Command("go", "build", "-o", agentBinary, "./cmd/agent-simulator")
	build.Dir = semanticsRepositoryRoot(t)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build hermetic agent: %v\n%s", err, output)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	executor, err := OpenTemporalExecutor(ctx, ExecutorConfig{
		TemporalPath: temporalPath, WorkRoot: filepath.Join(root, "work"), AgentBinary: agentBinary,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := executor.Close(); err != nil {
			t.Errorf("close Temporal executor: %v", err)
		}
	})
	for trial := 1; trial <= 3; trial++ {
		t.Run(fmt.Sprintf("trial-%02d", trial), func(t *testing.T) {
			pairID := fmt.Sprintf("integration/protected-supersession-child-scale/trial-%02d", trial)
			result, runErr := executor.Run(ctx, RunRequest{
				PairID: pairID, ScheduleBlockID: "schedule-block/" + pairID,
				Topology: protocol.TopologyChildWorkflow, Case: protocol.CaseQueuedExecutingSupersession,
				Boundary: "executing-after-process-start-before-effect", Probe: protocol.ProbeProtected, Fanout: 128,
			})
			if runErr != nil {
				t.Fatal(runErr)
			}
			if !result.Bundle.FaultBoundary.Injected || result.Bundle.Verdict.Admission != protocol.AdmissionValid ||
				result.Bundle.Verdict.Correctness != protocol.OutcomePass || result.Bundle.Verdict.Safety != protocol.OutcomePass ||
				result.Bundle.Verdict.Liveness != protocol.OutcomePass || !result.Bundle.NativeHistory.ReplayCompatible {
				t.Fatalf("protected child-Workflow supersession scale did not recover: fault=%+v verdict=%+v history=%+v",
					result.Bundle.FaultBoundary, result.Bundle.Verdict, result.Bundle.NativeHistory)
			}
		})
	}
}

func TestTemporalExecutorProtectedSilentProgressDirectScaleUsesControlLane(t *testing.T) {
	if testing.Short() {
		t.Skip("fanout-128 protected direct-Activity silent-progress recovery is real service/process integration coverage")
	}
	if os.Getenv("TOPOLOGY_SILENT_PROGRESS_SCALE_REGRESSION") != "1" {
		t.Skip("set TOPOLOGY_SILENT_PROGRESS_SCALE_REGRESSION=1 to run the repeated fanout-128 silent-progress profile")
	}
	temporalPath := temporalBinary(t)
	root := t.TempDir()
	agentBinary := filepath.Join(root, "agent-simulator")
	build := exec.Command("go", "build", "-o", agentBinary, "./cmd/agent-simulator")
	build.Dir = semanticsRepositoryRoot(t)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build hermetic agent: %v\n%s", err, output)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	executor, err := OpenTemporalExecutor(ctx, ExecutorConfig{
		TemporalPath: temporalPath, WorkRoot: filepath.Join(root, "work"), AgentBinary: agentBinary,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := executor.Close(); err != nil {
			t.Errorf("close Temporal executor: %v", err)
		}
	})
	for trial := 1; trial <= 3; trial++ {
		t.Run(fmt.Sprintf("trial-%02d", trial), func(t *testing.T) {
			pairID := fmt.Sprintf("integration/protected-silent-progress-direct-scale/trial-%02d", trial)
			result, runErr := executor.Run(ctx, RunRequest{
				PairID: pairID, ScheduleBlockID: "schedule-block/" + pairID,
				Topology: protocol.TopologyDirectActivity, Case: protocol.CaseSilentProgress,
				Boundary: "accepted-progress-before-executor-wedge", Probe: protocol.ProbeProtected, Fanout: 128,
			})
			if runErr != nil {
				t.Fatal(runErr)
			}
			if result.Bundle.Workload.Recovery == nil {
				t.Fatal("protected silent-progress result lacks recovery observations")
			}
			detectionMS := metricByName(result.Bundle.Workload.Recovery.Metrics, "failure_detection_latency_ms")
			t.Logf("failure detection latency: %dms", detectionMS)
			if detectionMS > progressDeadlineMS {
				t.Fatalf("failure detection latency = %dms, want <= %dms", detectionMS, progressDeadlineMS)
			}
			if !result.Bundle.FaultBoundary.Injected || result.Bundle.Verdict.Admission != protocol.AdmissionValid ||
				result.Bundle.Verdict.Correctness != protocol.OutcomePass || result.Bundle.Verdict.Safety != protocol.OutcomePass ||
				result.Bundle.Verdict.Liveness != protocol.OutcomePass || !result.Bundle.NativeHistory.ReplayCompatible {
				t.Fatalf("protected direct-Activity silent-progress scale missed the recovery bound: fault=%+v verdict=%+v replay_compatible=%t",
					result.Bundle.FaultBoundary, result.Bundle.Verdict, result.Bundle.NativeHistory.ReplayCompatible)
			}
		})
	}
}

func TestTemporalExecutorPilotV5FailureShapesRecoverRepeatedly(t *testing.T) {
	if testing.Short() {
		t.Skip("pilot-v5 corrections require the real Temporal/service/process path")
	}
	if os.Getenv("TOPOLOGY_PILOT_V5_REGRESSION") != "1" {
		t.Skip("set TOPOLOGY_PILOT_V5_REGRESSION=1 to run the repeated pilot-v5 correction profile")
	}
	temporalPath := temporalBinary(t)
	root := t.TempDir()
	agentBinary := filepath.Join(root, "agent-simulator")
	build := exec.Command("go", "build", "-o", agentBinary, "./cmd/agent-simulator")
	build.Dir = semanticsRepositoryRoot(t)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build hermetic agent: %v\n%s", err, output)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	executor, err := OpenTemporalExecutor(ctx, ExecutorConfig{
		TemporalPath: temporalPath, WorkRoot: filepath.Join(root, "work"), AgentBinary: agentBinary,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := executor.Close(); err != nil {
			t.Errorf("close Temporal executor: %v", err)
		}
	})
	tests := []struct {
		name             string
		topology         protocol.Topology
		benchmarkCase    protocol.CaseID
		boundary         string
		probe            protocol.Probe
		fanout           int
		wantFault        bool
		checkDetectionMS bool
	}{
		{
			name: "unfaulted-direct-supersession-32", topology: protocol.TopologyDirectActivity,
			benchmarkCase: protocol.CaseQueuedExecutingSupersession, boundary: protocol.UnfaultedBoundary,
			probe: protocol.ProbeUnfaulted, fanout: 32,
		},
		{
			name: "unfaulted-child-outage-128", topology: protocol.TopologyChildWorkflow,
			benchmarkCase: protocol.CaseOutageBacklogHerdRecovery, boundary: protocol.UnfaultedBoundary,
			probe: protocol.ProbeUnfaulted, fanout: 128,
		},
		{
			name: "protected-child-outage-128", topology: protocol.TopologyChildWorkflow,
			benchmarkCase: protocol.CaseOutageBacklogHerdRecovery,
			boundary:      "outage-backlog-restoration-and-catchup-worker-crash",
			probe:         protocol.ProbeProtected, fanout: 128, wantFault: true,
		},
		{
			name: "protected-child-silent-progress-8", topology: protocol.TopologyChildWorkflow,
			benchmarkCase: protocol.CaseSilentProgress, boundary: "accepted-progress-before-executor-wedge",
			probe: protocol.ProbeProtected, fanout: 8, wantFault: true, checkDetectionMS: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for trial := 1; trial <= 3; trial++ {
				pairID := fmt.Sprintf("integration/pilot-v5-correction/%s/trial-%02d", test.name, trial)
				result, runErr := executor.Run(ctx, RunRequest{
					PairID: pairID, ScheduleBlockID: "schedule-block/" + pairID,
					Topology: test.topology, Case: test.benchmarkCase, Boundary: test.boundary,
					Probe: test.probe, Fanout: test.fanout,
				})
				if runErr != nil {
					t.Fatalf("trial %d: %v", trial, runErr)
				}
				detectionMS := int64(-1)
				if test.checkDetectionMS {
					if result.Bundle.Workload.Recovery == nil {
						t.Fatalf("trial %d lacks recovery observations", trial)
					}
					detectionMS = metricByName(result.Bundle.Workload.Recovery.Metrics, "failure_detection_latency_ms")
					t.Logf("trial %d failure detection latency: %dms", trial, detectionMS)
				}
				if result.Bundle.FaultBoundary.Injected != test.wantFault ||
					result.Bundle.Verdict.Admission != protocol.AdmissionValid ||
					result.Bundle.Verdict.Correctness != protocol.OutcomePass ||
					result.Bundle.Verdict.Safety != protocol.OutcomePass ||
					result.Bundle.Verdict.Liveness != protocol.OutcomePass ||
					!result.Bundle.NativeHistory.ReplayCompatible {
					t.Fatalf("trial %d failed: fault=%+v verdict=%+v detection_ms=%d history_events=%d history_sha256=%s replay_compatible=%t replay_error=%q",
						trial, result.Bundle.FaultBoundary, result.Bundle.Verdict, detectionMS,
						result.Bundle.NativeHistory.EventCount, result.Bundle.NativeHistory.HistorySHA256,
						result.Bundle.NativeHistory.ReplayCompatible, result.Bundle.NativeHistory.ReplayError)
				}
				if test.checkDetectionMS {
					if detectionMS > progressDeadlineMS {
						t.Fatalf("trial %d detection latency = %dms, want <= %dms", trial, detectionMS, progressDeadlineMS)
					}
				}
			}
		})
	}
}

func temporalBinary(t *testing.T) string {
	t.Helper()
	if configured := os.Getenv("TEMPORAL_CLI_PATH"); configured != "" {
		return configured
	}
	if path, err := exec.LookPath("temporal"); err == nil {
		return path
	}
	path := "/home/ds/go/bin/temporal"
	if _, err := os.Stat(path); err != nil {
		t.Skip("Temporal CLI is not available")
	}
	return path
}

func semanticsRepositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate repository root")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "../../../.."))
}
