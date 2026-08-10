package evidence

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
)

func TestWriteAndLoadRunUsesAppendOnlyCompleteSealedInventory(t *testing.T) {
	root := t.TempDir()
	bundle := validBundle()
	directory, err := WriteRun(root, bundle)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != len(protocol.RequiredEvidenceFiles()) {
		t.Fatalf("evidence files = %d, want %d", len(entries), len(protocol.RequiredEvidenceFiles()))
	}
	loaded, err := LoadRun(root, directory)
	if err != nil {
		t.Fatal(err)
	}
	// RawMessage preserves the writer's indentation. Its canonical digest is
	// validated independently, so normalize only that representation here.
	bundle.NativeHistory.Export = loaded.NativeHistory.Export
	if !reflect.DeepEqual(loaded, bundle) {
		t.Fatalf("loaded bundle differs:\n got: %#v\nwant: %#v", loaded, bundle)
	}
	if _, err := WriteRun(root, bundle); !errors.Is(err, protocol.ErrEvidenceExists) {
		t.Fatalf("second write error = %v", err)
	}
}

func TestLoadRunRejectsHashMismatchMissingFileSymlinkAndExtraFile(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, string)
	}{
		{name: "hash mismatch", mutate: func(t *testing.T, directory string) {
			t.Helper()
			path := filepath.Join(directory, protocol.WorkloadStateFile)
			if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "missing", mutate: func(t *testing.T, directory string) {
			t.Helper()
			if err := os.Remove(filepath.Join(directory, protocol.DependencyStateFile)); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "symlink", mutate: func(t *testing.T, directory string) {
			t.Helper()
			path := filepath.Join(directory, protocol.DependencyStateFile)
			target := filepath.Join(directory, protocol.AuthorityStateFile)
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, path); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "unexpected file", mutate: func(t *testing.T, directory string) {
			t.Helper()
			if err := os.WriteFile(filepath.Join(directory, "unsealed-extra.json"), []byte("{}\n"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			directory, err := WriteRun(root, validBundle())
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(t, directory)
			if _, err := LoadRun(root, directory); !errors.Is(err, protocol.ErrInvalidEvidence) {
				t.Fatalf("LoadRun() error = %v", err)
			}
		})
	}
}

func TestLoadRunRejectsEvidenceOutsideCallerRoot(t *testing.T) {
	outside := t.TempDir()
	directory, err := WriteRun(outside, validBundle())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRun(t.TempDir(), directory); !errors.Is(err, protocol.ErrInvalidEvidence) {
		t.Fatalf("outside-root error = %v", err)
	}
}

func validBundle() protocol.EvidenceBundle {
	identity := protocol.Identity{
		ProtocolVersion: protocol.PublicationProtocolVersion,
		RunID:           "run-1", PairID: "pair-1", ScheduleBlockID: "schedule-block/pair-1", TrackerBeadID: "temporal_projects-4ic.1",
		Topology: protocol.TopologyDirectActivity, Case: protocol.CaseJoinBarrier,
		Boundary: "designated-item-result-observed-before-activity-completion", Probe: protocol.ProbeProtected, Fanout: 8,
		LogicalOperationID: "operation-1", WorkItemID: "item-001", Generation: 1, CapabilityHash: sha('a'),
		ParentWorkflowID: "parent-workflow", ParentRunID: "parent-run", ActivityID: "activity-item-001", ActivityAttempt: 1,
		WorkerID: "worker-1", WorkerPID: 101, ProcessIdentity: "pid:101/start:first",
	}
	kinds := []string{
		protocol.EventInputRegistered,
		protocol.EventBarrierReached,
		protocol.EventFaultCommitted,
		protocol.EventRecoveryObserved,
		protocol.EventResultAccepted,
		protocol.EventOutcomeAccepted,
		protocol.EventAcknowledged,
	}
	events := make([]protocol.CausalEvent, len(kinds))
	for index, kind := range kinds {
		parents := []string(nil)
		if index > 0 {
			parents = []string{"event-" + string(rune('0'+index))}
		}
		events[index] = protocol.CausalEvent{
			Identity: identity, Sequence: uint64(index + 1), EventID: "event-" + string(rune('1'+index)), ParentEventIDs: parents,
			TimestampUTC:      "2026-08-09T16:00:00.00000000" + string(rune('1'+index)) + "Z",
			MonotonicOffsetNS: int64(index) * 1_000_000, Kind: kind, Decision: protocol.DecisionObserved,
		}
	}
	events[1].Decision = protocol.DecisionBlocked
	events[2].Decision = protocol.DecisionAccepted
	events[4].Decision = protocol.DecisionAccepted
	events[5].Decision = protocol.DecisionAccepted
	events[6].Decision = protocol.DecisionAccepted
	edges := make([]protocol.LineageEdge, 0, len(events)-1)
	for index := 1; index < len(events); index++ {
		edges = append(edges, protocol.LineageEdge{ParentEventID: events[index-1].EventID, ChildEventID: events[index].EventID})
	}
	manifest := protocol.Manifest{
		ProtocolVersion: protocol.PublicationProtocolVersion, RunID: identity.RunID, PairID: identity.PairID,
		ScheduleBlockID: identity.ScheduleBlockID, TrackerBeadID: identity.TrackerBeadID, Topology: identity.Topology,
		Case: identity.Case, Boundary: identity.Boundary, Probe: identity.Probe, Fanout: identity.Fanout,
		LogicalOperationID: identity.LogicalOperationID, CreatedAtUTC: "2026-08-09T16:00:00Z",
		RequiredEvidence: protocol.RequiredEvidenceFiles(),
	}
	nativeExport := json.RawMessage(`{"workflow_id":"parent-1","events":7}`)
	nativeHash, err := protocol.NativeExportSHA256(nativeExport)
	if err != nil {
		panic(err)
	}
	return protocol.EvidenceBundle{
		Manifest:            manifest,
		CausalEvents:        events,
		Lineage:             protocol.Lineage{RunID: identity.RunID, Edges: edges},
		Authority:           protocol.AuthorityState{RunID: identity.RunID, CurrentGeneration: 1, CurrentCapabilityHash: sha('a'), Epochs: []protocol.AuthorityEpoch{{Generation: 1, CapabilityHash: sha('a'), State: protocol.AuthorityActive}}},
		Destination:         protocol.DestinationState{RunID: identity.RunID},
		Dependency:          protocol.DependencyState{RunID: identity.RunID},
		Workload:            protocol.WorkloadState{RunID: identity.RunID, RequiredItemIDs: []string{"item-001"}, AcceptedResultItemIDs: []string{"item-001"}, ExpectedLogicalOutput: "ok", ActualLogicalOutput: "ok"},
		FaultBoundary:       protocol.FaultBoundary{RunID: identity.RunID, Injected: true, ExpectedBoundary: identity.Boundary, BarrierEventID: "event-2", FaultEventID: "event-3", TargetProcessIdentity: identity.ProcessIdentity},
		NativeHistory:       protocol.NativeHistory{RunID: identity.RunID, Captured: true, EventCount: 7, Export: nativeExport, HistorySHA256: nativeHash, ReplayCompatible: true, ReplayWorkerSHA256: sha('c')},
		ProcessObservations: protocol.ProcessObservations{RunID: identity.RunID, Observations: []protocol.ProcessObservation{{EventID: "event-2", WorkItemID: identity.WorkItemID, WorkerID: identity.WorkerID, WorkerPID: identity.WorkerPID, ProcessIdentity: identity.ProcessIdentity, State: "blocked-at-barrier"}}},
		EffectiveInput: protocol.EffectiveInput{RunID: identity.RunID, PairID: identity.PairID, ScheduleBlockID: identity.ScheduleBlockID, Topology: identity.Topology, Case: identity.Case, Boundary: identity.Boundary, Probe: identity.Probe, Fanout: identity.Fanout,
			WorkloadSHA256: sha('d'), ActivityOptionsSHA256: sha('e'), HostEnvelopeSHA256: sha('f'), AgentBinarySHA256: sha('1'), DestinationProtocolSHA256: sha('2'), BarrierControllerSHA256: sha('3'), SourceSHA256: sha('4')},
		Verdict: protocol.Verdict{ProtocolVersion: protocol.PublicationProtocolVersion, RunID: identity.RunID, Admission: protocol.AdmissionValid,
			Correctness: protocol.OutcomePass, Safety: protocol.OutcomePass, Liveness: protocol.OutcomePass, Diagnosability: protocol.OutcomePass, EfficiencyEligible: true, Oracle: protocol.OracleProtocolVersion},
		Timing:    []protocol.TimingEvent{{Sequence: 1, Kind: protocol.EventInputRegistered, TimestampUTC: "2026-08-09T16:00:00Z"}, {Sequence: 2, Kind: protocol.EventAcknowledged, TimestampUTC: "2026-08-09T16:00:00.007Z", MonotonicOffsetNS: 7_000_000}},
		Execution: protocol.PublicationExecution{ProtocolVersion: protocol.PublicationProtocolVersion, RunID: identity.RunID, PairID: identity.PairID, ScheduleBlockID: identity.ScheduleBlockID, Topology: identity.Topology, ReplayVerified: true},
	}
}

func sha(character byte) string {
	value := make([]byte, 64)
	for index := range value {
		value[index] = character
	}
	return string(value)
}
