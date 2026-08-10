package protocol_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/internal/testfixture"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
)

func TestEvidenceBundleValidationCoversTypedArtifactsAndVerdict(t *testing.T) {
	bundle := testfixture.Bundle(testBlock(protocol.ProbeProtected), protocol.TopologyDirectActivity)
	acceptedEvent := bundle.CausalEvents[5]
	reconciledEvent := bundle.CausalEvents[4]
	dependencyEvent := bundle.CausalEvents[3]
	bundle.Destination.Actions = []protocol.DestinationAction{{
		EventID: acceptedEvent.EventID, WorkItemID: "item-001", LogicalEffectID: "effect-1", ReceiptID: "receipt-1", Generation: 1,
		CapabilityHash: testfixture.Hash('a'), Decision: protocol.DecisionAccepted, Applied: true,
	}}
	bundle.Dependency.Requests = []protocol.DependencyRequest{{
		RequestID: "request-1", EventID: dependencyEvent.EventID, WorkItemID: "item-001", Attempt: 1, Outcome: "ok", CostUnits: 1,
	}}
	bundle.Destination.Actions = append(bundle.Destination.Actions, protocol.DestinationAction{
		EventID: reconciledEvent.EventID, WorkItemID: "item-001", LogicalEffectID: "effect-1", ReceiptID: "receipt-1",
		Generation: 1, CapabilityHash: testfixture.Hash('a'), Decision: protocol.DecisionReconciled, Applied: false,
	})
	if err := bundle.Validate(); err != nil {
		t.Fatal(err)
	}
	if protocol.CaseJoinBarrier.Suite() != protocol.SuiteOrchestrationSemantics ||
		protocol.CaseSilentProgress.Suite() != protocol.SuiteRecoveryDynamics || protocol.CaseID("unknown").Suite() != "" {
		t.Fatal("case suite mapping drifted")
	}

	tests := []struct {
		name   string
		mutate func(*protocol.EvidenceBundle)
	}{
		{name: "authority", mutate: func(value *protocol.EvidenceBundle) { value.Authority.Epochs[0].State = protocol.AuthorityRevoked }},
		{name: "destination", mutate: func(value *protocol.EvidenceBundle) {
			value.Destination.Actions = []protocol.DestinationAction{{EventID: "bad", Decision: protocol.DecisionAccepted, Applied: false}}
		}},
		{name: "destination event reference", mutate: func(value *protocol.EvidenceBundle) {
			value.Destination.Actions = []protocol.DestinationAction{{
				EventID: "missing", WorkItemID: "item-001", LogicalEffectID: "effect-1", ReceiptID: "receipt-1",
				Generation: 1, CapabilityHash: testfixture.Hash('a'), Decision: protocol.DecisionAccepted, Applied: true,
			}}
		}},
		{name: "dependency", mutate: func(value *protocol.EvidenceBundle) {
			value.Dependency.Requests = []protocol.DependencyRequest{{RequestID: "bad"}}
		}},
		{name: "dependency event reference", mutate: func(value *protocol.EvidenceBundle) {
			value.Dependency.Requests = []protocol.DependencyRequest{{RequestID: "request", EventID: "missing", WorkItemID: "item-001", Attempt: 1, Outcome: "ok"}}
		}},
		{name: "fault", mutate: func(value *protocol.EvidenceBundle) { value.FaultBoundary.ExpectedBoundary = "wrong" }},
		{name: "native history", mutate: func(value *protocol.EvidenceBundle) { value.NativeHistory.Captured = false }},
		{name: "native history event count", mutate: func(value *protocol.EvidenceBundle) { value.NativeHistory.EventCount++ }},
		{name: "native history hash", mutate: func(value *protocol.EvidenceBundle) { value.NativeHistory.HistorySHA256 = testfixture.Hash('9') }},
		{name: "process", mutate: func(value *protocol.EvidenceBundle) { value.ProcessObservations.Observations[0].WorkerPID = 0 }},
		{name: "effective input", mutate: func(value *protocol.EvidenceBundle) { value.EffectiveInput.SourceSHA256 = "bad" }},
		{name: "verdict", mutate: func(value *protocol.EvidenceBundle) { value.Verdict.Correctness = "maybe" }},
		{name: "execution", mutate: func(value *protocol.EvidenceBundle) { value.Execution.Topology = protocol.TopologyChildWorkflow }},
		{name: "timing", mutate: func(value *protocol.EvidenceBundle) { value.Timing[1].MonotonicOffsetNS = -1 }},
		{name: "run identity", mutate: func(value *protocol.EvidenceBundle) { value.Dependency.RunID = "other" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := testfixture.Bundle(testBlock(protocol.ProbeProtected), protocol.TopologyDirectActivity)
			test.mutate(&candidate)
			if err := candidate.Validate(); !errors.Is(err, protocol.ErrInvalidEvidence) {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
}

func TestEvidenceBundleRejectsSyntheticHistoryWithoutExplicitFixtureProvenance(t *testing.T) {
	bundle := testfixture.Bundle(testBlock(protocol.ProbeProtected), protocol.TopologyDirectActivity)
	bundle.NativeHistory.Export = json.RawMessage(fmt.Sprintf(`{"run_id":%q,"event_count":%d}`,
		bundle.Manifest.RunID, bundle.NativeHistory.EventCount))
	hash, err := protocol.NativeExportSHA256(bundle.NativeHistory.Export)
	if err != nil {
		t.Fatal(err)
	}
	bundle.NativeHistory.HistorySHA256 = hash
	if err := bundle.ValidateRaw(); err == nil {
		t.Fatal("synthetic history without fixture provenance was accepted")
	}
}

func TestLineageRequiresEveryEventToLeadToAcknowledgement(t *testing.T) {
	bundle := testfixture.Bundle(testBlock(protocol.ProbeProtected), protocol.TopologyDirectActivity)
	ackIndex := len(bundle.CausalEvents) - 1
	ack := bundle.CausalEvents[ackIndex]
	orphan := bundle.CausalEvents[0]
	orphan.Sequence = ack.Sequence
	orphan.EventID = "orphan-event"
	orphan.ParentEventIDs = []string{bundle.CausalEvents[0].EventID}
	orphan.Kind = protocol.EventProgressAccepted
	orphan.TimestampUTC = ack.TimestampUTC
	orphan.MonotonicOffsetNS = ack.MonotonicOffsetNS
	ack.Sequence++
	bundle.CausalEvents = append(bundle.CausalEvents[:ackIndex], orphan, ack)
	bundle.Lineage.Edges = append(bundle.Lineage.Edges, protocol.LineageEdge{ParentEventID: orphan.ParentEventIDs[0], ChildEventID: orphan.EventID})
	if err := bundle.ValidateRaw(); !errors.Is(err, protocol.ErrInvalidEvidence) {
		t.Fatalf("orphan event error = %v", err)
	}
}

func TestUnfaultedBundleHasNoInjectedBoundary(t *testing.T) {
	bundle := testfixture.Bundle(testBlock(protocol.ProbeUnfaulted), protocol.TopologyChildWorkflow)
	if err := bundle.Validate(); err != nil {
		t.Fatal(err)
	}
	if bundle.FaultBoundary.Injected {
		t.Fatal("unfaulted fixture injected a fault")
	}
}

func testBlock(probe protocol.Probe) protocol.PairBlock {
	boundary := "designated-item-result-observed-before-activity-completion"
	if probe == protocol.ProbeUnfaulted {
		boundary = protocol.UnfaultedBoundary
	}
	stratum := protocol.Stratum{
		ID:   "join-barrier/" + boundary + "/" + string(probe) + "/fanout-008",
		Case: protocol.CaseJoinBarrier, Boundary: boundary, Probe: probe, Fanout: 8,
	}
	return protocol.PairBlock{
		Index: 1, PairID: "topology-pilot-v1/" + stratum.ID + "/slot-01",
		Stratum: stratum, Slot: 1, TopologyOrder: protocol.Topologies(),
		ScheduleBlockID: "schedule-block/topology-pilot-v1/" + stratum.ID + "/slot-01",
	}
}
