package oracle

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/evidence"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/internal/testfixture"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
)

func TestEvaluateIndependentlyAdmitsCompleteExactBarrierEvidence(t *testing.T) {
	bundle := completeBundle(protocol.TopologyDirectActivity)
	want := passingVerdict(bundle.Manifest.RunID)
	if got := Evaluate(bundle); !reflect.DeepEqual(got, want) {
		t.Fatalf("verdict = %+v, want %+v", got, want)
	}
	bundle.Verdict = want
	root := t.TempDir()
	directory, err := evidence.WriteRun(root, bundle)
	if err != nil {
		t.Fatal(err)
	}
	loaded, verdict, err := VerifyRun(root, directory)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Manifest.RunID != bundle.Manifest.RunID || !reflect.DeepEqual(verdict, want) {
		t.Fatalf("verified = %+v / %+v", loaded.Manifest, verdict)
	}
}

func TestEvaluateRejectsApparatusAndDiagnosabilityFailures(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.EvidenceBundle)
	}{
		{name: "missing lineage", mutate: func(value *protocol.EvidenceBundle) { value.Lineage.Edges = value.Lineage.Edges[1:] }},
		{name: "missed barrier", mutate: func(value *protocol.EvidenceBundle) {
			event(value, value.FaultBoundary.BarrierEventID).Kind = protocol.EventProgressAccepted
		}},
		{name: "wrong barrier process", mutate: func(value *protocol.EvidenceBundle) { value.FaultBoundary.TargetProcessIdentity = "pid:wrong" }},
		{name: "fault before barrier", mutate: func(value *protocol.EvidenceBundle) {
			barrier := event(value, value.FaultBoundary.BarrierEventID)
			fault := event(value, value.FaultBoundary.FaultEventID)
			barrier.Sequence, fault.Sequence = fault.Sequence, barrier.Sequence
		}},
		{name: "replay failure", mutate: func(value *protocol.EvidenceBundle) {
			value.NativeHistory.ReplayCompatible = false
			value.NativeHistory.ReplayError = "nondeterminism"
			value.Execution.ReplayVerified = false
		}},
		{name: "missing required item", mutate: func(value *protocol.EvidenceBundle) {
			value.Workload.RequiredItemIDs = value.Workload.RequiredItemIDs[:7]
		}},
		{name: "child without child identity", mutate: func(value *protocol.EvidenceBundle) { setTopology(value, protocol.TopologyChildWorkflow, false) }},
		{name: "direct with child identity", mutate: func(value *protocol.EvidenceBundle) {
			for index := range value.CausalEvents {
				value.CausalEvents[index].ChildWorkflowID = "unexpected-child"
				value.CausalEvents[index].ChildRunID = "unexpected-run"
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bundle := completeBundle(protocol.TopologyDirectActivity)
			test.mutate(&bundle)
			verdict := Evaluate(bundle)
			if verdict.Admission != protocol.AdmissionInvalid || verdict.Diagnosability != protocol.OutcomeFail || verdict.EfficiencyEligible || len(verdict.ReasonCodes) == 0 {
				t.Fatalf("invalid apparatus verdict = %+v", verdict)
			}
		})
	}
}

func TestLogicalFailureIsRetainedAsAdmittedOutcomeNotExcluded(t *testing.T) {
	bundle := completeBundle(protocol.TopologyDirectActivity)
	bundle.Workload.ActualLogicalOutput = "wrong"
	bundle.Workload.Semantics.Continuations[0].Members = bundle.Workload.Semantics.Continuations[0].Members[:7]
	setMetric(bundle.Workload.Semantics.Metrics, "premature_continuation_count", 1)
	bundle.Workload.ProhibitedActionCount = 1
	bundle.Workload.LivenessSatisfied = false
	verdict := Evaluate(bundle)
	if verdict.Admission != protocol.AdmissionValid || verdict.Correctness != protocol.OutcomeFail ||
		verdict.Safety != protocol.OutcomeFail || verdict.Liveness != protocol.OutcomeFail || verdict.Diagnosability != protocol.OutcomePass ||
		verdict.EfficiencyEligible || len(verdict.ReasonCodes) != 0 {
		t.Fatalf("logical failure verdict = %+v", verdict)
	}
}

func TestEvaluateRejectsOrchestrationControlCountNotReconstructedFromRawEvidence(t *testing.T) {
	bundle := testfixture.Bundle(caseFixtureBlock(protocol.CaseJoinBarrier, protocol.ProbeProtected), protocol.TopologyDirectActivity)
	bundle.Workload.ProhibitedActionCount = 1
	verdict := Evaluate(bundle)
	if verdict.Admission != protocol.AdmissionInvalid ||
		!reflect.DeepEqual(verdict.ReasonCodes, []string{"orchestration_control_mismatch"}) {
		t.Fatalf("unreconstructible orchestration control verdict = %+v", verdict)
	}
}

func TestEvaluateRejectsOrchestrationMetricNotReconstructedFromRawEvidence(t *testing.T) {
	bundle := testfixture.Bundle(caseFixtureBlock(protocol.CaseJoinBarrier, protocol.ProbeProtected), protocol.TopologyDirectActivity)
	setMetric(bundle.Workload.Semantics.Metrics, "premature_continuation_count", 1)
	verdict := Evaluate(bundle)
	if verdict.Admission != protocol.AdmissionInvalid ||
		!reflect.DeepEqual(verdict.ReasonCodes, []string{"orchestration_metric_mismatch"}) {
		t.Fatalf("unreconstructible orchestration metric verdict = %+v", verdict)
	}
}

func TestIncompleteLogicalWorkIsAResultNotAnApparatusExclusion(t *testing.T) {
	bundle := completeBundle(protocol.TopologyDirectActivity)
	bundle.Workload.AcceptedResultItemIDs = bundle.Workload.AcceptedResultItemIDs[:7]
	bundle.Workload.ActualLogicalOutput = "incomplete"
	bundle.Workload.LivenessSatisfied = false
	verdict := Evaluate(bundle)
	if verdict.Admission != protocol.AdmissionValid || verdict.Correctness != protocol.OutcomeFail ||
		verdict.Liveness != protocol.OutcomeFail || verdict.EfficiencyEligible {
		t.Fatalf("incomplete logical work verdict = %+v", verdict)
	}
}

func TestVerifyRunRejectsStoredVerdictThatDisagreesWithRawEvidence(t *testing.T) {
	bundle := completeBundle(protocol.TopologyDirectActivity)
	bundle.Verdict = passingVerdict(bundle.Manifest.RunID)
	bundle.Verdict.Safety = protocol.OutcomeFail
	bundle.Verdict.EfficiencyEligible = false
	root := t.TempDir()
	directory, err := evidence.WriteRun(root, bundle)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := VerifyRun(root, directory); !errors.Is(err, protocol.ErrInvalidEvidence) {
		t.Fatalf("mismatched verdict error = %v", err)
	}
	if filepath.Dir(directory) != root {
		t.Fatalf("test evidence escaped root: %s", directory)
	}
}

func TestEvaluateUnfaultedEvidenceRequiresNoBarrier(t *testing.T) {
	bundle := testfixture.Bundle(fixtureBlock(protocol.ProbeUnfaulted), protocol.TopologyChildWorkflow)
	if verdict := Evaluate(bundle); verdict.Admission != protocol.AdmissionValid || !verdict.EfficiencyEligible {
		t.Fatalf("unfaulted verdict = %+v", verdict)
	}
	bundle.FaultBoundary.Injected = true
	bundle.FaultBoundary.ExpectedBoundary = protocol.UnfaultedBoundary
	bundle.FaultBoundary.BarrierEventID = "invented"
	bundle.FaultBoundary.FaultEventID = "invented-fault"
	bundle.FaultBoundary.TargetProcessIdentity = "invented-process"
	if verdict := Evaluate(bundle); verdict.Admission != protocol.AdmissionInvalid {
		t.Fatalf("faulted unfaulted verdict = %+v", verdict)
	}
}

func TestEvaluateDerivesDestinationSafetyFromAuthorityAndStableEffectID(t *testing.T) {
	tests := []struct {
		name    string
		actions []protocol.DestinationAction
	}{
		{name: "stale generation", actions: []protocol.DestinationAction{{
			EventID: "action-1", WorkItemID: "item-001", LogicalEffectID: "effect-1", ReceiptID: "receipt-1", Generation: 2,
			CapabilityHash: testfixture.Hash('a'), Decision: protocol.DecisionAccepted, Applied: true,
		}}},
		{name: "duplicate stable effect", actions: []protocol.DestinationAction{
			{EventID: "action-1", WorkItemID: "item-001", LogicalEffectID: "effect-1", ReceiptID: "receipt-1", Generation: 1, CapabilityHash: testfixture.Hash('a'), Decision: protocol.DecisionAccepted, Applied: true},
			{EventID: "action-2", WorkItemID: "item-001", LogicalEffectID: "effect-1", ReceiptID: "receipt-2", Generation: 1, CapabilityHash: testfixture.Hash('a'), Decision: protocol.DecisionAccepted, Applied: true},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bundle := testfixture.Bundle(fixtureBlock(protocol.ProbeProtected), protocol.TopologyDirectActivity)
			bundle.Destination.Actions = test.actions
			for index := range bundle.Destination.Actions {
				event := &bundle.CausalEvents[4+index]
				bundle.Destination.Actions[index].EventID = event.EventID
				event.Generation = bundle.Destination.Actions[index].Generation
				event.CapabilityHash = bundle.Destination.Actions[index].CapabilityHash
			}
			verdict := Evaluate(bundle)
			if verdict.Admission != protocol.AdmissionValid || verdict.Safety != protocol.OutcomeFail || verdict.EfficiencyEligible {
				t.Fatalf("destination verdict = %+v", verdict)
			}
		})
	}
}

func TestEvaluateAcceptsDestinationReceiptReconciliationWithoutReapplying(t *testing.T) {
	bundle := testfixture.Bundle(fixtureBlock(protocol.ProbeProtected), protocol.TopologyDirectActivity)
	bundle.Destination.Actions = []protocol.DestinationAction{
		{EventID: bundle.CausalEvents[4].EventID, WorkItemID: "item-001", LogicalEffectID: "effect-1", ReceiptID: "receipt-1", Generation: 1, CapabilityHash: testfixture.Hash('a'), Decision: protocol.DecisionAccepted, Applied: true},
		{EventID: bundle.CausalEvents[5].EventID, WorkItemID: "item-001", LogicalEffectID: "effect-1", ReceiptID: "receipt-1", Generation: 1, CapabilityHash: testfixture.Hash('a'), Decision: protocol.DecisionReconciled},
	}
	if verdict := Evaluate(bundle); verdict.Admission != protocol.AdmissionValid || verdict.Safety != protocol.OutcomePass {
		t.Fatalf("reconciled destination verdict = %+v", verdict)
	}
}

func TestEvaluateReconstructsOrchestrationSemanticsInsteadOfTrustingReportedOutcome(t *testing.T) {
	tests := []struct {
		name          string
		benchmarkCase protocol.CaseID
		mutate        func(*protocol.EvidenceBundle)
	}{
		{
			name: "premature join continuation", benchmarkCase: protocol.CaseJoinBarrier,
			mutate: func(bundle *protocol.EvidenceBundle) {
				continuation := &bundle.Workload.Semantics.Continuations[0]
				continuation.Members = continuation.Members[:len(continuation.Members)-1]
				setMetric(bundle.Workload.Semantics.Metrics, "premature_continuation_count", 1)
			},
		},
		{
			name: "incorrect partial reduction", benchmarkCase: protocol.CaseIncrementalPartialReduction,
			mutate: func(bundle *protocol.EvidenceBundle) {
				bundle.Workload.Semantics.Checkpoints[0].Value++
				setMetric(bundle.Workload.Semantics.Metrics, "incorrect_reduction_count", 1)
			},
		},
		{
			name: "stale result after supersession", benchmarkCase: protocol.CaseQueuedExecutingSupersession,
			mutate: func(bundle *protocol.EvidenceBundle) {
				observation := bundle.Workload.Semantics.Supersession
				for index := range bundle.CausalEvents {
					event := &bundle.CausalEvents[index]
					if event.Kind == protocol.EventResultAccepted && event.Sequence > eventSequence(*bundle, observation.CommitEventID) &&
						event.WorkItemID == observation.ObsoleteItemID {
						event.Generation = observation.ObsoleteGeneration
						event.CapabilityHash = observation.ObsoleteCapabilityHash
						break
					}
				}
				setMetric(bundle.Workload.Semantics.Metrics, "stale_action_accept_count", 1)
			},
		},
		{
			name: "destructive version jump", benchmarkCase: protocol.CaseDestructiveTransition,
			mutate: func(bundle *protocol.EvidenceBundle) {
				observation := bundle.Workload.Semantics.Destructive
				observation.Deliveries[0].ResultingVersion = 2
				observation.FinalVersion = 2
				bundle.Workload.Semantics.Continuations[0].Value = 2
				setMetric(bundle.Workload.Semantics.Metrics, "invariant_violation_count", 2)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bundle := testfixture.Bundle(caseFixtureBlock(test.benchmarkCase, protocol.ProbeProtected), protocol.TopologyDirectActivity)
			test.mutate(&bundle)
			bundle.Workload.ProhibitedActionCount = int(fixtureMetricValueForCase(bundle.Workload.Semantics.Metrics, test.benchmarkCase))
			verdict := Evaluate(bundle)
			if verdict.Admission != protocol.AdmissionValid || verdict.Correctness != protocol.OutcomeFail ||
				verdict.Safety != protocol.OutcomeFail || verdict.EfficiencyEligible {
				t.Fatalf("reconstructed verdict = %+v", verdict)
			}
		})
	}
}

func fixtureMetricValueForCase(metrics []protocol.Metric, benchmarkCase protocol.CaseID) int64 {
	name := map[protocol.CaseID]string{
		protocol.CaseJoinBarrier:                 "premature_continuation_count",
		protocol.CaseIncrementalPartialReduction: "incorrect_reduction_count",
		protocol.CaseQueuedExecutingSupersession: "stale_action_accept_count",
		protocol.CaseDestructiveTransition:       "invariant_violation_count",
	}[benchmarkCase]
	for _, metric := range metrics {
		if metric.Name == name {
			return metric.Value
		}
	}
	return 0
}

func TestEvaluateRejectsMissingCaseMetricAsInvalidEvidence(t *testing.T) {
	bundle := testfixture.Bundle(caseFixtureBlock(protocol.CaseJoinBarrier, protocol.ProbeProtected), protocol.TopologyDirectActivity)
	bundle.Workload.Semantics.Metrics = bundle.Workload.Semantics.Metrics[1:]
	if verdict := Evaluate(bundle); verdict.Admission != protocol.AdmissionInvalid ||
		!reflect.DeepEqual(verdict.ReasonCodes, []string{ReasonRawEvidenceInvalid}) {
		t.Fatalf("missing metric verdict = %+v", verdict)
	}
}

func TestEvaluateRejectsRecoveryProhibitedCountNotReconstructedFromRawEvidence(t *testing.T) {
	bundle := testfixture.Bundle(recoveryFixtureBlock(protocol.CaseBackpressureOverload, protocol.ProbeProtected), protocol.TopologyDirectActivity)
	bundle.Workload.ProhibitedActionCount = 1
	verdict := Evaluate(bundle)
	if verdict.Admission != protocol.AdmissionInvalid ||
		!reflect.DeepEqual(verdict.ReasonCodes, []string{"recovery_control_mismatch"}) {
		t.Fatalf("unreconstructible recovery control verdict = %+v", verdict)
	}
}

func TestEvaluateRejectsRecoveryAggregateCountsNotBackedByRawEvidence(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.RecoveryItemObservation)
	}{
		{name: "accepted effects", mutate: func(item *protocol.RecoveryItemObservation) { item.AcceptedEffects++ }},
		{name: "accepted results", mutate: func(item *protocol.RecoveryItemObservation) { item.AcceptedResults++ }},
		{name: "activity attempts", mutate: func(item *protocol.RecoveryItemObservation) { item.ActivityAttempts++ }},
		{name: "agent processes", mutate: func(item *protocol.RecoveryItemObservation) { item.AgentProcesses++ }},
		{name: "cost units", mutate: func(item *protocol.RecoveryItemObservation) { item.CostUnits++ }},
		{name: "role", mutate: func(item *protocol.RecoveryItemObservation) { item.Role = "declared-wait" }},
		{name: "poison designation", mutate: func(item *protocol.RecoveryItemObservation) { item.Poison = true }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bundle := testfixture.Bundle(recoveryFixtureBlock(protocol.CaseCrashRecoveryBoundaries, protocol.ProbeProtected), protocol.TopologyDirectActivity)
			test.mutate(&bundle.Workload.Recovery.Items[0])
			verdict := Evaluate(bundle)
			if verdict.Admission != protocol.AdmissionInvalid ||
				!reflect.DeepEqual(verdict.ReasonCodes, []string{"recovery_observation_mismatch"}) {
				t.Fatalf("unbacked recovery aggregate verdict = %+v", verdict)
			}
		})
	}
}

func TestEvaluateRejectsRecoveryMetricNotReconstructedFromRawEvidence(t *testing.T) {
	bundle := testfixture.Bundle(recoveryFixtureBlock(protocol.CaseBackpressureOverload, protocol.ProbeProtected), protocol.TopologyDirectActivity)
	setMetric(bundle.Workload.Recovery.Metrics, "peak_in_flight_count", 7)
	verdict := Evaluate(bundle)
	if verdict.Admission != protocol.AdmissionInvalid ||
		!reflect.DeepEqual(verdict.ReasonCodes, []string{"recovery_metric_mismatch"}) {
		t.Fatalf("unreconstructible recovery metric verdict = %+v", verdict)
	}
}

func TestEvaluateRejectsEveryRecoveryMetricNotReconstructedFromRawEvidence(t *testing.T) {
	for _, benchmarkCase := range protocol.Cases() {
		if benchmarkCase.Suite() != protocol.SuiteRecoveryDynamics {
			continue
		}
		baseline := testfixture.Bundle(recoveryFixtureBlock(benchmarkCase, protocol.ProbeProtected), protocol.TopologyDirectActivity)
		for _, metric := range baseline.Workload.Recovery.Metrics {
			t.Run(string(benchmarkCase)+"/"+metric.Name, func(t *testing.T) {
				bundle := testfixture.Bundle(recoveryFixtureBlock(benchmarkCase, protocol.ProbeProtected), protocol.TopologyDirectActivity)
				setMetric(bundle.Workload.Recovery.Metrics, metric.Name, metric.Value+1)
				verdict := Evaluate(bundle)
				if verdict.Admission != protocol.AdmissionInvalid ||
					!reflect.DeepEqual(verdict.ReasonCodes, []string{ReasonRecoveryMetricMismatch}) {
					t.Fatalf("unreconstructible %s metric verdict = %+v", metric.Name, verdict)
				}
			})
		}
	}
}

func TestSilentProgressDetectionIgnoresGenericRecoveryObservation(t *testing.T) {
	bundle := protocol.EvidenceBundle{CausalEvents: []protocol.CausalEvent{
		{Identity: protocol.Identity{WorkItemID: "item-001"}, Sequence: 1, EventID: "progress", MonotonicOffsetNS: 1_000_000, Kind: protocol.EventProgressAccepted, Decision: protocol.DecisionAccepted},
		{Identity: protocol.Identity{WorkItemID: "item-001"}, Sequence: 2, EventID: "generic-recovery", MonotonicOffsetNS: 2_000_000, Kind: protocol.EventRecoveryObserved, Decision: protocol.DecisionObserved},
		{Identity: protocol.Identity{WorkItemID: "item-001"}, Sequence: 3, EventID: "deadline-revocation", MonotonicOffsetNS: 4_000_000, Kind: protocol.EventAuthorityRevoked, Decision: protocol.DecisionAccepted},
	}}

	progress, detection := silentProgressDetectionSequences(bundle)
	if progress != 1 || detection != 3 {
		t.Fatalf("progress/detection = %d/%d, want 1/3", progress, detection)
	}
}

func TestSilentProgressDetectionUsesFailedRecoveryWithoutRevocation(t *testing.T) {
	bundle := protocol.EvidenceBundle{CausalEvents: []protocol.CausalEvent{
		{Identity: protocol.Identity{WorkItemID: "item-001"}, Sequence: 1, EventID: "progress", MonotonicOffsetNS: 1_000_000, Kind: protocol.EventProgressAccepted, Decision: protocol.DecisionAccepted},
		{Identity: protocol.Identity{WorkItemID: "item-001"}, Sequence: 2, EventID: "generic-recovery", MonotonicOffsetNS: 2_000_000, Kind: protocol.EventRecoveryObserved, Decision: protocol.DecisionObserved},
		{Identity: protocol.Identity{WorkItemID: "item-001"}, Sequence: 3, EventID: "deadline-missed", MonotonicOffsetNS: 4_000_000, Kind: protocol.EventRecoveryObserved, Decision: protocol.DecisionFailed},
	}}

	progress, detection := silentProgressDetectionSequences(bundle)
	if progress != 1 || detection != 3 {
		t.Fatalf("progress/detection = %d/%d, want 1/3", progress, detection)
	}
}

func TestEvaluateRejectsEveryOrchestrationMetricNotReconstructedFromRawEvidence(t *testing.T) {
	for _, benchmarkCase := range protocol.Cases() {
		if benchmarkCase.Suite() != protocol.SuiteOrchestrationSemantics {
			continue
		}
		baseline := testfixture.Bundle(caseFixtureBlock(benchmarkCase, protocol.ProbeProtected), protocol.TopologyDirectActivity)
		for _, metric := range baseline.Workload.Semantics.Metrics {
			t.Run(string(benchmarkCase)+"/"+metric.Name, func(t *testing.T) {
				bundle := testfixture.Bundle(caseFixtureBlock(benchmarkCase, protocol.ProbeProtected), protocol.TopologyDirectActivity)
				setMetric(bundle.Workload.Semantics.Metrics, metric.Name, metric.Value+1)
				verdict := Evaluate(bundle)
				if verdict.Admission != protocol.AdmissionInvalid ||
					!reflect.DeepEqual(verdict.ReasonCodes, []string{ReasonOrchestrationMetricMismatch}) {
					t.Fatalf("unreconstructible %s metric verdict = %+v", metric.Name, verdict)
				}
			})
		}
	}
}

func completeBundle(topology protocol.Topology) protocol.EvidenceBundle {
	const fanout = 8
	base := protocol.Identity{
		ProtocolVersion: protocol.PublicationProtocolVersion,
		RunID:           "run-direct", PairID: "pair-1", ScheduleBlockID: "schedule-block/pair-1", TrackerBeadID: "temporal_projects-4ic.1",
		Topology: topology, Case: protocol.CaseJoinBarrier,
		Boundary: "designated-item-result-observed-before-activity-completion", Probe: protocol.ProbeProtected, Fanout: fanout,
		LogicalOperationID: "operation-1", Generation: 1, CapabilityHash: hash('a'), ParentWorkflowID: "parent-workflow", ParentRunID: "parent-run",
		ActivityAttempt: 1, WorkerID: "worker-1", WorkerPID: 101,
	}
	if topology == protocol.TopologyChildWorkflow {
		base.RunID = "run-child"
	}
	events := make([]protocol.CausalEvent, 0, fanout*2+5)
	appendEvent := func(identity protocol.Identity, kind, decision string, parents ...string) string {
		sequence := len(events) + 1
		eventID := fmt.Sprintf("event-%03d", sequence)
		events = append(events, protocol.CausalEvent{
			Identity: identity, Sequence: uint64(sequence), EventID: eventID, ParentEventIDs: parents,
			TimestampUTC: fmt.Sprintf("2026-08-09T16:00:00.%09dZ", sequence), MonotonicOffsetNS: int64(sequence),
			Kind: kind, Decision: decision,
		})
		return eventID
	}
	itemIdentity := func(index int) protocol.Identity {
		identity := base
		identity.WorkItemID = fmt.Sprintf("item-%03d", index)
		identity.ActivityID = fmt.Sprintf("activity-%03d", index)
		identity.ProcessIdentity = fmt.Sprintf("pid:%d/start:item-%03d", 100+index, index)
		identity.WorkerPID = 100 + index
		if topology == protocol.TopologyChildWorkflow {
			identity.ChildWorkflowID = fmt.Sprintf("child-%03d", index)
			identity.ChildRunID = fmt.Sprintf("child-run-%03d", index)
		}
		return identity
	}
	rootIdentity := itemIdentity(1)
	root := appendEvent(rootIdentity, protocol.EventInputRegistered, protocol.DecisionObserved)
	results := make([]string, 0, fanout)
	items := make([]string, 0, fanout)
	processes := make([]protocol.ProcessObservation, 0, fanout)
	barrierID, faultID := "", ""
	for index := 1; index <= fanout; index++ {
		identity := itemIdentity(index)
		items = append(items, identity.WorkItemID)
		started := appendEvent(identity, protocol.EventActivityStarted, protocol.DecisionObserved, root)
		processes = append(processes, protocol.ProcessObservation{EventID: started, WorkItemID: identity.WorkItemID, WorkerID: identity.WorkerID, WorkerPID: identity.WorkerPID, ProcessIdentity: identity.ProcessIdentity, State: "running"})
		parent := started
		if index == 1 {
			barrierID = appendEvent(identity, protocol.EventBarrierReached, protocol.DecisionBlocked, started)
			faultID = appendEvent(identity, protocol.EventFaultCommitted, protocol.DecisionAccepted, barrierID)
			parent = appendEvent(identity, protocol.EventRecoveryObserved, protocol.DecisionObserved, faultID)
			processes[0].EventID = barrierID
			processes[0].State = "blocked-at-barrier"
		}
		results = append(results, appendEvent(identity, protocol.EventResultAccepted, protocol.DecisionAccepted, parent))
	}
	continuationID := base.LogicalOperationID + "/continuation"
	continuationEvent := appendEvent(rootIdentity, protocol.EventContinuationAccepted, protocol.DecisionAccepted, results...)
	outcome := appendEvent(rootIdentity, protocol.EventOutcomeAccepted, protocol.DecisionAccepted, continuationEvent)
	appendEvent(rootIdentity, protocol.EventAcknowledged, protocol.DecisionAccepted, outcome)
	edges := make([]protocol.LineageEdge, 0, len(events))
	for _, causal := range events {
		for _, parent := range causal.ParentEventIDs {
			edges = append(edges, protocol.LineageEdge{ParentEventID: parent, ChildEventID: causal.EventID})
		}
	}
	manifest := protocol.Manifest{
		ProtocolVersion: protocol.PublicationProtocolVersion, RunID: base.RunID, PairID: base.PairID,
		ScheduleBlockID: base.ScheduleBlockID, TrackerBeadID: base.TrackerBeadID, Topology: topology, Case: base.Case,
		Boundary: base.Boundary, Probe: base.Probe, Fanout: fanout, LogicalOperationID: base.LogicalOperationID,
		CreatedAtUTC: "2026-08-09T16:00:00Z", RequiredEvidence: protocol.RequiredEvidenceFiles(),
	}
	nativeExport := json.RawMessage(fmt.Sprintf(`{"run_id":%q,"event_count":%d,"fixture":true}`, base.RunID, len(events)))
	nativeHash, err := protocol.NativeExportSHA256(nativeExport)
	if err != nil {
		panic(err)
	}
	nativeBytes, err := protocol.NativeExportByteCount(nativeExport)
	if err != nil {
		panic(err)
	}
	bundle := protocol.EvidenceBundle{
		Manifest: manifest, CausalEvents: events, Lineage: protocol.Lineage{RunID: base.RunID, Edges: edges},
		Authority:   protocol.AuthorityState{RunID: base.RunID, CurrentGeneration: 1, CurrentCapabilityHash: base.CapabilityHash, Epochs: []protocol.AuthorityEpoch{{Generation: 1, CapabilityHash: base.CapabilityHash, State: protocol.AuthorityActive}}},
		Destination: protocol.DestinationState{RunID: base.RunID}, Dependency: protocol.DependencyState{RunID: base.RunID},
		Workload: protocol.WorkloadState{
			RunID: base.RunID, RequiredItemIDs: items, AcceptedResultItemIDs: append([]string(nil), items...),
			ExpectedLogicalOutput: "ok", ActualLogicalOutput: "ok", LivenessSatisfied: true,
			Semantics: protocol.OrchestrationSemantics{
				Continuations: []protocol.ContinuationObservation{{
					EventID: continuationEvent, ContinuationID: continuationID, Members: append([]string(nil), items...),
					ReceiptID: "receipt/" + continuationID, Decision: protocol.DecisionAccepted, Applied: true,
				}},
				Metrics: []protocol.Metric{
					{Name: "premature_continuation_count", Unit: "count"},
					{Name: "accepted_continuation_count", Unit: "count", Value: 1},
					{Name: "join_lag_after_last_required_result_ms", Unit: "ms"},
					{Name: "end_to_end_latency_ms", Unit: "ms"},
					{Name: "history_bytes_per_item", Unit: "bytes_per_item", Value: int64(nativeBytes / fanout)},
				},
			},
		},
		FaultBoundary:       protocol.FaultBoundary{RunID: base.RunID, Injected: true, ExpectedBoundary: base.Boundary, BarrierEventID: barrierID, FaultEventID: faultID, TargetProcessIdentity: itemIdentity(1).ProcessIdentity},
		NativeHistory:       protocol.NativeHistory{RunID: base.RunID, Captured: true, EventCount: len(events), Export: nativeExport, HistorySHA256: nativeHash, ReplayCompatible: true, ReplayWorkerSHA256: hash('c')},
		ProcessObservations: protocol.ProcessObservations{RunID: base.RunID, Observations: processes},
		EffectiveInput: protocol.EffectiveInput{RunID: base.RunID, PairID: base.PairID, ScheduleBlockID: base.ScheduleBlockID, Topology: topology, Case: base.Case, Boundary: base.Boundary, Probe: base.Probe, Fanout: fanout,
			WorkloadSHA256: hash('d'), ActivityOptionsSHA256: hash('e'), HostEnvelopeSHA256: hash('f'), AgentBinarySHA256: hash('1'), DestinationProtocolSHA256: hash('2'), BarrierControllerSHA256: hash('3'), SourceSHA256: hash('4')},
		Timing:    []protocol.TimingEvent{{Sequence: 1, Kind: protocol.EventInputRegistered, TimestampUTC: "2026-08-09T16:00:00Z"}, {Sequence: 2, Kind: protocol.EventAcknowledged, TimestampUTC: "2026-08-09T16:00:00.000000099Z", MonotonicOffsetNS: 99}},
		Execution: protocol.PublicationExecution{ProtocolVersion: protocol.PublicationProtocolVersion, RunID: base.RunID, PairID: base.PairID, ScheduleBlockID: base.ScheduleBlockID, Topology: topology, ReplayVerified: true},
	}
	bundle.Verdict = passingVerdict(base.RunID)
	return bundle
}

func event(bundle *protocol.EvidenceBundle, eventID string) *protocol.CausalEvent {
	for index := range bundle.CausalEvents {
		if bundle.CausalEvents[index].EventID == eventID {
			return &bundle.CausalEvents[index]
		}
	}
	panic("missing event " + eventID)
}

func setTopology(bundle *protocol.EvidenceBundle, topology protocol.Topology, addChild bool) {
	bundle.Manifest.Topology = topology
	bundle.EffectiveInput.Topology = topology
	bundle.Execution.Topology = topology
	for index := range bundle.CausalEvents {
		bundle.CausalEvents[index].Topology = topology
		if addChild {
			bundle.CausalEvents[index].ChildWorkflowID = "child"
			bundle.CausalEvents[index].ChildRunID = "child-run"
		}
	}
}

func passingVerdict(runID string) protocol.Verdict {
	return protocol.Verdict{
		ProtocolVersion: protocol.PublicationProtocolVersion, RunID: runID, Admission: protocol.AdmissionValid,
		Correctness: protocol.OutcomePass, Safety: protocol.OutcomePass, Liveness: protocol.OutcomePass,
		Diagnosability: protocol.OutcomePass, EfficiencyEligible: true, Oracle: protocol.OracleProtocolVersion,
	}
}

func hash(character byte) string {
	value := make([]byte, 64)
	for index := range value {
		value[index] = character
	}
	return string(value)
}

func fixtureBlock(probe protocol.Probe) protocol.PairBlock {
	boundary := "designated-item-result-observed-before-activity-completion"
	if probe == protocol.ProbeUnfaulted {
		boundary = protocol.UnfaultedBoundary
	}
	stratum := protocol.Stratum{
		ID:   "join-barrier/" + boundary + "/" + string(probe) + "/fanout-008",
		Case: protocol.CaseJoinBarrier, Boundary: boundary, Probe: probe, Fanout: 8,
	}
	pairID := "topology-pilot-v1/" + stratum.ID + "/slot-01"
	return protocol.PairBlock{
		Index: 1, ScheduleBlockID: "schedule-block/" + pairID, PairID: pairID, Stratum: stratum,
		Slot: 1, TopologyOrder: protocol.Topologies(),
	}
}

func caseFixtureBlock(benchmarkCase protocol.CaseID, probe protocol.Probe) protocol.PairBlock {
	boundary := map[protocol.CaseID]string{
		protocol.CaseJoinBarrier:                 "designated-item-result-observed-before-activity-completion",
		protocol.CaseIncrementalPartialReduction: "partial-checkpoint-accepted-before-checkpoint-activity-completion",
		protocol.CaseQueuedExecutingSupersession: "executing-after-process-start-before-effect",
		protocol.CaseDestructiveTransition:       "destination-accepted-before-activity-completion",
	}[benchmarkCase]
	if probe == protocol.ProbeUnfaulted {
		boundary = protocol.UnfaultedBoundary
	}
	stratum := protocol.Stratum{
		ID:   string(benchmarkCase) + "/" + boundary + "/" + string(probe) + "/fanout-008",
		Case: benchmarkCase, Boundary: boundary, Probe: probe, Fanout: 8,
	}
	pairID := "topology-pilot-v1/" + stratum.ID + "/slot-01"
	return protocol.PairBlock{
		Index: 1, ScheduleBlockID: "schedule-block/" + pairID, PairID: pairID, Stratum: stratum,
		Slot: 1, TopologyOrder: protocol.Topologies(),
	}
}

func recoveryFixtureBlock(benchmarkCase protocol.CaseID, probe protocol.Probe) protocol.PairBlock {
	boundary := map[protocol.CaseID]string{
		protocol.CaseCrashRecoveryBoundaries:   "result-observed-before-activity-completion",
		protocol.CaseLayeredRetryAmplification: "dependency-first-request-before-scripted-timeout-500-429-sequence",
		protocol.CaseOutageBacklogHerdRecovery: "outage-backlog-restoration-and-catchup-worker-crash",
		protocol.CaseBackpressureOverload:      "ready-workers-before-fixed-cohort-release",
		protocol.CasePoisonWorkIsolation:       "mixed-cohort-admitted-before-poison-failure-release",
		protocol.CaseSilentProgress:            "accepted-progress-before-executor-wedge",
	}[benchmarkCase]
	if probe == protocol.ProbeUnfaulted {
		boundary = protocol.UnfaultedBoundary
	}
	stratum := protocol.Stratum{
		ID:   string(benchmarkCase) + "/" + boundary + "/" + string(probe) + "/fanout-008",
		Case: benchmarkCase, Boundary: boundary, Probe: probe, Fanout: 8,
	}
	pairID := "topology-fixture-v1/" + stratum.ID + "/slot-01"
	return protocol.PairBlock{
		Index: 1, ScheduleBlockID: "schedule-block/" + pairID, PairID: pairID, Stratum: stratum,
		Slot: 1, TopologyOrder: protocol.Topologies(),
	}
}

func setMetric(metrics []protocol.Metric, name string, value int64) {
	for index := range metrics {
		if metrics[index].Name == name {
			metrics[index].Value = value
			return
		}
	}
	panic("missing metric " + name)
}
