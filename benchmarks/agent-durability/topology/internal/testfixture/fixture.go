// Package testfixture supplies deterministic topology evidence for apparatus
// admission checks. Fixture evidence is never publication evidence.
package testfixture

import (
	"encoding/json"
	"fmt"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
)

func Bundle(block protocol.PairBlock, topology protocol.Topology) protocol.EvidenceBundle {
	if block.Stratum.Case.Suite() == protocol.SuiteRecoveryDynamics {
		return recoveryBundle(block, topology)
	}
	runID := block.PairID + "/" + string(topology)
	base := protocol.Identity{
		ProtocolVersion: protocol.PublicationProtocolVersion,
		RunID:           runID, PairID: block.PairID, ScheduleBlockID: block.ScheduleBlockID, TrackerBeadID: "temporal_projects-4ic.4",
		Topology: topology, Case: block.Stratum.Case, Boundary: block.Stratum.Boundary, Probe: block.Stratum.Probe,
		Fanout: block.Stratum.Fanout, LogicalOperationID: block.PairID + "/operation", Generation: 1, CapabilityHash: Hash('a'),
		ParentWorkflowID: block.PairID + "/parent", ParentRunID: runID + "/parent-run", ActivityAttempt: 1, WorkerID: "worker-1",
	}
	itemIdentity := func(index int) protocol.Identity {
		identity := base
		identity.WorkItemID = fmt.Sprintf("item-%03d", index)
		identity.ActivityID = fmt.Sprintf("activity-%03d", index)
		identity.WorkerPID = 1000 + index
		identity.ProcessIdentity = fmt.Sprintf("pid:%d/start:item-%03d", identity.WorkerPID, index)
		if topology == protocol.TopologyChildWorkflow {
			identity.ChildWorkflowID = fmt.Sprintf("child-%03d", index)
			identity.ChildRunID = fmt.Sprintf("child-run-%03d", index)
		}
		return identity
	}
	events := make([]protocol.CausalEvent, 0, block.Stratum.Fanout*2+5)
	appendEvent := func(identity protocol.Identity, kind, decision string, parents ...string) string {
		sequence := len(events) + 1
		id := fmt.Sprintf("event-%04d", sequence)
		events = append(events, protocol.CausalEvent{
			Identity: identity, Sequence: uint64(sequence), EventID: id, ParentEventIDs: parents,
			TimestampUTC: fmt.Sprintf("2026-08-09T16:00:00.%09dZ", sequence), MonotonicOffsetNS: int64(sequence),
			Kind: kind, Decision: decision,
		})
		return id
	}
	rootIdentity := itemIdentity(1)
	root := appendEvent(rootIdentity, protocol.EventInputRegistered, protocol.DecisionObserved)
	items := make([]string, 0, block.Stratum.Fanout)
	results := make([]string, 0, block.Stratum.Fanout)
	processes := make([]protocol.ProcessObservation, 0, block.Stratum.Fanout)
	barrierID, faultID := "", ""
	for index := 1; index <= block.Stratum.Fanout; index++ {
		identity := itemIdentity(index)
		items = append(items, identity.WorkItemID)
		started := appendEvent(identity, protocol.EventActivityStarted, protocol.DecisionObserved, root)
		processes = append(processes, protocol.ProcessObservation{
			EventID: started, WorkItemID: identity.WorkItemID, WorkerID: identity.WorkerID, WorkerPID: identity.WorkerPID,
			ProcessIdentity: identity.ProcessIdentity, State: "running",
		})
		parent := started
		if index == 1 && block.Stratum.Probe != protocol.ProbeUnfaulted {
			barrierID = appendEvent(identity, protocol.EventBarrierReached, protocol.DecisionBlocked, started)
			faultID = appendEvent(identity, protocol.EventFaultCommitted, protocol.DecisionAccepted, barrierID)
			parent = appendEvent(identity, protocol.EventRecoveryObserved, protocol.DecisionObserved, faultID)
			processes[0].EventID, processes[0].State = barrierID, "blocked-at-barrier"
		}
		results = append(results, appendEvent(identity, protocol.EventResultAccepted, protocol.DecisionAccepted, parent))
	}
	semantics := protocol.OrchestrationSemantics{}
	continuationIdentity := rootIdentity
	authority := protocol.AuthorityState{
		RunID: runID, CurrentGeneration: 1, CurrentCapabilityHash: base.CapabilityHash,
		Epochs: []protocol.AuthorityEpoch{{Generation: 1, CapabilityHash: base.CapabilityHash, State: protocol.AuthorityActive}},
	}
	destination := protocol.DestinationState{RunID: runID}
	continuationParents := append([]string(nil), results...)
	continuationValue := int64(0)
	switch block.Stratum.Case {
	case protocol.CaseIncrementalPartialReduction:
		contributionEvents := make([]string, 0, len(items))
		for index, item := range items {
			identity := itemIdentity(index + 1)
			eventID := appendEvent(identity, protocol.EventContributionAccepted, protocol.DecisionAccepted, results[index])
			contributionEvents = append(contributionEvents, eventID)
			semantics.Contributions = append(semantics.Contributions, protocol.ContributionObservation{
				EventID: eventID, WorkItemID: item, Ordinal: index + 1, ActivityAttempt: 1, Decision: protocol.DecisionAccepted,
			})
		}
		checkpointParents := contributionEvents
		for _, cardinality := range fixtureReductionThresholds(len(items)) {
			members := append([]string(nil), items[:cardinality]...)
			value := int64(cardinality * (cardinality + 1) / 2)
			eventID := appendEvent(rootIdentity, protocol.EventCheckpointAccepted, protocol.DecisionAccepted, checkpointParents...)
			checkpointID := fmt.Sprintf("%s/checkpoint/%03d", base.LogicalOperationID, cardinality)
			semantics.Checkpoints = append(semantics.Checkpoints, protocol.CheckpointObservation{
				EventID: eventID, CheckpointID: checkpointID, Cardinality: cardinality, Members: members, Value: value,
				ReceiptID: "receipt/" + checkpointID, Decision: protocol.DecisionAccepted, Applied: true,
			})
			checkpointParents = []string{eventID}
		}
		continuationParents = checkpointParents
		continuationValue = int64(len(items) * (len(items) + 1) / 2)
		duplicateApplies := int64(0)
		if block.Stratum.Probe == protocol.ProbeUnsafe {
			first := semantics.Checkpoints[0]
			eventID := appendEvent(rootIdentity, protocol.EventCheckpointAccepted, protocol.DecisionAccepted, continuationParents...)
			first.EventID = eventID
			first.ReceiptID += "/duplicate"
			semantics.Checkpoints = append(semantics.Checkpoints, first)
			continuationParents = []string{eventID}
			duplicateApplies = 2
		}
		semantics.Metrics = fixtureMetrics(block.Stratum.Case, map[string]int64{
			"incorrect_reduction_count": 0, "duplicate_checkpoint_apply_count": duplicateApplies,
		})
	case protocol.CaseQueuedExecutingSupersession:
		commitID := appendEvent(rootIdentity, protocol.EventSupersessionCommitted, protocol.DecisionAccepted, results[0])
		cancelID := appendEvent(rootIdentity, protocol.EventCancellationRequested, protocol.DecisionAccepted, commitID)
		dispositionID := appendEvent(rootIdentity, protocol.EventProcessDisposed, protocol.DecisionObserved, cancelID)
		replacement := itemIdentity(1)
		replacement.Generation = 2
		replacement.CapabilityHash = Hash('b')
		replacement.ActivityID += "/generation-2"
		if topology == protocol.TopologyChildWorkflow {
			replacement.ChildWorkflowID += "/generation-2"
			replacement.ChildRunID += "/generation-2"
		}
		replacementResult := appendEvent(replacement, protocol.EventResultAccepted, protocol.DecisionAccepted, dispositionID)
		continuationParents = append(results[1:], replacementResult)
		continuationIdentity = replacement
		staleActions := int64(0)
		if block.Stratum.Probe == protocol.ProbeUnsafe {
			stale := appendEvent(rootIdentity, protocol.EventProgressAccepted, protocol.DecisionAccepted, commitID)
			continuationParents = append(continuationParents, stale)
			staleActions = 1
		}
		semantics.Supersession = &protocol.SupersessionObservation{
			CommitEventID: commitID, CancellationEventID: cancelID, ProcessDispositionEventID: dispositionID,
			ObsoleteItemID: items[0], ObsoleteGeneration: 1, ObsoleteCapabilityHash: Hash('a'),
			ReplacementGeneration: 2, ReplacementCapabilityHash: Hash('b'),
		}
		authority = protocol.AuthorityState{
			RunID: runID, CurrentGeneration: 2, CurrentCapabilityHash: Hash('b'),
			Epochs: []protocol.AuthorityEpoch{
				{Generation: 1, CapabilityHash: Hash('a'), State: protocol.AuthorityRevoked},
				{Generation: 2, CapabilityHash: Hash('b'), State: protocol.AuthorityActive},
			},
		}
		semantics.Metrics = fixtureMetrics(block.Stratum.Case, map[string]int64{"stale_action_accept_count": staleActions})
	case protocol.CaseDestructiveTransition:
		operationID := base.LogicalOperationID + "/destructive"
		deliveryCount := 1
		if block.Stratum.Probe == protocol.ProbeUnsafe {
			deliveryCount = 2
		}
		var destructiveParents = append([]string(nil), results...)
		for attempt := 1; attempt <= deliveryCount; attempt++ {
			identity := rootIdentity
			identity.ActivityAttempt = attempt
			eventID := appendEvent(identity, protocol.EventDestructiveAccepted, protocol.DecisionAccepted, destructiveParents...)
			receiptID := "receipt/" + operationID
			if block.Stratum.Probe == protocol.ProbeUnsafe {
				receiptID = fmt.Sprintf("%s/attempt-%d", receiptID, attempt)
			}
			semantics.Destructive = ensureDestructive(semantics.Destructive, operationID)
			semantics.Destructive.Deliveries = append(semantics.Destructive.Deliveries, protocol.DestructiveDelivery{
				EventID: eventID, ActivityAttempt: attempt, OperationID: operationID, ExpectedVersion: 0,
				PreviousVersion: uint64(attempt - 1), ResultingVersion: uint64(attempt), ReceiptID: receiptID,
				Decision: protocol.DecisionAccepted, Applied: true,
			})
			destination.Actions = append(destination.Actions, protocol.DestinationAction{
				EventID: eventID, WorkItemID: items[0], LogicalEffectID: operationID, ReceiptID: receiptID,
				Generation: 1, CapabilityHash: Hash('a'), Decision: protocol.DecisionAccepted, Applied: true,
			})
			destructiveParents = []string{eventID}
		}
		semantics.Destructive.FinalVersion = uint64(deliveryCount)
		semantics.Destructive.OutcomeReceiptID = semantics.Destructive.Deliveries[len(semantics.Destructive.Deliveries)-1].ReceiptID
		continuationParents = destructiveParents
		continuationValue = int64(deliveryCount)
		violations := int64(0)
		if deliveryCount > 1 {
			violations = 3
		}
		semantics.Metrics = fixtureMetrics(block.Stratum.Case, map[string]int64{
			"accepted_destructive_apply_count": int64(deliveryCount), "invariant_violation_count": violations,
			"physical_delivery_count": int64(deliveryCount),
		})
	default:
		premature, accepted := int64(0), int64(1)
		if block.Stratum.Probe == protocol.ProbeUnsafe {
			members := append([]string(nil), items[:len(items)-1]...)
			eventID := appendEvent(rootIdentity, protocol.EventContinuationAccepted, protocol.DecisionAccepted, results[:len(results)-1]...)
			semantics.Continuations = append(semantics.Continuations, protocol.ContinuationObservation{
				EventID: eventID, ContinuationID: base.LogicalOperationID + "/premature-continuation", Members: members,
				ReceiptID: "receipt/" + base.LogicalOperationID + "/premature-continuation", Decision: protocol.DecisionAccepted, Applied: true,
			})
			continuationParents = append(continuationParents, eventID)
			premature, accepted = 1, 2
		}
		semantics.Metrics = fixtureMetrics(block.Stratum.Case, map[string]int64{
			"premature_continuation_count": premature, "accepted_continuation_count": accepted,
		})
	}
	continuationID := base.LogicalOperationID + "/continuation"
	continuationEvent := appendEvent(continuationIdentity, protocol.EventContinuationAccepted, protocol.DecisionAccepted, continuationParents...)
	semantics.Continuations = append(semantics.Continuations, protocol.ContinuationObservation{
		EventID: continuationEvent, ContinuationID: continuationID, Members: append([]string(nil), items...), Value: continuationValue,
		ReceiptID: "receipt/" + continuationID, Decision: protocol.DecisionAccepted, Applied: true,
	})
	outcome := appendEvent(continuationIdentity, protocol.EventOutcomeAccepted, protocol.DecisionAccepted, continuationEvent)
	appendEvent(continuationIdentity, protocol.EventAcknowledged, protocol.DecisionAccepted, outcome)
	edges := make([]protocol.LineageEdge, 0, len(events))
	for _, event := range events {
		for _, parent := range event.ParentEventIDs {
			edges = append(edges, protocol.LineageEdge{ParentEventID: parent, ChildEventID: event.EventID})
		}
	}
	manifest := protocol.Manifest{
		ProtocolVersion: protocol.PublicationProtocolVersion, RunID: runID, PairID: block.PairID, ScheduleBlockID: block.ScheduleBlockID,
		TrackerBeadID: base.TrackerBeadID, Topology: topology, Case: block.Stratum.Case, Boundary: block.Stratum.Boundary,
		Probe: block.Stratum.Probe, Fanout: block.Stratum.Fanout, LogicalOperationID: base.LogicalOperationID,
		CreatedAtUTC: "2026-08-09T16:00:00Z", RequiredEvidence: protocol.RequiredEvidenceFiles(),
	}
	fault := protocol.FaultBoundary{RunID: runID}
	if block.Stratum.Probe != protocol.ProbeUnfaulted {
		fault = protocol.FaultBoundary{
			RunID: runID, Injected: true, ExpectedBoundary: block.Stratum.Boundary, BarrierEventID: barrierID,
			FaultEventID: faultID, TargetProcessIdentity: itemIdentity(1).ProcessIdentity,
		}
	}
	nativeExport := json.RawMessage(fmt.Sprintf(`{"run_id":%q,"event_count":%d,"fixture":true}`, runID, len(events)))
	semantics.Metrics = fixtureOrchestrationMetrics(block.Stratum.Case, events, semantics, items, nativeExport)
	prohibited := fixtureProhibitedCount(block.Stratum.Case, semantics)
	nativeHash, err := protocol.NativeExportSHA256(nativeExport)
	if err != nil {
		panic(err)
	}
	bundle := protocol.EvidenceBundle{
		Manifest: manifest, CausalEvents: events, Lineage: protocol.Lineage{RunID: runID, Edges: edges},
		Authority: authority, Destination: destination, Dependency: protocol.DependencyState{RunID: runID},
		Workload: protocol.WorkloadState{
			RunID: runID, RequiredItemIDs: items, AcceptedResultItemIDs: append([]string(nil), items...),
			ExpectedLogicalOutput: "ok", ActualLogicalOutput: "ok", ProhibitedActionCount: prohibited,
			LivenessSatisfied: true, Semantics: semantics,
		},
		FaultBoundary:       fault,
		NativeHistory:       protocol.NativeHistory{RunID: runID, Captured: true, EventCount: len(events), Export: nativeExport, HistorySHA256: nativeHash, ReplayCompatible: true, ReplayWorkerSHA256: Hash('c')},
		ProcessObservations: protocol.ProcessObservations{RunID: runID, Observations: processes},
		EffectiveInput: protocol.EffectiveInput{RunID: runID, PairID: block.PairID, ScheduleBlockID: block.ScheduleBlockID, Topology: topology,
			Case: block.Stratum.Case, Boundary: block.Stratum.Boundary, Probe: block.Stratum.Probe, Fanout: block.Stratum.Fanout,
			WorkloadSHA256: Hash('d'), ActivityOptionsSHA256: Hash('e'), HostEnvelopeSHA256: Hash('f'), AgentBinarySHA256: Hash('1'),
			DestinationProtocolSHA256: Hash('2'), BarrierControllerSHA256: Hash('3'), SourceSHA256: Hash('4')},
		Timing: []protocol.TimingEvent{{Sequence: 1, Kind: protocol.EventInputRegistered, TimestampUTC: "2026-08-09T16:00:00Z"},
			{Sequence: 2, Kind: protocol.EventAcknowledged, TimestampUTC: "2026-08-09T16:00:00.999Z", MonotonicOffsetNS: 999}},
		Execution: protocol.PublicationExecution{ProtocolVersion: protocol.PublicationProtocolVersion, RunID: runID, PairID: block.PairID,
			ScheduleBlockID: block.ScheduleBlockID, Topology: topology, ReplayVerified: true},
	}
	safety := protocol.OutcomePass
	if block.Stratum.Probe == protocol.ProbeUnsafe {
		safety = protocol.OutcomeFail
	}
	bundle.Verdict = protocol.Verdict{
		ProtocolVersion: protocol.PublicationProtocolVersion, RunID: runID, Admission: protocol.AdmissionValid,
		Correctness: protocol.OutcomePass, Safety: safety, Liveness: protocol.OutcomePass,
		Diagnosability: protocol.OutcomePass, EfficiencyEligible: safety == protocol.OutcomePass, Oracle: protocol.OracleProtocolVersion,
	}
	return bundle
}

func ensureDestructive(value *protocol.DestructiveObservation, operationID string) *protocol.DestructiveObservation {
	if value != nil {
		return value
	}
	return &protocol.DestructiveObservation{OperationID: operationID, ExpectedPriorVersion: 0}
}

func fixtureReductionThresholds(count int) []int {
	values := []int{1, (count + 3) / 4, (count + 1) / 2, (3*count + 3) / 4, count}
	result := values[:0]
	for _, value := range values {
		if len(result) == 0 || result[len(result)-1] != value {
			result = append(result, value)
		}
	}
	return result
}

func fixtureMetrics(benchmarkCase protocol.CaseID, overrides map[string]int64) []protocol.Metric {
	specs := protocol.MetricsForCase(benchmarkCase)
	for index := range specs {
		if value, exists := overrides[specs[index].Name]; exists {
			specs[index].Value = value
		}
	}
	return specs
}

func fixtureOrchestrationMetrics(
	benchmarkCase protocol.CaseID,
	events []protocol.CausalEvent,
	semantics protocol.OrchestrationSemantics,
	items []string,
	nativeExport json.RawMessage,
) []protocol.Metric {
	overrides := make(map[string]int64, len(semantics.Metrics))
	for _, metric := range semantics.Metrics {
		overrides[metric.Name] = metric.Value
	}
	historyBytes, err := protocol.NativeExportByteCount(nativeExport)
	if err != nil {
		panic(err)
	}
	endToEnd := fixtureOrchestrationSequenceDeltaMS(events, 1, uint64(len(events)))
	switch benchmarkCase {
	case protocol.CaseJoinBarrier:
		overrides["join_lag_after_last_required_result_ms"] = fixtureOrchestrationSequenceDeltaMS(events,
			fixtureLastKindSequence(events, protocol.EventResultAccepted), fixtureFirstKindSequence(events, protocol.EventContinuationAccepted))
		overrides["end_to_end_latency_ms"] = endToEnd
		overrides["history_bytes_per_item"] = int64(historyBytes / len(items))
	case protocol.CaseIncrementalPartialReduction:
		overrides["time_to_first_reduction_ms"] = fixtureOrchestrationSequenceDeltaMS(events, 1,
			fixtureFirstKindSequence(events, protocol.EventCheckpointAccepted))
		overrides["final_makespan_ms"] = endToEnd
		overrides["history_bytes_per_item"] = int64(historyBytes / len(items))
	case protocol.CaseQueuedExecutingSupersession:
		observation := semantics.Supersession
		commit := fixtureEventSequence(events, observation.CommitEventID)
		cancellation := fixtureEventSequence(events, observation.CancellationEventID)
		disposition := fixtureEventSequence(events, observation.ProcessDispositionEventID)
		overrides["cancellation_propagation_ms"] = fixtureOrchestrationSequenceDeltaMS(events, commit, disposition)
		overrides["replacement_recovery_ms"] = fixtureOrchestrationSequenceDeltaMS(events, commit, fixtureReplacementResultSequence(events))
		overrides["wasted_compute_ms"] = fixtureOrchestrationSequenceDeltaMS(events, cancellation, disposition)
		overrides["wasted_cost_units"] = 0
	case protocol.CaseDestructiveTransition:
		overrides["recovery_delay_ms"] = fixtureOrchestrationSequenceDeltaMS(events,
			fixtureFirstKindSequence(events, protocol.EventFaultCommitted), fixtureFirstKindSequence(events, protocol.EventRecoveryObserved))
		overrides["end_to_end_latency_ms"] = endToEnd
	}
	return fixtureMetrics(benchmarkCase, overrides)
}

func fixtureFirstKindSequence(events []protocol.CausalEvent, kind string) uint64 {
	for _, event := range events {
		if event.Kind == kind {
			return event.Sequence
		}
	}
	return 0
}

func fixtureLastKindSequence(events []protocol.CausalEvent, kind string) uint64 {
	for index := len(events) - 1; index >= 0; index-- {
		if events[index].Kind == kind {
			return events[index].Sequence
		}
	}
	return 0
}

func fixtureReplacementResultSequence(events []protocol.CausalEvent) uint64 {
	for _, event := range events {
		if event.Kind == protocol.EventResultAccepted && event.Generation == 2 {
			return event.Sequence
		}
	}
	return 0
}

func fixtureOrchestrationSequenceDeltaMS(events []protocol.CausalEvent, first, last uint64) int64 {
	if first == 0 || last == 0 || last < first {
		return 0
	}
	return (events[last-1].MonotonicOffsetNS - events[first-1].MonotonicOffsetNS) / int64(1_000_000)
}

func fixtureProhibitedCount(benchmarkCase protocol.CaseID, semantics protocol.OrchestrationSemantics) int {
	switch benchmarkCase {
	case protocol.CaseJoinBarrier:
		return int(fixtureMetricValue(semantics.Metrics, "premature_continuation_count"))
	case protocol.CaseIncrementalPartialReduction:
		return int(fixtureMetricValue(semantics.Metrics, "duplicate_checkpoint_apply_count"))
	case protocol.CaseQueuedExecutingSupersession:
		return int(fixtureMetricValue(semantics.Metrics, "stale_action_accept_count"))
	case protocol.CaseDestructiveTransition:
		return int(fixtureMetricValue(semantics.Metrics, "invariant_violation_count"))
	default:
		return 0
	}
}

func fixtureMetricValue(metrics []protocol.Metric, name string) int64 {
	for _, metric := range metrics {
		if metric.Name == name {
			return metric.Value
		}
	}
	return 0
}

func Hash(character byte) string {
	value := make([]byte, 64)
	for index := range value {
		value[index] = character
	}
	return string(value)
}
