package testfixture

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
)

const fixtureMillisecond = int64(time.Millisecond)

type recoveryFixtureBuilder struct {
	base   protocol.Identity
	events []protocol.CausalEvent
	offset int64
}

func (b *recoveryFixtureBuilder) append(identity protocol.Identity, kind, decision string, parents ...string) string {
	return b.appendAfter(identity, 1, kind, decision, parents...)
}

func (b *recoveryFixtureBuilder) appendAfter(identity protocol.Identity, delayMS int64, kind, decision string, parents ...string) string {
	if len(b.events) == 0 {
		delayMS = 0
	}
	b.offset += delayMS * fixtureMillisecond
	sequence := len(b.events) + 1
	eventID := fmt.Sprintf("event-%05d", sequence)
	timestamp := time.Date(2026, 8, 9, 16, 0, 0, 0, time.UTC).Add(time.Duration(b.offset)).Format(time.RFC3339Nano)
	b.events = append(b.events, protocol.CausalEvent{
		Identity: identity, Sequence: uint64(sequence), EventID: eventID, ParentEventIDs: parents,
		TimestampUTC: timestamp, MonotonicOffsetNS: b.offset, Kind: kind, Decision: decision,
	})
	return eventID
}

func (b *recoveryFixtureBuilder) appendDetailed(
	identity protocol.Identity,
	kind, decision string,
	details map[string]string,
	parents ...string,
) string {
	eventID := b.append(identity, kind, decision, parents...)
	b.events[len(b.events)-1].Details = details
	return eventID
}

func recoveryBundle(block protocol.PairBlock, topology protocol.Topology) protocol.EvidenceBundle {
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
		identity.WorkerPID = 2000 + index
		identity.ProcessIdentity = fmt.Sprintf("pid:%d/start:item-%03d", identity.WorkerPID, index)
		if topology == protocol.TopologyChildWorkflow {
			identity.ChildWorkflowID = fmt.Sprintf("child-%03d", index)
			identity.ChildRunID = fmt.Sprintf("child-run-%03d", index)
		}
		return identity
	}
	builder := &recoveryFixtureBuilder{base: base}
	rootIdentity := itemIdentity(1)
	root := builder.append(rootIdentity, protocol.EventInputRegistered, protocol.DecisionObserved)
	if block.Stratum.Case == protocol.CaseOutageBacklogHerdRecovery {
		root = builder.appendDetailed(rootIdentity, protocol.EventDependencyStateChanged, protocol.DecisionAccepted,
			map[string]string{"state": "outage"}, root)
	}

	items := make([]string, block.Stratum.Fanout)
	for index := range items {
		items[index] = fmt.Sprintf("item-%03d", index+1)
	}
	schedules := make([]string, block.Stratum.Fanout)
	terminals := make([]string, block.Stratum.Fanout)
	recoveryItems := make([]protocol.RecoveryItemObservation, block.Stratum.Fanout)
	accepted := make([]string, 0, block.Stratum.Fanout)
	processes := make([]protocol.ProcessObservation, 0, block.Stratum.Fanout+1)
	actions := make([]protocol.DestinationAction, 0, block.Stratum.Fanout+1)
	requests := make([]protocol.DependencyRequest, 0, block.Stratum.Fanout*5)
	barrierID, faultID := "", ""
	outageRestoredID := ""
	currentGeneration, currentCapability := uint64(1), Hash('a')

	processItem := func(index int) {
		ordinal := index + 1
		identity := itemIdentity(ordinal)
		role := "healthy"
		poison := block.Stratum.Case == protocol.CasePoisonWorkIsolation && ordinal == 1
		if poison {
			role = "poison"
		}
		if block.Stratum.Case == protocol.CaseSilentProgress && ordinal == 1 {
			role = "wedged"
		}
		if block.Stratum.Case == protocol.CaseSilentProgress && ordinal == 2 {
			role = "declared-wait"
		}
		startedParents := []string{schedules[index]}
		if outageRestoredID != "" {
			startedParents = append(startedParents, outageRestoredID)
		}
		started := builder.append(identity, protocol.EventActivityStarted, protocol.DecisionObserved, startedParents...)
		processEvent, processState := started, "running"
		parent := started
		if !poison {
			processEvent = builder.append(identity, protocol.EventProcessStarted, protocol.DecisionObserved, started)
			parent = processEvent
		}

		if ordinal == 1 && block.Stratum.Case == protocol.CaseSilentProgress {
			progress := builder.append(identity, protocol.EventProgressAccepted, protocol.DecisionAccepted, parent)
			parent = progress
			if block.Stratum.Probe != protocol.ProbeUnfaulted {
				barrierID = builder.append(identity, protocol.EventBarrierReached, protocol.DecisionBlocked, progress)
				faultID = builder.append(identity, protocol.EventFaultCommitted, protocol.DecisionAccepted, barrierID)
				processEvent, processState = barrierID, "blocked-at-barrier"
				targetLatencyMS := int64(3000)
				if block.Stratum.Probe == protocol.ProbeUnsafe {
					targetLatencyMS = 5201
				}
				detection := builder.appendAfter(identity, targetLatencyMS-2, protocol.EventAuthorityRevoked, protocol.DecisionAccepted, faultID)
				replacement := identity
				replacement.Generation = 2
				replacement.CapabilityHash = Hash('b')
				replacement.ActivityID += "/generation-2"
				replacement.WorkerPID += 1000
				replacement.ProcessIdentity += "/generation-2"
				if topology == protocol.TopologyChildWorkflow {
					replacement.ChildWorkflowID += "/generation-2"
					replacement.ChildRunID += "/generation-2"
				}
				recovered := builder.append(replacement, protocol.EventRecoveryObserved, protocol.DecisionObserved, detection)
				replacementStarted := builder.append(replacement, protocol.EventActivityStarted, protocol.DecisionObserved, recovered)
				replacementProcess := builder.append(replacement, protocol.EventProcessStarted, protocol.DecisionObserved, replacementStarted)
				processes = append(processes, protocol.ProcessObservation{
					EventID: replacementProcess, WorkItemID: replacement.WorkItemID, WorkerID: replacement.WorkerID,
					WorkerPID: replacement.WorkerPID, ProcessIdentity: replacement.ProcessIdentity, State: "replacement-running",
				})
				identity, parent = replacement, replacementProcess
				currentGeneration, currentCapability = 2, Hash('b')
			}
		} else if ordinal == 1 && block.Stratum.Probe != protocol.ProbeUnfaulted {
			barrierID = builder.append(identity, protocol.EventBarrierReached, protocol.DecisionBlocked, parent)
			faultID = builder.append(identity, protocol.EventFaultCommitted, protocol.DecisionAccepted, barrierID)
			parent = builder.append(identity, protocol.EventRecoveryObserved, protocol.DecisionObserved, faultID)
			processEvent, processState = barrierID, "blocked-at-barrier"
		}
		processes = append(processes, protocol.ProcessObservation{
			EventID: processEvent, WorkItemID: itemIdentity(ordinal).WorkItemID, WorkerID: itemIdentity(ordinal).WorkerID,
			WorkerPID: itemIdentity(ordinal).WorkerPID, ProcessIdentity: itemIdentity(ordinal).ProcessIdentity, State: processState,
		})

		activityAttempts, agentProcesses, acceptedEffects, acceptedResults, costUnits := 1, 1, 1, 1, int64(0)
		requestCount, retryOwner, concurrent := 0, "fixture", 1
		switch block.Stratum.Case {
		case protocol.CaseCrashRecoveryBoundaries:
			if ordinal == 1 && block.Stratum.Probe == protocol.ProbeUnsafe {
				activityAttempts, agentProcesses, acceptedEffects, acceptedResults = 2, 2, 2, 2
				second := identity
				second.ActivityAttempt = 2
				second.WorkerPID += 2000
				second.ProcessIdentity += "/attempt-2"
				secondStart := builder.append(second, protocol.EventActivityStarted, protocol.DecisionObserved, parent)
				secondProcess := builder.append(second, protocol.EventProcessStarted, protocol.DecisionObserved, secondStart)
				processes = append(processes, protocol.ProcessObservation{
					EventID: secondProcess, WorkItemID: second.WorkItemID, WorkerID: second.WorkerID,
					WorkerPID: second.WorkerPID, ProcessIdentity: second.ProcessIdentity, State: "retry-running",
				})
				parent = secondProcess
			}
		case protocol.CaseLayeredRetryAmplification:
			requestCount = 1
			if block.Stratum.Probe == protocol.ProbeProtected {
				requestCount = 4
			}
			if block.Stratum.Probe == protocol.ProbeUnsafe {
				requestCount = 5
			}
			retryOwner = "activity-owned"
		case protocol.CaseOutageBacklogHerdRecovery:
			requestCount, retryOwner = 1, "outage-catchup"
			if block.Stratum.Probe == protocol.ProbeProtected {
				concurrent = 2
			}
			if block.Stratum.Probe == protocol.ProbeUnsafe {
				concurrent = 3
			}
		case protocol.CasePoisonWorkIsolation:
			requestCount, retryOwner = 1, "activity-owned"
			if poison {
				activityAttempts, agentProcesses, acceptedEffects, acceptedResults = 3, 0, 0, 0
				requestCount = 3
				if block.Stratum.Probe == protocol.ProbeUnsafe {
					activityAttempts, requestCount = 5, 5
				}
			}
		case protocol.CaseSilentProgress:
			if ordinal == 1 && block.Stratum.Probe != protocol.ProbeUnfaulted {
				agentProcesses = 2
			}
		}

		previousRequest := ""
		for retry := 1; retry <= requestCount; retry++ {
			requestIdentity := identity
			if block.Stratum.Case == protocol.CasePoisonWorkIsolation && poison {
				requestIdentity.ActivityAttempt = retry
				if retry > 1 {
					parent = builder.append(requestIdentity, protocol.EventActivityStarted, protocol.DecisionObserved, parent)
				}
			}
			dependencyStart := builder.append(requestIdentity, protocol.EventDependencyStarted, protocol.DecisionObserved, parent)
			dependencyFinish := builder.append(requestIdentity, protocol.EventDependencyFinished, protocol.DecisionObserved, dependencyStart)
			outcome := "ok"
			if poison {
				outcome = "permanent_failure"
			}
			requestID := fmt.Sprintf("request/%s/%02d", items[index], retry)
			requests = append(requests, protocol.DependencyRequest{
				RequestID: requestID, EventID: dependencyFinish, StartedEventID: dependencyStart, ParentRequestID: previousRequest,
				WorkItemID: items[index], Attempt: requestIdentity.ActivityAttempt, RetryOrdinal: retry, RetryOwner: retryOwner,
				Outcome: outcome, CostUnits: 1, StartedOffsetNS: eventOffset(builder.events, dependencyStart),
				FinishedOffsetNS: eventOffset(builder.events, dependencyFinish), ConcurrentAtStart: concurrent,
			})
			previousRequest, parent, costUnits = requestID, dependencyFinish, costUnits+1
		}

		for effect := 1; effect <= acceptedEffects; effect++ {
			effectIdentity := identity
			if effect > 1 {
				effectIdentity.ActivityAttempt = effect
			}
			effectEvent := builder.append(effectIdentity, protocol.EventEffectAccepted, protocol.DecisionAccepted, parent)
			effectID := base.LogicalOperationID + "/effect/" + items[index]
			receiptID := fmt.Sprintf("receipt/%s/%02d", effectID, effect)
			actions = append(actions, protocol.DestinationAction{
				EventID: effectEvent, WorkItemID: items[index], LogicalEffectID: effectID, ReceiptID: receiptID,
				Generation: effectIdentity.Generation, CapabilityHash: effectIdentity.CapabilityHash,
				Decision: protocol.DecisionAccepted, Applied: true,
			})
			parent = effectEvent
		}

		disposition := protocol.RecoveryDispositionSucceeded
		if poison {
			terminal := builder.append(identity, protocol.EventItemQuarantined, protocol.DecisionAccepted, parent)
			terminals[index], disposition = terminal, protocol.RecoveryDispositionQuarantined
		} else {
			for result := 1; result <= acceptedResults; result++ {
				resultIdentity := identity
				if result > 1 {
					resultIdentity.ActivityAttempt = result
				}
				parent = builder.append(resultIdentity, protocol.EventResultAccepted, protocol.DecisionAccepted, parent)
			}
			terminals[index] = parent
			accepted = append(accepted, items[index])
		}
		recoveryItems[index] = protocol.RecoveryItemObservation{
			WorkItemID: items[index], Role: role, Poison: poison, Admitted: true, Disposition: disposition,
			ScheduleEventID: schedules[index], StartEventID: started, TerminalEventID: terminals[index],
			ActivityAttempts: activityAttempts, AgentProcesses: agentProcesses, AcceptedEffects: acceptedEffects,
			AcceptedResults: acceptedResults, CostUnits: costUnits,
		}
	}

	if block.Stratum.Case == protocol.CaseOutageBacklogHerdRecovery {
		for index := range block.Stratum.Fanout {
			schedules[index] = builder.append(itemIdentity(index+1), protocol.EventActivityScheduled, protocol.DecisionObserved, root)
		}
		backlogID := builder.appendDetailed(rootIdentity, protocol.EventDependencyStateChanged, protocol.DecisionAccepted,
			map[string]string{"state": "exact-backlog", "cardinality": fmt.Sprint(block.Stratum.Fanout)}, schedules...)
		outageRestoredID = builder.appendDetailed(rootIdentity, protocol.EventDependencyStateChanged, protocol.DecisionAccepted,
			map[string]string{"state": "restored"}, backlogID)
		for index := range block.Stratum.Fanout {
			processItem(index)
		}
	} else {
		window := block.Stratum.Fanout
		if block.Stratum.Case == protocol.CaseBackpressureOverload && block.Stratum.Probe != protocol.ProbeUnsafe {
			window = 8
		}
		for first := 0; first < block.Stratum.Fanout; first += window {
			last := first + window
			if last > block.Stratum.Fanout {
				last = block.Stratum.Fanout
			}
			for index := first; index < last; index++ {
				schedules[index] = builder.append(itemIdentity(index+1), protocol.EventActivityScheduled, protocol.DecisionObserved, root)
			}
			for index := first; index < last; index++ {
				processItem(index)
			}
		}
	}

	caller := itemIdentity(1)
	caller.Generation, caller.CapabilityHash = currentGeneration, currentCapability
	if topology == protocol.TopologyChildWorkflow && currentGeneration == 2 {
		caller.ChildWorkflowID += "/generation-2"
		caller.ChildRunID += "/generation-2"
	}
	caller.ActivityID = "caller/outcome-acknowledgement"
	outcomeID := builder.append(caller, protocol.EventOutcomeAccepted, protocol.DecisionAccepted, terminals...)
	acknowledgementID := builder.append(caller, protocol.EventAcknowledged, protocol.DecisionAccepted, outcomeID)

	authority := protocol.AuthorityState{
		RunID: runID, CurrentGeneration: currentGeneration, CurrentCapabilityHash: currentCapability,
		Epochs: []protocol.AuthorityEpoch{{Generation: 1, CapabilityHash: Hash('a'), State: protocol.AuthorityActive}},
	}
	if currentGeneration == 2 {
		authority.Epochs = []protocol.AuthorityEpoch{
			{Generation: 1, CapabilityHash: Hash('a'), State: protocol.AuthorityRevoked},
			{Generation: 2, CapabilityHash: Hash('b'), State: protocol.AuthorityActive},
		}
	}
	fault := protocol.FaultBoundary{RunID: runID}
	if block.Stratum.Probe != protocol.ProbeUnfaulted {
		fault = protocol.FaultBoundary{
			RunID: runID, Injected: true, ExpectedBoundary: block.Stratum.Boundary, BarrierEventID: barrierID,
			FaultEventID: faultID, TargetProcessIdentity: itemIdentity(1).ProcessIdentity,
		}
	}

	nativeExport := json.RawMessage(fmt.Sprintf(`{"run_id":%q,"event_count":%d,"fixture":true}`, runID, len(builder.events)))
	recovery := protocol.RecoveryDynamics{
		Items: recoveryItems, ParentAcknowledgementEventID: acknowledgementID, Bounds: recoveryFixtureBounds(),
	}
	recovery.Metrics = recoveryFixtureMetrics(block.Stratum.Case, builder.events, recoveryItems, requests, nativeExport)
	prohibited := recoveryFixtureViolationCount(block.Stratum.Case, builder.events, recoveryItems, requests, recovery.Metrics)
	expectedOutput := fmt.Sprintf("completed:%d", block.Stratum.Fanout)
	if block.Stratum.Case == protocol.CasePoisonWorkIsolation {
		expectedOutput = fmt.Sprintf("healthy:%d/quarantined:1", block.Stratum.Fanout-1)
	}
	nativeHash, err := protocol.NativeExportSHA256(nativeExport)
	if err != nil {
		panic(err)
	}
	edges := make([]protocol.LineageEdge, 0, len(builder.events))
	for _, event := range builder.events {
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
	safety := protocol.OutcomePass
	if prohibited > 0 {
		safety = protocol.OutcomeFail
	}
	bundle := protocol.EvidenceBundle{
		Manifest: manifest, CausalEvents: builder.events, Lineage: protocol.Lineage{RunID: runID, Edges: edges},
		Authority: authority, Destination: protocol.DestinationState{RunID: runID, Actions: actions},
		Dependency: protocol.DependencyState{RunID: runID, Requests: requests},
		Workload: protocol.WorkloadState{
			RunID: runID, RequiredItemIDs: items, AcceptedResultItemIDs: accepted,
			ExpectedLogicalOutput: expectedOutput, ActualLogicalOutput: expectedOutput,
			ProhibitedActionCount: prohibited, LivenessSatisfied: true, Recovery: &recovery,
		},
		FaultBoundary: fault,
		NativeHistory: protocol.NativeHistory{
			RunID: runID, Captured: true, EventCount: len(builder.events), Export: nativeExport, HistorySHA256: nativeHash,
			ReplayCompatible: true, ReplayWorkerSHA256: Hash('c'),
		},
		ProcessObservations: protocol.ProcessObservations{RunID: runID, Observations: processes},
		EffectiveInput: protocol.EffectiveInput{
			RunID: runID, PairID: block.PairID, ScheduleBlockID: block.ScheduleBlockID, Topology: topology,
			Case: block.Stratum.Case, Boundary: block.Stratum.Boundary, Probe: block.Stratum.Probe, Fanout: block.Stratum.Fanout,
			WorkloadSHA256: Hash('d'), ActivityOptionsSHA256: Hash('e'), HostEnvelopeSHA256: Hash('f'), AgentBinarySHA256: Hash('1'),
			DestinationProtocolSHA256: Hash('2'), BarrierControllerSHA256: Hash('3'), SourceSHA256: Hash('4'),
		},
		Verdict: protocol.Verdict{
			ProtocolVersion: protocol.PublicationProtocolVersion, RunID: runID, Admission: protocol.AdmissionValid,
			Correctness: protocol.OutcomePass, Safety: safety, Liveness: protocol.OutcomePass, Diagnosability: protocol.OutcomePass,
			EfficiencyEligible: safety == protocol.OutcomePass, Oracle: protocol.OracleProtocolVersion,
		},
		Timing: []protocol.TimingEvent{
			{Sequence: 1, Kind: protocol.EventInputRegistered, TimestampUTC: builder.events[0].TimestampUTC},
			{Sequence: 2, Kind: protocol.EventAcknowledged, TimestampUTC: builder.events[len(builder.events)-1].TimestampUTC, MonotonicOffsetNS: builder.offset},
		},
		Execution: protocol.PublicationExecution{
			ProtocolVersion: protocol.PublicationProtocolVersion, RunID: runID, PairID: block.PairID,
			ScheduleBlockID: block.ScheduleBlockID, Topology: topology, ReplayVerified: true,
		},
	}
	return bundle
}

func eventOffset(events []protocol.CausalEvent, eventID string) int64 {
	for _, event := range events {
		if event.EventID == eventID {
			return event.MonotonicOffsetNS
		}
	}
	return 0
}

func recoveryFixtureBounds() []protocol.Metric {
	return []protocol.Metric{
		{Name: "requests_per_item_max", Unit: "count", Value: 4},
		{Name: "retry_concurrency_max", Unit: "count", Value: 2},
		{Name: "in_flight_max", Unit: "count", Value: 8},
		{Name: "poison_attempts_max", Unit: "count", Value: 3},
		{Name: "progress_deadline_ms", Unit: "ms", Value: 5000},
	}
}

func recoveryFixtureMetrics(
	benchmarkCase protocol.CaseID,
	events []protocol.CausalEvent,
	items []protocol.RecoveryItemObservation,
	requests []protocol.DependencyRequest,
	nativeExport json.RawMessage,
) []protocol.Metric {
	overrides := make(map[string]int64)
	sumProcesses, duplicateEffects, duplicateResults, attempts, cost := int64(0), int64(0), int64(0), int64(0), int64(0)
	for _, item := range items {
		sumProcesses += int64(item.AgentProcesses)
		attempts += int64(item.ActivityAttempts)
		if item.AcceptedEffects > 1 {
			duplicateEffects += int64(item.AcceptedEffects - 1)
		}
		if item.AcceptedResults > 1 {
			duplicateResults += int64(item.AcceptedResults - 1)
		}
	}
	for _, request := range requests {
		cost += request.CostUnits
	}
	historyBytes, err := protocol.NativeExportByteCount(nativeExport)
	if err != nil {
		panic(err)
	}
	overrides["history_event_count"] = int64(len(events))
	overrides["history_bytes"] = int64(historyBytes)
	endToEnd := fixtureSequenceDeltaMS(events, 1, uint64(len(events)))
	switch benchmarkCase {
	case protocol.CaseCrashRecoveryBoundaries:
		overrides["agent_process_count"] = sumProcesses
		overrides["duplicate_effect_count"] = duplicateEffects
		overrides["duplicate_result_count"] = duplicateResults
		overrides["time_to_recovery_ms"] = fixtureRecoveryDelayMS(events)
		overrides["schedule_to_start_ms"] = fixtureMaximumScheduleToStartMS(events, items, false)
		overrides["activity_attempt_count"] = attempts
		overrides["workflow_task_count"] = 0
	case protocol.CaseLayeredRetryAmplification:
		overrides["physical_request_count"] = int64(len(requests))
		overrides["amplification_factor"] = int64(len(requests)) * 1000 / int64(len(items))
		overrides["retry_delay_ms"] = fixtureSumRequestMetric(requests, func(request protocol.DependencyRequest) int64 {
			return request.RetryDelayMS
		})
		overrides["active_execution_ms"] = fixtureSumRequestMetric(requests, func(request protocol.DependencyRequest) int64 {
			return request.ServiceMS
		})
		overrides["recovery_delay_ms"] = fixtureRecoveryDelayMS(events)
		overrides["cost_units"] = cost
	case protocol.CaseOutageBacklogHerdRecovery:
		peak := int64(0)
		for _, request := range requests {
			if request.RetryOwner == "outage-catchup" && int64(request.ConcurrentAtStart) > peak {
				peak = int64(request.ConcurrentAtStart)
			}
		}
		restoredOffset := fixtureEventKindOffset(events, protocol.EventDependencyStateChanged, "state", "restored")
		drains := fixtureBacklogDrainDurationsMS(events, items, restoredOffset)
		overrides["peak_qps"] = fixturePeakRequestQPS(requests, restoredOffset, 10)
		overrides["peak_retry_concurrency"] = peak
		overrides["backlog_integral_ms"] = fixtureSumInt64(drains)
		overrides["backlog_drain_p50_ms"] = fixturePercentile(drains, 50)
		overrides["backlog_drain_p90_ms"] = fixturePercentile(drains, 90)
		overrides["recovery_delay_ms"] = fixtureRecoveryDelayMS(events)
		overrides["duplicate_effect_count"] = duplicateEffects
	case protocol.CaseBackpressureOverload:
		durationMS := endToEnd
		if durationMS < 1 {
			durationMS = 1
		}
		rejected := 0
		for _, item := range items {
			if !item.Admitted {
				rejected++
			}
		}
		overrides["schedule_to_start_ms"] = fixtureMaximumScheduleToStartMS(events, items, false)
		overrides["queue_age_ms"] = overrides["schedule_to_start_ms"]
		overrides["end_to_end_latency_ms"] = endToEnd
		overrides["throughput_per_second"] = int64(len(items)) * 1_000_000 / durationMS
		overrides["admission_rejection_fraction"] = int64(rejected) * 1000 / int64(len(items))
		overrides["peak_in_flight_count"] = fixturePeakInFlight(events, items)
		overrides["history_events_per_item"] = int64(len(events)) * 1000 / int64(len(items))
		overrides["history_bytes_per_item"] = int64(historyBytes) / int64(len(items))
	case protocol.CasePoisonWorkIsolation:
		for _, item := range items {
			if item.Poison {
				overrides["poison_attempt_count"] = int64(item.ActivityAttempts)
				overrides["poison_cost_units"] = item.CostUnits
				overrides["poison_capacity_ms"] = fixtureItemDurationMS(events, item.StartEventID, item.TerminalEventID)
			}
		}
		overrides["healthy_schedule_to_start_ms"] = fixtureMaximumScheduleToStartMS(events, items, true)
		overrides["healthy_task_latency_ms"] = fixtureMaximumHealthyTaskLatencyMS(events, items)
		overrides["healthy_completion_fraction"] = fixtureHealthyCompletionFractionMilli(items)
	case protocol.CaseSilentProgress:
		progress, detection := fixtureSilentDetectionSequences(events)
		if progress > 0 && detection > 0 {
			overrides["failure_detection_latency_ms"] = fixtureSequenceDeltaMS(events, progress, detection)
		}
		overrides["replacement_recovery_ms"] = fixtureReplacementRecoveryMS(events, items, detection)
		overrides["healthy_task_latency_ms"] = fixtureMaximumHealthyTaskLatencyMS(events, items)
		overrides["end_to_end_latency_ms"] = endToEnd
	}
	return recoveryMetricSpecs(benchmarkCase, overrides)
}

func fixtureMaximumScheduleToStartMS(events []protocol.CausalEvent, items []protocol.RecoveryItemObservation, healthyOnly bool) int64 {
	maximum := int64(0)
	for _, item := range items {
		if !item.Admitted || healthyOnly && item.Poison {
			continue
		}
		if value := fixtureItemDurationMS(events, item.ScheduleEventID, item.StartEventID); value > maximum {
			maximum = value
		}
	}
	return maximum
}

func fixtureMaximumHealthyTaskLatencyMS(events []protocol.CausalEvent, items []protocol.RecoveryItemObservation) int64 {
	maximum := int64(0)
	for _, item := range items {
		if !item.Admitted || item.Poison || item.Role == "wedged" {
			continue
		}
		if value := fixtureItemDurationMS(events, item.StartEventID, item.TerminalEventID); value > maximum {
			maximum = value
		}
	}
	return maximum
}

func fixtureHealthyCompletionFractionMilli(items []protocol.RecoveryItemObservation) int64 {
	healthy, completed := 0, 0
	for _, item := range items {
		if item.Poison {
			continue
		}
		healthy++
		if item.Disposition == protocol.RecoveryDispositionSucceeded {
			completed++
		}
	}
	return int64(completed) * 1000 / int64(healthy)
}

func fixtureRecoveryDelayMS(events []protocol.CausalEvent) int64 {
	return fixtureSequenceDeltaMS(events, fixtureFirstEventSequence(events, protocol.EventFaultCommitted),
		fixtureFirstEventSequence(events, protocol.EventRecoveryObserved))
}

func fixtureFirstEventSequence(events []protocol.CausalEvent, kind string) uint64 {
	for _, event := range events {
		if event.Kind == kind {
			return event.Sequence
		}
	}
	return 0
}

func fixtureItemDurationMS(events []protocol.CausalEvent, firstID, lastID string) int64 {
	return fixtureSequenceDeltaMS(events, fixtureEventSequence(events, firstID), fixtureEventSequence(events, lastID))
}

func fixtureSequenceDeltaMS(events []protocol.CausalEvent, first, last uint64) int64 {
	if first == 0 || last == 0 || last < first {
		return 0
	}
	return (events[last-1].MonotonicOffsetNS - events[first-1].MonotonicOffsetNS) / fixtureMillisecond
}

func fixtureEventKindOffset(events []protocol.CausalEvent, kind, detailKey, detailValue string) int64 {
	for _, event := range events {
		if event.Kind == kind && event.Details[detailKey] == detailValue {
			return event.MonotonicOffsetNS
		}
	}
	return 0
}

func fixtureBacklogDrainDurationsMS(events []protocol.CausalEvent, items []protocol.RecoveryItemObservation, restoredOffset int64) []int64 {
	durations := make([]int64, 0, len(items))
	for _, item := range items {
		terminal := events[fixtureEventSequence(events, item.TerminalEventID)-1].MonotonicOffsetNS
		value := int64(0)
		if terminal > restoredOffset {
			value = (terminal - restoredOffset) / fixtureMillisecond
		}
		durations = append(durations, value)
	}
	slices.Sort(durations)
	return durations
}

func fixturePeakRequestQPS(requests []protocol.DependencyRequest, restoredOffset, windowMS int64) int64 {
	starts := make([]int64, 0, len(requests))
	for _, request := range requests {
		if request.StartedOffsetNS >= restoredOffset {
			starts = append(starts, request.StartedOffsetNS)
		}
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	peak := 0
	windowNS := windowMS * fixtureMillisecond
	for first, last := 0, 0; first < len(starts); first++ {
		for last < len(starts) && starts[last] < starts[first]+windowNS {
			last++
		}
		if count := last - first; count > peak {
			peak = count
		}
	}
	return int64(peak) * 1000 / windowMS
}

func fixturePercentile(values []int64, percent int) int64 {
	if len(values) == 0 {
		return 0
	}
	index := (len(values)*percent + 99) / 100
	if index < 1 {
		index = 1
	}
	return values[index-1]
}

func fixtureSumInt64(values []int64) int64 {
	result := int64(0)
	for _, value := range values {
		result += value
	}
	return result
}

func fixtureSumRequestMetric(requests []protocol.DependencyRequest, selectValue func(protocol.DependencyRequest) int64) int64 {
	result := int64(0)
	for _, request := range requests {
		result += selectValue(request)
	}
	return result
}

func fixtureSilentDetectionSequences(events []protocol.CausalEvent) (uint64, uint64) {
	progress, detection := uint64(0), uint64(0)
	for _, event := range events {
		if event.WorkItemID != "item-001" {
			continue
		}
		if progress == 0 && event.Kind == protocol.EventProgressAccepted {
			progress = event.Sequence
		}
		if progress > 0 && event.Sequence > progress && detection == 0 &&
			(event.Kind == protocol.EventAuthorityRevoked || event.Kind == protocol.EventRecoveryObserved) {
			detection = event.Sequence
		}
	}
	return progress, detection
}

func fixtureReplacementRecoveryMS(events []protocol.CausalEvent, items []protocol.RecoveryItemObservation, detection uint64) int64 {
	if detection == 0 {
		return 0
	}
	for _, item := range items {
		if item.Role != "wedged" {
			continue
		}
		terminal := fixtureEventSequence(events, item.TerminalEventID)
		if terminal > detection && events[terminal-1].Generation > events[detection-1].Generation {
			return fixtureSequenceDeltaMS(events, detection, terminal)
		}
	}
	return 0
}

func recoveryMetricSpecs(benchmarkCase protocol.CaseID, overrides map[string]int64) []protocol.Metric {
	specs := protocol.MetricsForCase(benchmarkCase)
	for index := range specs {
		if value, exists := overrides[specs[index].Name]; exists {
			specs[index].Value = value
		}
	}
	return specs
}

func fixturePeakInFlight(events []protocol.CausalEvent, items []protocol.RecoveryItemObservation) int64 {
	peak := int64(0)
	for sequence := uint64(1); sequence <= uint64(len(events)); sequence++ {
		active := int64(0)
		for _, item := range items {
			scheduled := fixtureEventSequence(events, item.ScheduleEventID)
			terminal := fixtureEventSequence(events, item.TerminalEventID)
			if scheduled <= sequence && sequence < terminal {
				active++
			}
		}
		if active > peak {
			peak = active
		}
	}
	return peak
}

func fixtureEventSequence(events []protocol.CausalEvent, eventID string) uint64 {
	for _, event := range events {
		if event.EventID == eventID {
			return event.Sequence
		}
	}
	return 0
}

func recoveryFixtureViolationCount(
	benchmarkCase protocol.CaseID,
	events []protocol.CausalEvent,
	items []protocol.RecoveryItemObservation,
	requests []protocol.DependencyRequest,
	metrics []protocol.Metric,
) int {
	value := 0
	switch benchmarkCase {
	case protocol.CaseCrashRecoveryBoundaries:
		value += int(fixtureMetricValue(metrics, "duplicate_effect_count") + fixtureMetricValue(metrics, "duplicate_result_count"))
	case protocol.CaseLayeredRetryAmplification:
		counts := make(map[string]int)
		for _, request := range requests {
			counts[request.WorkItemID]++
		}
		for _, count := range counts {
			if count > 4 {
				value += count - 4
			}
		}
	case protocol.CaseOutageBacklogHerdRecovery:
		if peak := fixtureMetricValue(metrics, "peak_retry_concurrency"); peak > 2 {
			value += int(peak - 2)
		}
		value += int(fixtureMetricValue(metrics, "duplicate_effect_count"))
	case protocol.CaseBackpressureOverload:
		if peak := fixturePeakInFlight(events, items); peak > 8 {
			value += int(peak - 8)
		}
	case protocol.CasePoisonWorkIsolation:
		if attempts := fixtureMetricValue(metrics, "poison_attempt_count"); attempts > 3 {
			value += int(attempts - 3)
		}
	case protocol.CaseSilentProgress:
		if latency := fixtureMetricValue(metrics, "failure_detection_latency_ms"); latency > 5000 {
			value++
		}
		value += int(fixtureMetricValue(metrics, "false_positive_revocation_count") + fixtureMetricValue(metrics, "stale_action_accept_count"))
	}
	for _, item := range items {
		if item.Disposition == protocol.RecoveryDispositionUnresolved {
			value++
		}
	}
	return value
}
