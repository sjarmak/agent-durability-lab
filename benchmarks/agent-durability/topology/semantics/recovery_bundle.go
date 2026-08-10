package semantics

import (
	"fmt"
	"os"
	"runtime"
	"slices"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/oracle"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
	"github.com/sjarmak/temporal_projects/internal/workstore"
)

func (r *EpisodeRuntime) buildRecoveryBundle(
	output ParentOutput,
	workflowError string,
	native protocol.NativeHistory,
) (protocol.EvidenceBundle, error) {
	if workflowError != "" {
		return protocol.EvidenceBundle{}, fmt.Errorf("recovery Workflow failed: %s", workflowError)
	}
	finalAuthority := r.Input().InitialAuthority
	if r.spec.Case == protocol.CaseSilentProgress && r.spec.Probe == protocol.ProbeProtected {
		finalAuthority = r.Input().ReplacementAuthority
	}
	identity := r.callerIdentity(finalAuthority)
	outcomeID := r.appendEvent(identity, protocol.EventOutcomeAccepted, protocol.DecisionAccepted, map[string]string{
		"recovery_results": fmt.Sprint(len(output.RecoveryResults)),
	})
	if r.spec.Case == protocol.CaseCrashRecoveryBoundaries && r.spec.Probe != protocol.ProbeUnfaulted &&
		r.spec.Boundary == "parent-outcome-recorded-before-acknowledgement" {
		barrierID := r.appendEvent(identity, protocol.EventBarrierReached, protocol.DecisionBlocked, map[string]string{
			"point": r.spec.Boundary,
		}, outcomeID)
		r.mu.Lock()
		r.processes = append(r.processes, protocol.ProcessObservation{
			EventID: barrierID, WorkItemID: identity.WorkItemID, WorkerID: identity.WorkerID,
			WorkerPID: identity.WorkerPID, ProcessIdentity: identity.ProcessIdentity, State: "caller-lost-before-acknowledgement",
		})
		r.mu.Unlock()
		faultID := r.appendEvent(identity, protocol.EventFaultCommitted, protocol.DecisionAccepted, nil, barrierID)
		r.mu.Lock()
		r.faultCommitted = true
		r.fault = protocol.FaultBoundary{
			RunID: r.spec.RunID, Injected: true, ExpectedBoundary: r.spec.Boundary,
			BarrierEventID: barrierID, FaultEventID: faultID, TargetProcessIdentity: identity.ProcessIdentity,
		}
		r.mu.Unlock()
		r.recordRecovery(identity)
		if r.spec.Probe == protocol.ProbeUnsafe {
			if err := r.recordBlindRedelivery(identity); err != nil {
				return protocol.EvidenceBundle{}, err
			}
		}
	}
	r.mu.Lock()
	if r.faultCommitted && !r.recoverySeen {
		r.appendEventLocked(identity, protocol.EventRecoveryObserved, protocol.DecisionObserved, nil, r.fault.FaultEventID)
		r.recoverySeen = true
	}
	r.mu.Unlock()
	acknowledgementID := r.appendEvent(identity, protocol.EventAcknowledged, protocol.DecisionAccepted, nil)

	r.mu.Lock()
	events := slices.Clone(r.events)
	processes := slices.Clone(r.processes)
	requests := slices.Clone(r.requests)
	actions := slices.Clone(r.destinationActions)
	fault := r.fault
	r.mu.Unlock()
	if !fault.Injected {
		fault = protocol.FaultBoundary{RunID: r.spec.RunID}
	}
	native.RunID = r.spec.RunID
	recoveryItems := r.recoveryItems()
	required := make([]string, r.spec.Fanout)
	for index := range required {
		required[index] = fmt.Sprintf("item-%03d", index+1)
	}
	accepted := acceptedResultItems(events, required)
	expectedOutput, actualOutput := recoveryLogicalOutputs(r.spec, output)
	recovery := protocol.RecoveryDynamics{
		Items: recoveryItems, ParentAcknowledgementEventID: acknowledgementID,
		Bounds: []protocol.Metric{
			{Name: "requests_per_item_max", Unit: "count", Value: protectedRequestsPerItemMax},
			{Name: "retry_concurrency_max", Unit: "count", Value: protectedRetryConcurrencyMax},
			{Name: "in_flight_max", Unit: "count", Value: protectedInFlightMax},
			{Name: "poison_attempts_max", Unit: "count", Value: protectedPoisonAttemptsMax},
			{Name: "progress_deadline_ms", Unit: "ms", Value: progressDeadlineMS},
		},
	}
	recovery.Metrics = r.deriveRecoveryMetrics(events, recoveryItems, requests, native)
	prohibited := r.recoveryProhibitedCount(recoveryItems, requests, recovery.Metrics)
	authority := protocol.AuthorityState{
		RunID: r.spec.RunID, CurrentGeneration: finalAuthority.Generation, CurrentCapabilityHash: finalAuthority.CapabilityHash,
		Epochs: []protocol.AuthorityEpoch{{Generation: finalAuthority.Generation, CapabilityHash: finalAuthority.CapabilityHash, State: protocol.AuthorityActive}},
	}
	if finalAuthority.Generation == 2 {
		authority.Epochs = []protocol.AuthorityEpoch{
			{Generation: 1, CapabilityHash: r.Input().InitialAuthority.CapabilityHash, State: protocol.AuthorityRevoked},
			{Generation: 2, CapabilityHash: finalAuthority.CapabilityHash, State: protocol.AuthorityActive},
		}
	}
	lineage := protocol.Lineage{RunID: r.spec.RunID}
	for _, event := range events {
		for _, parent := range event.ParentEventIDs {
			lineage.Edges = append(lineage.Edges, protocol.LineageEdge{ParentEventID: parent, ChildEventID: event.EventID})
		}
	}
	inputHash := hashJSON(struct {
		Case                 protocol.CaseID
		Boundary             string
		Probe                protocol.Probe
		Fanout               int
		LogicalOperationID   string
		InitialAuthority     Authority
		ReplacementAuthority Authority
		StoreLayout          string
	}{
		Case: r.spec.Case, Boundary: r.spec.Boundary, Probe: r.spec.Probe, Fanout: r.spec.Fanout,
		LogicalOperationID: r.spec.LogicalOperationID, InitialAuthority: r.Input().InitialAuthority,
		ReplacementAuthority: r.Input().ReplacementAuthority, StoreLayout: "stable-per-item-bbolt-v1",
	})
	bundle := protocol.EvidenceBundle{
		Manifest: r.manifest, CausalEvents: events, Lineage: lineage, Authority: authority,
		Destination: protocol.DestinationState{RunID: r.spec.RunID, Actions: actions},
		Dependency:  protocol.DependencyState{RunID: r.spec.RunID, Requests: requests},
		Workload: protocol.WorkloadState{
			RunID: r.spec.RunID, RequiredItemIDs: required, AcceptedResultItemIDs: accepted,
			ExpectedLogicalOutput: expectedOutput, ActualLogicalOutput: actualOutput,
			ProhibitedActionCount: prohibited, LivenessSatisfied: len(output.RecoveryResults) == r.spec.Fanout,
			Recovery: &recovery,
		},
		FaultBoundary: fault, NativeHistory: native,
		ProcessObservations: protocol.ProcessObservations{RunID: r.spec.RunID, Observations: processes},
		EffectiveInput: protocol.EffectiveInput{
			RunID: r.spec.RunID, PairID: r.spec.PairID, ScheduleBlockID: r.spec.ScheduleBlockID,
			Topology: r.spec.Topology, Case: r.spec.Case, Boundary: r.spec.Boundary, Probe: r.spec.Probe, Fanout: r.spec.Fanout,
			WorkloadSHA256:        inputHash,
			ActivityOptionsSHA256: recoveryActivityOptionsSHA256(),
			HostEnvelopeSHA256:    hashString(runtime.GOOS + "/" + runtime.GOARCH + ":worker-concurrency-8"),
			AgentBinarySHA256:     r.agentSHA256, DestinationProtocolSHA256: hashString("workstore-fenced-destination-v1"),
			BarrierControllerSHA256: hashString("topology-exact-loopback-barrier-v1"), SourceSHA256: r.sourceSHA256,
		},
		Timing: timingEvents(events),
		Execution: protocol.PublicationExecution{
			ProtocolVersion: protocol.PublicationProtocolVersion, RunID: r.spec.RunID, PairID: r.spec.PairID,
			ScheduleBlockID: r.spec.ScheduleBlockID, Topology: r.spec.Topology, ReplayVerified: native.ReplayCompatible,
		},
	}
	bundle.Verdict = oracle.Evaluate(bundle)
	if err := bundle.Validate(); err != nil {
		return protocol.EvidenceBundle{}, err
	}
	return bundle, nil
}

func recoveryActivityOptionsSHA256() string {
	return hashString("recovery-work-v2:work-2m:30s:2s:admission-1m:30s:2s:cohort-4m:3m:10s:silent-detect-1s:wait-cancel:shared-case-policy")
}

func (r *EpisodeRuntime) callerIdentity(authority Authority) protocol.Identity {
	return protocol.Identity{
		ProtocolVersion: protocol.PublicationProtocolVersion, RunID: r.spec.RunID, PairID: r.spec.PairID,
		ScheduleBlockID: r.spec.ScheduleBlockID, TrackerBeadID: r.manifest.TrackerBeadID, Topology: r.spec.Topology,
		Case: r.spec.Case, Boundary: r.spec.Boundary, Probe: r.spec.Probe, Fanout: r.spec.Fanout,
		LogicalOperationID: r.spec.LogicalOperationID, WorkItemID: "item-001",
		Generation: authority.Generation, CapabilityHash: authority.CapabilityHash,
		ParentWorkflowID: r.parentWorkflow, ParentRunID: r.parentRun,
		ActivityID: "caller/outcome-acknowledgement", ActivityAttempt: 1,
		WorkerID: "topology-benchmark-caller", WorkerPID: os.Getpid(), ProcessIdentity: fmt.Sprintf("caller:pid:%d", os.Getpid()),
	}
}

func recoveryLogicalOutputs(spec EpisodeSpec, output ParentOutput) (string, string) {
	if spec.Case == protocol.CasePoisonWorkIsolation {
		expected := fmt.Sprintf("healthy:%d/quarantined:1", spec.Fanout-1)
		healthy, quarantined := 0, 0
		for _, result := range output.RecoveryResults {
			if result.Disposition == protocol.RecoveryDispositionSucceeded {
				healthy++
			}
			if result.Disposition == protocol.RecoveryDispositionQuarantined {
				quarantined++
			}
		}
		return expected, fmt.Sprintf("healthy:%d/quarantined:%d", healthy, quarantined)
	}
	expected := fmt.Sprintf("completed:%d", spec.Fanout)
	completed := 0
	for _, result := range output.RecoveryResults {
		if result.Disposition == protocol.RecoveryDispositionSucceeded {
			completed++
		}
	}
	return expected, fmt.Sprintf("completed:%d", completed)
}

func (r *EpisodeRuntime) deriveRecoveryMetrics(
	events []protocol.CausalEvent,
	items []protocol.RecoveryItemObservation,
	requests []protocol.DependencyRequest,
	native protocol.NativeHistory,
) []protocol.Metric {
	endToEnd := eventDeltaMS(events, events[0].EventID, events[len(events)-1].EventID)
	historyByteCount, _ := protocol.NativeExportByteCount(native.Export)
	historyEventCount, workflowTaskCount, _ := protocol.NativeExportEventCounts(native.Export)
	historyBytes := int64(historyByteCount)
	historyEvents := int64(historyEventCount)
	sumProcesses, duplicateEffects, duplicateResults, attemptCount := int64(0), int64(0), int64(0), int64(0)
	for _, item := range items {
		sumProcesses += int64(item.AgentProcesses)
		attemptCount += int64(item.ActivityAttempts)
		if item.AcceptedEffects > 1 {
			duplicateEffects += int64(item.AcceptedEffects - 1)
		}
		if item.AcceptedResults > 1 {
			duplicateResults += int64(item.AcceptedResults - 1)
		}
	}
	switch r.spec.Case {
	case protocol.CaseCrashRecoveryBoundaries:
		return []protocol.Metric{
			{Name: "agent_process_count", Unit: "count", Value: sumProcesses},
			{Name: "duplicate_effect_count", Unit: "count", Value: duplicateEffects},
			{Name: "duplicate_result_count", Unit: "count", Value: duplicateResults},
			{Name: "time_to_recovery_ms", Unit: "ms", Value: recoveryDelay(events)},
			{Name: "schedule_to_start_ms", Unit: "ms", Value: r.maximumScheduleToStartMS()},
			{Name: "activity_attempt_count", Unit: "count", Value: attemptCount},
			{Name: "workflow_task_count", Unit: "count", Value: int64(workflowTaskCount)},
			{Name: "history_event_count", Unit: "count", Value: historyEvents},
			{Name: "history_bytes", Unit: "bytes", Value: historyBytes},
		}
	case protocol.CaseLayeredRetryAmplification:
		return []protocol.Metric{
			{Name: "physical_request_count", Unit: "count", Value: int64(len(requests))},
			{Name: "amplification_factor", Unit: "ratio_milli", Value: ratioMilli(len(requests), r.spec.Fanout)},
			{Name: "retry_delay_ms", Unit: "ms", Value: sumRequestMetric(requests, func(value protocol.DependencyRequest) int64 { return value.RetryDelayMS })},
			{Name: "active_execution_ms", Unit: "ms", Value: sumRequestMetric(requests, func(value protocol.DependencyRequest) int64 { return value.ServiceMS })},
			{Name: "recovery_delay_ms", Unit: "ms", Value: recoveryDelay(events)},
			{Name: "cost_units", Unit: "cost_units", Value: sumRequestMetric(requests, func(value protocol.DependencyRequest) int64 { return value.CostUnits })},
			{Name: "history_event_count", Unit: "count", Value: historyEvents},
			{Name: "history_bytes", Unit: "bytes", Value: historyBytes},
		}
	case protocol.CaseOutageBacklogHerdRecovery:
		drains := r.backlogDrainDurationsMS()
		return []protocol.Metric{
			{Name: "peak_qps", Unit: "requests_per_second", Value: peakRecoveryQPS(requests, r.recovery.outageRestoredOffset)},
			{Name: "peak_retry_concurrency", Unit: "count", Value: int64(r.peakRecoveryConcurrency())},
			{Name: "backlog_integral_ms", Unit: "item_ms", Value: sumInt64(drains)},
			{Name: "backlog_drain_p50_ms", Unit: "ms", Value: percentile(drains, 50)},
			{Name: "backlog_drain_p90_ms", Unit: "ms", Value: percentile(drains, 90)},
			{Name: "recovery_delay_ms", Unit: "ms", Value: recoveryDelay(events)},
			{Name: "duplicate_effect_count", Unit: "count", Value: duplicateEffects},
			{Name: "history_event_count", Unit: "count", Value: historyEvents},
			{Name: "history_bytes", Unit: "bytes", Value: historyBytes},
		}
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
		return []protocol.Metric{
			{Name: "schedule_to_start_ms", Unit: "ms", Value: r.maximumScheduleToStartMS()},
			{Name: "queue_age_ms", Unit: "ms", Value: r.maximumScheduleToStartMS()},
			{Name: "end_to_end_latency_ms", Unit: "ms", Value: endToEnd},
			{Name: "throughput_per_second", Unit: "items_per_second_milli", Value: int64(r.spec.Fanout) * 1_000_000 / durationMS},
			{Name: "admission_rejection_fraction", Unit: "ratio_milli", Value: ratioMilli(rejected, len(items))},
			{Name: "peak_in_flight_count", Unit: "count", Value: int64(r.recovery.peakAdmitted)},
			{Name: "history_events_per_item", Unit: "events_per_item_milli", Value: historyEvents * 1000 / int64(r.spec.Fanout)},
			{Name: "history_bytes_per_item", Unit: "bytes_per_item", Value: historyBytes / int64(r.spec.Fanout)},
		}
	case protocol.CasePoisonWorkIsolation:
		poison := r.recovery.items["item-001"]
		return []protocol.Metric{
			{Name: "poison_attempt_count", Unit: "count", Value: int64(len(poison.attempts))},
			{Name: "poison_cost_units", Unit: "cost_units", Value: poison.costUnits},
			{Name: "poison_capacity_ms", Unit: "ms", Value: offsetDeltaMS(poison.startedOffset, poison.terminalOffset)},
			{Name: "healthy_schedule_to_start_ms", Unit: "ms", Value: r.maximumHealthyScheduleToStartMS()},
			{Name: "healthy_task_latency_ms", Unit: "ms", Value: r.maximumHealthyTaskLatencyMS()},
			{Name: "healthy_completion_fraction", Unit: "ratio_milli", Value: r.healthyCompletionFractionMilli()},
			{Name: "history_event_count", Unit: "count", Value: historyEvents},
			{Name: "history_bytes", Unit: "bytes", Value: historyBytes},
		}
	case protocol.CaseSilentProgress:
		return []protocol.Metric{
			{Name: "failure_detection_latency_ms", Unit: "ms", Value: nonnegative(eventDeltaMS(events, r.recovery.progressEventID, r.recovery.detectionEventID))},
			{Name: "false_positive_revocation_count", Unit: "count", Value: int64(r.recovery.falsePositiveRevocations)},
			{Name: "stale_action_accept_count", Unit: "count", Value: int64(r.recovery.staleActionAccepts)},
			{Name: "replacement_recovery_ms", Unit: "ms", Value: nonnegative(eventDeltaMS(events, r.recovery.detectionEventID, r.recovery.replacementEventID))},
			{Name: "healthy_task_latency_ms", Unit: "ms", Value: r.maximumHealthyTaskLatencyMS()},
			{Name: "end_to_end_latency_ms", Unit: "ms", Value: endToEnd},
			{Name: "history_event_count", Unit: "count", Value: historyEvents},
			{Name: "history_bytes", Unit: "bytes", Value: historyBytes},
		}
	}
	return nil
}

func (r *EpisodeRuntime) recoveryProhibitedCount(
	items []protocol.RecoveryItemObservation,
	requests []protocol.DependencyRequest,
	metrics []protocol.Metric,
) int {
	value := 0
	switch r.spec.Case {
	case protocol.CaseCrashRecoveryBoundaries:
		value += int(metricByName(metrics, "duplicate_effect_count") + metricByName(metrics, "duplicate_result_count"))
	case protocol.CaseLayeredRetryAmplification:
		counts := make(map[string]int)
		for _, request := range requests {
			counts[request.WorkItemID]++
		}
		for _, count := range counts {
			if count > protectedRequestsPerItemMax {
				value += count - protectedRequestsPerItemMax
			}
		}
	case protocol.CaseOutageBacklogHerdRecovery:
		if peak := metricByName(metrics, "peak_retry_concurrency"); peak > protectedRetryConcurrencyMax {
			value += int(peak - protectedRetryConcurrencyMax)
		}
		value += int(metricByName(metrics, "duplicate_effect_count"))
	case protocol.CaseBackpressureOverload:
		if peak := metricByName(metrics, "peak_in_flight_count"); peak > protectedInFlightMax {
			value += int(peak - protectedInFlightMax)
		}
	case protocol.CasePoisonWorkIsolation:
		if attempts := metricByName(metrics, "poison_attempt_count"); attempts > protectedPoisonAttemptsMax {
			value += int(attempts - protectedPoisonAttemptsMax)
		}
	case protocol.CaseSilentProgress:
		if detection := metricByName(metrics, "failure_detection_latency_ms"); detection > progressDeadlineMS {
			value++
		}
		value += int(metricByName(metrics, "false_positive_revocation_count") + metricByName(metrics, "stale_action_accept_count"))
	}
	for _, item := range items {
		if item.Disposition == protocol.RecoveryDispositionUnresolved {
			value++
		}
	}
	return value
}

func (r *EpisodeRuntime) maximumScheduleToStartMS() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	maximum := int64(0)
	for _, item := range r.recovery.items {
		if value := offsetDeltaMS(item.scheduledOffset, item.startedOffset); value > maximum {
			maximum = value
		}
	}
	return maximum
}

func (r *EpisodeRuntime) maximumHealthyScheduleToStartMS() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	maximum := int64(0)
	for _, item := range r.recovery.items {
		if item.poison {
			continue
		}
		if value := offsetDeltaMS(item.scheduledOffset, item.startedOffset); value > maximum {
			maximum = value
		}
	}
	return maximum
}

func (r *EpisodeRuntime) maximumHealthyTaskLatencyMS() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	maximum := int64(0)
	for _, item := range r.recovery.items {
		if item.poison || item.role == "wedged" {
			continue
		}
		if value := offsetDeltaMS(item.startedOffset, item.terminalOffset); value > maximum {
			maximum = value
		}
	}
	return maximum
}

func (r *EpisodeRuntime) healthyCompletionFractionMilli() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	healthy, completed := 0, 0
	for _, item := range r.recovery.items {
		if item.poison {
			continue
		}
		healthy++
		if item.disposition == protocol.RecoveryDispositionSucceeded {
			completed++
		}
	}
	return ratioMilli(completed, healthy)
}

func (r *EpisodeRuntime) backlogDrainDurationsMS() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]int64, 0, len(r.recovery.items))
	for _, item := range r.recovery.items {
		result = append(result, offsetDeltaMS(r.recovery.outageRestoredOffset, item.terminalOffset))
	}
	slices.Sort(result)
	return result
}

func (r *EpisodeRuntime) peakRecoveryConcurrency() int {
	r.recovery.dependency.mu.Lock()
	defer r.recovery.dependency.mu.Unlock()
	restored := r.startedAt.Add(time.Duration(r.recovery.outageRestoredOffset))
	peak := 0
	for _, observation := range r.recovery.dependency.observations {
		if !observation.started.Before(restored) && observation.outcome == "ok" && observation.concurrent > peak {
			peak = observation.concurrent
		}
	}
	return peak
}

func peakRecoveryQPS(requests []protocol.DependencyRequest, restoredOffset int64) int64 {
	starts := make([]int64, 0, len(requests))
	for _, request := range requests {
		if request.StartedOffsetNS >= restoredOffset {
			starts = append(starts, request.StartedOffsetNS)
		}
	}
	slices.Sort(starts)
	peak := 0
	windowNS := int64(10 * time.Millisecond)
	for first, last := 0, 0; first < len(starts); first++ {
		for last < len(starts) && starts[last] < starts[first]+windowNS {
			last++
		}
		if count := last - first; count > peak {
			peak = count
		}
	}
	return int64(peak * 100)
}

func sumRequestMetric(requests []protocol.DependencyRequest, selectValue func(protocol.DependencyRequest) int64) int64 {
	value := int64(0)
	for _, request := range requests {
		value += selectValue(request)
	}
	return value
}

func ratioMilli(numerator, denominator int) int64 {
	if denominator <= 0 {
		return 0
	}
	return int64(numerator) * 1000 / int64(denominator)
}

func offsetDeltaMS(first, last int64) int64 {
	if last <= first {
		return 0
	}
	return (last - first) / int64(time.Millisecond)
}

func percentile(values []int64, percent int) int64 {
	if len(values) == 0 {
		return 0
	}
	copyValues := slices.Clone(values)
	slices.Sort(copyValues)
	index := (len(copyValues)*percent + 99) / 100
	if index < 1 {
		index = 1
	}
	return copyValues[index-1]
}

func sumInt64(values []int64) int64 {
	result := int64(0)
	for _, value := range values {
		result += value
	}
	return result
}

func metricByName(metrics []protocol.Metric, name string) int64 {
	for _, metric := range metrics {
		if metric.Name == name {
			return metric.Value
		}
	}
	return 0
}

var _ = workstore.HashToken
