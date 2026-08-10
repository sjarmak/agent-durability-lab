// Package oracle independently reconstructs topology benchmark admission and
// logical outcomes from sealed common evidence.
package oracle

import (
	"fmt"
	"reflect"
	"slices"
	"sort"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/evidence"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
)

const (
	ReasonRawEvidenceInvalid           = "raw_evidence_invalid"
	ReasonMembershipInvalid            = "fixed_membership_invalid"
	ReasonBarrierInvalid               = "exact_barrier_invalid"
	ReasonReplayFailed                 = "history_replay_failed"
	ReasonExecutionExcluded            = "execution_excluded"
	ReasonRecoveryObservationMismatch  = "recovery_observation_mismatch"
	ReasonRecoveryMetricMismatch       = "recovery_metric_mismatch"
	ReasonRecoveryControlMismatch      = "recovery_control_mismatch"
	ReasonOrchestrationMetricMismatch  = "orchestration_metric_mismatch"
	ReasonOrchestrationControlMismatch = "orchestration_control_mismatch"
)

func Evaluate(bundle protocol.EvidenceBundle) protocol.Verdict {
	if err := bundle.ValidateRaw(); err != nil {
		return invalidVerdict(bundle.Manifest.RunID, ReasonRawEvidenceInvalid)
	}
	if !validMembership(bundle) {
		return invalidVerdict(bundle.Manifest.RunID, ReasonMembershipInvalid)
	}
	if !validExactBoundary(bundle) {
		return invalidVerdict(bundle.Manifest.RunID, ReasonBarrierInvalid)
	}
	if !bundle.NativeHistory.ReplayCompatible || !bundle.Execution.ReplayVerified {
		return invalidVerdict(bundle.Manifest.RunID, ReasonReplayFailed)
	}
	if bundle.Execution.ExclusionReason != "" {
		return invalidVerdict(bundle.Manifest.RunID, ReasonExecutionExcluded)
	}
	if bundle.Manifest.Case.Suite() == protocol.SuiteRecoveryDynamics {
		if !recoveryObservationsMatchRaw(bundle) {
			return invalidVerdict(bundle.Manifest.RunID, ReasonRecoveryObservationMismatch)
		}
		if !recoveryMetricsMatch(bundle, *bundle.Workload.Recovery) {
			return invalidVerdict(bundle.Manifest.RunID, ReasonRecoveryMetricMismatch)
		}
		if recoveryViolationCount(bundle, *bundle.Workload.Recovery) != bundle.Workload.ProhibitedActionCount {
			return invalidVerdict(bundle.Manifest.RunID, ReasonRecoveryControlMismatch)
		}
		return evaluateRecoveryDynamics(bundle)
	}
	semanticsCorrect, semanticsSafe, prohibitedActions, metricsMatch := evaluateOrchestrationSemantics(bundle)
	if !metricsMatch {
		return invalidVerdict(bundle.Manifest.RunID, ReasonOrchestrationMetricMismatch)
	}
	if prohibitedActions != bundle.Workload.ProhibitedActionCount {
		return invalidVerdict(bundle.Manifest.RunID, ReasonOrchestrationControlMismatch)
	}

	correctness := protocol.OutcomeFail
	if bundle.Workload.ExpectedLogicalOutput == bundle.Workload.ActualLogicalOutput &&
		bundle.Workload.TerminalFailureExpected == bundle.Workload.TerminalFailureObserved &&
		(bundle.Workload.TerminalFailureObserved || len(bundle.Workload.AcceptedResultItemIDs) == bundle.Manifest.Fanout) {
		correctness = protocol.OutcomePass
	}
	safety := protocol.OutcomeFail
	if bundle.Workload.ProhibitedActionCount == 0 && destinationSafe(bundle) {
		safety = protocol.OutcomePass
	}
	if !semanticsCorrect {
		correctness = protocol.OutcomeFail
	}
	if !semanticsSafe {
		safety = protocol.OutcomeFail
	}
	liveness := protocol.OutcomeFail
	if bundle.Workload.LivenessSatisfied {
		liveness = protocol.OutcomePass
	}
	verdict := protocol.Verdict{
		ProtocolVersion: protocol.PublicationProtocolVersion,
		RunID:           bundle.Manifest.RunID,
		Admission:       protocol.AdmissionValid,
		Correctness:     correctness,
		Safety:          safety,
		Liveness:        liveness,
		Diagnosability:  protocol.OutcomePass,
		Oracle:          protocol.OracleProtocolVersion,
	}
	verdict.EfficiencyEligible = correctness == protocol.OutcomePass && safety == protocol.OutcomePass && liveness == protocol.OutcomePass
	return verdict
}

func evaluateRecoveryDynamics(bundle protocol.EvidenceBundle) protocol.Verdict {
	recovery := bundle.Workload.Recovery
	correctness, safety := true, bundle.Workload.ProhibitedActionCount == 0
	if recovery == nil || bundle.Workload.ExpectedLogicalOutput != bundle.Workload.ActualLogicalOutput ||
		len(recovery.Items) != len(bundle.Workload.RequiredItemIDs) {
		correctness = false
	}
	accepted := make(map[string]bool, len(bundle.Workload.AcceptedResultItemIDs))
	for _, item := range bundle.Workload.AcceptedResultItemIDs {
		accepted[item] = true
	}
	if recovery != nil {
		for _, item := range recovery.Items {
			switch item.Disposition {
			case protocol.RecoveryDispositionSucceeded:
				if !accepted[item.WorkItemID] || item.AcceptedResults < 1 {
					correctness = false
				}
			case protocol.RecoveryDispositionQuarantined:
				if !item.Poison || accepted[item.WorkItemID] || item.AcceptedResults != 0 {
					correctness = false
				}
			default:
				correctness = false
			}
			if item.AcceptedEffects > 1 || item.AcceptedResults > 1 {
				safety = false
			}
		}
	}
	liveness := bundle.Workload.LivenessSatisfied && correctness
	verdict := protocol.Verdict{
		ProtocolVersion: protocol.PublicationProtocolVersion, RunID: bundle.Manifest.RunID,
		Admission: protocol.AdmissionValid, Correctness: outcome(correctness), Safety: outcome(safety),
		Liveness: outcome(liveness), Diagnosability: protocol.OutcomePass, Oracle: protocol.OracleProtocolVersion,
	}
	verdict.EfficiencyEligible = correctness && safety && liveness
	return verdict
}

func recoveryObservationsMatchRaw(bundle protocol.EvidenceBundle) bool {
	recovery := bundle.Workload.Recovery
	if recovery == nil {
		return false
	}
	results := make(map[string]int)
	attempts := make(map[string]map[int]bool)
	startedProcesses := make(map[string]map[string]bool)
	for _, event := range bundle.CausalEvents {
		if event.Kind == protocol.EventResultAccepted && event.Decision == protocol.DecisionAccepted {
			results[event.WorkItemID]++
		}
		if event.Kind == protocol.EventActivityStarted {
			if attempts[event.WorkItemID] == nil {
				attempts[event.WorkItemID] = make(map[int]bool)
			}
			attempts[event.WorkItemID][event.ActivityAttempt] = true
		}
		if event.Kind == protocol.EventProcessStarted {
			if startedProcesses[event.WorkItemID] == nil {
				startedProcesses[event.WorkItemID] = make(map[string]bool)
			}
			startedProcesses[event.WorkItemID][event.ProcessIdentity] = true
		}
	}
	effects := make(map[string]int)
	for _, action := range bundle.Destination.Actions {
		if action.Decision == protocol.DecisionAccepted && action.Applied {
			effects[action.WorkItemID]++
		}
	}
	costs := make(map[string]int64)
	for _, request := range bundle.Dependency.Requests {
		costs[request.WorkItemID] += request.CostUnits
	}
	for _, item := range recovery.Items {
		agentProcesses := effects[item.WorkItemID]
		if started := len(startedProcesses[item.WorkItemID]); started > agentProcesses {
			agentProcesses = started
		}
		wantRole, wantPoison := recoveryItemRole(bundle.Manifest.Case, item.WorkItemID)
		if item.AcceptedEffects != effects[item.WorkItemID] || item.AcceptedResults != results[item.WorkItemID] ||
			item.ActivityAttempts != len(attempts[item.WorkItemID]) || item.AgentProcesses != agentProcesses ||
			item.CostUnits != costs[item.WorkItemID] || item.Role != wantRole || item.Poison != wantPoison {
			return false
		}
	}
	return true
}

func recoveryItemRole(benchmarkCase protocol.CaseID, workItemID string) (string, bool) {
	if benchmarkCase == protocol.CasePoisonWorkIsolation && workItemID == "item-001" {
		return "poison", true
	}
	if benchmarkCase == protocol.CaseSilentProgress {
		switch workItemID {
		case "item-001":
			return "wedged", false
		case "item-002":
			return "declared-wait", false
		}
	}
	return "healthy", false
}

func recoveryViolationCount(bundle protocol.EvidenceBundle, recovery protocol.RecoveryDynamics) int {
	value := 0
	switch bundle.Manifest.Case {
	case protocol.CaseCrashRecoveryBoundaries:
		for _, item := range recovery.Items {
			if item.AcceptedEffects > 1 {
				value += item.AcceptedEffects - 1
			}
			if item.AcceptedResults > 1 {
				value += item.AcceptedResults - 1
			}
		}
	case protocol.CaseLayeredRetryAmplification:
		counts := make(map[string]int)
		for _, request := range bundle.Dependency.Requests {
			counts[request.WorkItemID]++
		}
		for _, count := range counts {
			if count > 4 {
				value += count - 4
			}
		}
	case protocol.CaseOutageBacklogHerdRecovery:
		peak := 0
		for _, request := range bundle.Dependency.Requests {
			if request.RetryOwner == "outage-catchup" && request.ConcurrentAtStart > peak {
				peak = request.ConcurrentAtStart
			}
		}
		if peak > 2 {
			value += peak - 2
		}
		for _, item := range recovery.Items {
			if item.AcceptedEffects > 1 {
				value += item.AcceptedEffects - 1
			}
		}
	case protocol.CaseBackpressureOverload:
		if peak := recoveryPeakInFlight(bundle, recovery); peak > 8 {
			value += int(peak - 8)
		}
	case protocol.CasePoisonWorkIsolation:
		for _, item := range recovery.Items {
			if item.Poison && item.ActivityAttempts > 3 {
				value += item.ActivityAttempts - 3
			}
		}
	case protocol.CaseSilentProgress:
		latency, falsePositives, staleActions := silentProgressRawMetrics(bundle, recovery)
		if latency > 5000 {
			value++
		}
		value += int(falsePositives + staleActions)
	}
	for _, item := range recovery.Items {
		if item.Disposition == protocol.RecoveryDispositionUnresolved {
			value++
		}
	}
	return value
}

func recoveryMetricsMatch(bundle protocol.EvidenceBundle, recovery protocol.RecoveryDynamics) bool {
	want, ok := recoveryRawMetrics(bundle, recovery)
	if !ok {
		return false
	}
	for _, metric := range recovery.Metrics {
		if want[metric.Name] != metric.Value {
			return false
		}
	}
	return true
}

func recoveryRawMetrics(bundle protocol.EvidenceBundle, recovery protocol.RecoveryDynamics) (map[string]int64, bool) {
	metrics := make(map[string]int64, len(recovery.Metrics))
	historyEvents, workflowTasks, err := protocol.NativeExportEventCounts(bundle.NativeHistory.Export)
	if err != nil {
		return nil, false
	}
	historyBytes, err := protocol.NativeExportByteCount(bundle.NativeHistory.Export)
	if err != nil {
		return nil, false
	}
	endToEnd := eventSequenceDeltaMS(bundle, 1, uint64(len(bundle.CausalEvents)))
	duplicateEffects, duplicateResults, processes, attempts := int64(0), int64(0), int64(0), int64(0)
	for _, item := range recovery.Items {
		processes += int64(item.AgentProcesses)
		attempts += int64(item.ActivityAttempts)
		if item.AcceptedEffects > 1 {
			duplicateEffects += int64(item.AcceptedEffects - 1)
		}
		if item.AcceptedResults > 1 {
			duplicateResults += int64(item.AcceptedResults - 1)
		}
	}
	metrics["history_event_count"] = int64(historyEvents)
	metrics["history_bytes"] = int64(historyBytes)
	switch bundle.Manifest.Case {
	case protocol.CaseCrashRecoveryBoundaries:
		metrics["agent_process_count"] = processes
		metrics["duplicate_effect_count"] = duplicateEffects
		metrics["duplicate_result_count"] = duplicateResults
		metrics["time_to_recovery_ms"] = recoveryDelayMS(bundle)
		metrics["schedule_to_start_ms"] = maximumScheduleToStartMS(bundle, recovery, false)
		metrics["activity_attempt_count"] = attempts
		metrics["workflow_task_count"] = int64(workflowTasks)
	case protocol.CaseLayeredRetryAmplification:
		metrics["physical_request_count"] = int64(len(bundle.Dependency.Requests))
		metrics["amplification_factor"] = ratioMilli(len(bundle.Dependency.Requests), bundle.Manifest.Fanout)
		metrics["retry_delay_ms"] = sumRequestMetric(bundle.Dependency.Requests, func(request protocol.DependencyRequest) int64 {
			return request.RetryDelayMS
		})
		metrics["active_execution_ms"] = sumRequestMetric(bundle.Dependency.Requests, func(request protocol.DependencyRequest) int64 {
			return request.ServiceMS
		})
		metrics["recovery_delay_ms"] = recoveryDelayMS(bundle)
		metrics["cost_units"] = sumRequestMetric(bundle.Dependency.Requests, func(request protocol.DependencyRequest) int64 {
			return request.CostUnits
		})
	case protocol.CaseOutageBacklogHerdRecovery:
		restoredOffset, found := dependencyRestoredOffset(bundle)
		if !found {
			return nil, false
		}
		peak := int64(0)
		for _, request := range bundle.Dependency.Requests {
			if request.RetryOwner == "outage-catchup" && int64(request.ConcurrentAtStart) > peak {
				peak = int64(request.ConcurrentAtStart)
			}
		}
		drains := backlogDrainDurationsMS(bundle, recovery, restoredOffset)
		metrics["peak_qps"] = peakRequestQPS(bundle.Dependency.Requests, restoredOffset, 10)
		metrics["peak_retry_concurrency"] = peak
		metrics["backlog_integral_ms"] = sumInt64(drains)
		metrics["backlog_drain_p50_ms"] = percentile(drains, 50)
		metrics["backlog_drain_p90_ms"] = percentile(drains, 90)
		metrics["recovery_delay_ms"] = recoveryDelayMS(bundle)
		metrics["duplicate_effect_count"] = duplicateEffects
	case protocol.CaseBackpressureOverload:
		durationMS := endToEnd
		if durationMS < 1 {
			durationMS = 1
		}
		rejected := 0
		for _, item := range recovery.Items {
			if !item.Admitted {
				rejected++
			}
		}
		metrics["schedule_to_start_ms"] = maximumScheduleToStartMS(bundle, recovery, false)
		metrics["queue_age_ms"] = metrics["schedule_to_start_ms"]
		metrics["end_to_end_latency_ms"] = endToEnd
		metrics["throughput_per_second"] = int64(bundle.Manifest.Fanout) * 1_000_000 / durationMS
		metrics["admission_rejection_fraction"] = ratioMilli(rejected, len(recovery.Items))
		metrics["peak_in_flight_count"] = recoveryPeakInFlight(bundle, recovery)
		metrics["history_events_per_item"] = int64(historyEvents) * 1000 / int64(bundle.Manifest.Fanout)
		metrics["history_bytes_per_item"] = int64(historyBytes) / int64(bundle.Manifest.Fanout)
	case protocol.CasePoisonWorkIsolation:
		poisonFound := false
		for _, item := range recovery.Items {
			if item.Poison {
				poisonFound = true
				metrics["poison_attempt_count"] = int64(item.ActivityAttempts)
				metrics["poison_cost_units"] = item.CostUnits
				metrics["poison_capacity_ms"] = itemDurationMS(bundle, item.StartEventID, item.TerminalEventID)
				if item.Disposition != protocol.RecoveryDispositionQuarantined {
					return nil, false
				}
			}
		}
		if !poisonFound {
			return nil, false
		}
		metrics["healthy_schedule_to_start_ms"] = maximumScheduleToStartMS(bundle, recovery, true)
		metrics["healthy_task_latency_ms"] = maximumHealthyTaskLatencyMS(bundle, recovery)
		metrics["healthy_completion_fraction"] = healthyCompletionFractionMilli(recovery)
	case protocol.CaseSilentProgress:
		latency, falsePositives, staleActions := silentProgressRawMetrics(bundle, recovery)
		metrics["failure_detection_latency_ms"] = latency
		metrics["false_positive_revocation_count"] = falsePositives
		metrics["stale_action_accept_count"] = staleActions
		metrics["replacement_recovery_ms"] = replacementRecoveryMS(bundle, recovery)
		metrics["healthy_task_latency_ms"] = maximumHealthyTaskLatencyMS(bundle, recovery)
		metrics["end_to_end_latency_ms"] = endToEnd
	default:
		return nil, false
	}
	return metrics, true
}

func maximumScheduleToStartMS(bundle protocol.EvidenceBundle, recovery protocol.RecoveryDynamics, healthyOnly bool) int64 {
	maximum := int64(0)
	for _, item := range recovery.Items {
		if !item.Admitted || healthyOnly && item.Poison {
			continue
		}
		if value := itemDurationMS(bundle, item.ScheduleEventID, item.StartEventID); value > maximum {
			maximum = value
		}
	}
	return maximum
}

func maximumHealthyTaskLatencyMS(bundle protocol.EvidenceBundle, recovery protocol.RecoveryDynamics) int64 {
	maximum := int64(0)
	for _, item := range recovery.Items {
		if !item.Admitted || item.Poison || item.Role == "wedged" {
			continue
		}
		if value := itemDurationMS(bundle, item.StartEventID, item.TerminalEventID); value > maximum {
			maximum = value
		}
	}
	return maximum
}

func healthyCompletionFractionMilli(recovery protocol.RecoveryDynamics) int64 {
	healthy, completed := 0, 0
	for _, item := range recovery.Items {
		if item.Poison {
			continue
		}
		healthy++
		if item.Disposition == protocol.RecoveryDispositionSucceeded {
			completed++
		}
	}
	return ratioMilli(completed, healthy)
}

func itemDurationMS(bundle protocol.EvidenceBundle, firstID, lastID string) int64 {
	return eventSequenceDeltaMS(bundle, eventSequence(bundle, firstID), eventSequence(bundle, lastID))
}

func recoveryDelayMS(bundle protocol.EvidenceBundle) int64 {
	return eventSequenceDeltaMS(bundle, firstEventSequence(bundle, protocol.EventFaultCommitted), firstEventSequence(bundle, protocol.EventRecoveryObserved))
}

func firstEventSequence(bundle protocol.EvidenceBundle, kind string) uint64 {
	for _, event := range bundle.CausalEvents {
		if event.Kind == kind {
			return event.Sequence
		}
	}
	return 0
}

func dependencyRestoredOffset(bundle protocol.EvidenceBundle) (int64, bool) {
	for _, event := range bundle.CausalEvents {
		if event.Kind == protocol.EventDependencyStateChanged && event.Details["state"] == "restored" {
			return event.MonotonicOffsetNS, true
		}
	}
	return 0, false
}

func backlogDrainDurationsMS(bundle protocol.EvidenceBundle, recovery protocol.RecoveryDynamics, restoredOffset int64) []int64 {
	durations := make([]int64, 0, len(recovery.Items))
	for _, item := range recovery.Items {
		terminal := bundle.CausalEvents[eventSequence(bundle, item.TerminalEventID)-1].MonotonicOffsetNS
		durations = append(durations, nonnegativeDeltaMS(restoredOffset, terminal))
	}
	slices.Sort(durations)
	return durations
}

func peakRequestQPS(requests []protocol.DependencyRequest, restoredOffset int64, windowMS int64) int64 {
	starts := make([]int64, 0, len(requests))
	for _, request := range requests {
		if request.StartedOffsetNS >= restoredOffset {
			starts = append(starts, request.StartedOffsetNS)
		}
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	peak := 0
	windowNS := windowMS * 1_000_000
	for first, last := 0, 0; first < len(starts); first++ {
		if last < first {
			last = first
		}
		for last < len(starts) && starts[last] < starts[first]+windowNS {
			last++
		}
		if count := last - first; count > peak {
			peak = count
		}
	}
	return int64(peak) * 1000 / windowMS
}

func replacementRecoveryMS(bundle protocol.EvidenceBundle, recovery protocol.RecoveryDynamics) int64 {
	_, detection := silentProgressDetectionSequences(bundle)
	if detection == 0 {
		return 0
	}
	for _, item := range recovery.Items {
		if item.Role != "wedged" {
			continue
		}
		terminal := eventSequence(bundle, item.TerminalEventID)
		if terminal > detection && bundle.CausalEvents[terminal-1].Generation > bundle.CausalEvents[detection-1].Generation {
			return eventSequenceDeltaMS(bundle, detection, terminal)
		}
	}
	return 0
}

func silentProgressDetectionSequences(bundle protocol.EvidenceBundle) (uint64, uint64) {
	progress, failedRecovery := uint64(0), uint64(0)
	for _, event := range bundle.CausalEvents {
		if event.WorkItemID != "item-001" {
			continue
		}
		if progress == 0 && event.Kind == protocol.EventProgressAccepted {
			progress = event.Sequence
			continue
		}
		if progress == 0 || event.Sequence <= progress {
			continue
		}
		if event.Kind == protocol.EventAuthorityRevoked {
			return progress, event.Sequence
		}
		if failedRecovery == 0 && event.Kind == protocol.EventRecoveryObserved && event.Decision == protocol.DecisionFailed {
			failedRecovery = event.Sequence
		}
	}
	return progress, failedRecovery
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

func percentile(values []int64, percent int) int64 {
	if len(values) == 0 {
		return 0
	}
	index := (len(values)*percent + 99) / 100
	if index < 1 {
		index = 1
	}
	return values[index-1]
}

func sumInt64(values []int64) int64 {
	result := int64(0)
	for _, value := range values {
		result += value
	}
	return result
}

func nonnegativeDeltaMS(first, last int64) int64 {
	if last <= first {
		return 0
	}
	return (last - first) / 1_000_000
}

func silentProgressRawMetrics(bundle protocol.EvidenceBundle, recovery protocol.RecoveryDynamics) (int64, int64, int64) {
	progress, detection := silentProgressDetectionSequences(bundle)
	roles := make(map[string]string, len(recovery.Items))
	for _, item := range recovery.Items {
		roles[item.WorkItemID] = item.Role
	}
	type authorityIdentity struct {
		workItemID     string
		generation     uint64
		capabilityHash string
	}
	revokedAt := make(map[authorityIdentity]uint64)
	falsePositives := int64(0)
	for _, event := range bundle.CausalEvents {
		if event.Kind == protocol.EventAuthorityRevoked {
			revokedAt[authorityIdentity{event.WorkItemID, event.Generation, event.CapabilityHash}] = event.Sequence
			if roles[event.WorkItemID] != "wedged" {
				falsePositives++
			}
		}
	}
	staleActions := int64(0)
	for _, action := range bundle.Destination.Actions {
		if action.Decision != protocol.DecisionAccepted && action.Decision != protocol.DecisionReconciled {
			continue
		}
		revocation := revokedAt[authorityIdentity{action.WorkItemID, action.Generation, action.CapabilityHash}]
		if revocation > 0 && eventSequence(bundle, action.EventID) > revocation {
			staleActions++
		}
	}
	return eventSequenceDeltaMS(bundle, progress, detection), falsePositives, staleActions
}

func recoveryPeakInFlight(bundle protocol.EvidenceBundle, recovery protocol.RecoveryDynamics) int64 {
	terminals := make(map[string]uint64, len(recovery.Items))
	admissions := make(map[string]uint64, len(recovery.Items))
	for _, item := range recovery.Items {
		admissions[item.WorkItemID] = eventSequence(bundle, item.ScheduleEventID)
		terminals[item.WorkItemID] = eventSequence(bundle, item.TerminalEventID)
	}
	peak := int64(0)
	for sequence := uint64(1); sequence <= uint64(len(bundle.CausalEvents)); sequence++ {
		active := int64(0)
		for item := range admissions {
			if admissions[item] <= sequence && sequence < terminals[item] {
				active++
			}
		}
		if active > peak {
			peak = active
		}
	}
	return peak
}

func eventSequenceDeltaMS(bundle protocol.EvidenceBundle, firstSequence, lastSequence uint64) int64 {
	if firstSequence == 0 || lastSequence == 0 || lastSequence < firstSequence {
		return 0
	}
	first := bundle.CausalEvents[firstSequence-1].MonotonicOffsetNS
	last := bundle.CausalEvents[lastSequence-1].MonotonicOffsetNS
	return (last - first) / 1_000_000
}

func outcome(passed bool) protocol.Outcome {
	if passed {
		return protocol.OutcomePass
	}
	return protocol.OutcomeFail
}

func evaluateOrchestrationSemantics(bundle protocol.EvidenceBundle) (bool, bool, int, bool) {
	switch bundle.Manifest.Case {
	case protocol.CaseJoinBarrier:
		return evaluateJoin(bundle)
	case protocol.CaseIncrementalPartialReduction:
		return evaluateReduction(bundle)
	case protocol.CaseQueuedExecutingSupersession:
		return evaluateSupersession(bundle)
	case protocol.CaseDestructiveTransition:
		return evaluateDestructive(bundle)
	default:
		return true, true, 0, true
	}
}

func evaluateJoin(bundle protocol.EvidenceBundle) (bool, bool, int, bool) {
	semantics := bundle.Workload.Semantics
	required := slices.Clone(bundle.Workload.RequiredItemIDs)
	slices.Sort(required)
	resultSequence := make(map[string]uint64, len(required))
	for _, event := range bundle.CausalEvents {
		if event.Kind == protocol.EventResultAccepted && event.Decision == protocol.DecisionAccepted {
			resultSequence[event.WorkItemID] = event.Sequence
		}
	}
	accepted, premature := int64(0), int64(0)
	completeContinuation := false
	for _, continuation := range semantics.Continuations {
		if continuation.Decision != protocol.DecisionAccepted || !continuation.Applied {
			continue
		}
		accepted++
		complete := slices.Equal(continuation.Members, required)
		continuationSequence := eventSequence(bundle, continuation.EventID)
		if complete {
			for _, item := range required {
				if resultSequence[item] == 0 || resultSequence[item] >= continuationSequence {
					complete = false
					break
				}
			}
		}
		if complete {
			completeContinuation = true
		} else {
			premature++
		}
	}
	lastResult := lastEventSequence(bundle, protocol.EventResultAccepted)
	metricsMatch := metricValuesMatch(semantics.Metrics, map[string]int64{
		"accepted_continuation_count":            accepted,
		"premature_continuation_count":           premature,
		"join_lag_after_last_required_result_ms": eventSequenceDeltaMS(bundle, lastResult, firstEventSequence(bundle, protocol.EventContinuationAccepted)),
		"end_to_end_latency_ms":                  eventSequenceDeltaMS(bundle, 1, uint64(len(bundle.CausalEvents))),
		"history_bytes_per_item":                 orchestrationHistoryBytesPerItem(bundle),
	})
	if bundle.Workload.TerminalFailureExpected {
		correct := bundle.Workload.TerminalFailureObserved && accepted == 0
		return correct, correct && premature == 0 && metricsMatch, int(premature), metricsMatch
	}
	correct := completeContinuation && accepted == 1
	return correct, correct && premature == 0 && metricsMatch, int(premature), metricsMatch
}

func evaluateReduction(bundle protocol.EvidenceBundle) (bool, bool, int, bool) {
	semantics := bundle.Workload.Semantics
	ordinals := make(map[string]int, len(bundle.Workload.RequiredItemIDs))
	duplicateContribution := false
	for _, contribution := range semantics.Contributions {
		if contribution.Decision != protocol.DecisionAccepted {
			continue
		}
		if _, exists := ordinals[contribution.WorkItemID]; exists {
			duplicateContribution = true
			continue
		}
		ordinals[contribution.WorkItemID] = contribution.Ordinal
	}
	expectedValue := int64(0)
	for _, item := range bundle.Workload.RequiredItemIDs {
		ordinal, exists := ordinals[item]
		if !exists {
			continue
		}
		expectedValue += int64(ordinal)
	}
	expectedThresholds := reductionThresholds(len(bundle.Workload.RequiredItemIDs))
	acceptedByID := make(map[string]int)
	appliedCardinalities := make(map[int]bool)
	incorrect := int64(0)
	duplicateApplies := int64(0)
	acceptedReceipt := make(map[string]string)
	for _, checkpoint := range semantics.Checkpoints {
		value, valid := reductionValue(checkpoint.Members, ordinals)
		if !valid || value != checkpoint.Value || !slices.Contains(expectedThresholds, checkpoint.Cardinality) {
			incorrect++
		}
		if checkpoint.Decision == protocol.DecisionAccepted && checkpoint.Applied {
			acceptedByID[checkpoint.CheckpointID]++
			if acceptedByID[checkpoint.CheckpointID] > 1 {
				duplicateApplies++
			}
			if appliedCardinalities[checkpoint.Cardinality] {
				duplicateApplies++
			}
			appliedCardinalities[checkpoint.Cardinality] = true
			acceptedReceipt[checkpoint.CheckpointID] = checkpoint.ReceiptID
		}
		if checkpoint.Decision == protocol.DecisionReconciled && acceptedReceipt[checkpoint.CheckpointID] != checkpoint.ReceiptID {
			incorrect++
		}
	}
	for _, threshold := range expectedThresholds {
		if !appliedCardinalities[threshold] {
			incorrect++
		}
	}
	required := slices.Clone(bundle.Workload.RequiredItemIDs)
	slices.Sort(required)
	finalContinuations := 0
	for _, continuation := range semantics.Continuations {
		if continuation.Decision != protocol.DecisionAccepted || !continuation.Applied {
			continue
		}
		finalContinuations++
		if !slices.Equal(continuation.Members, required) || continuation.Value != expectedValue {
			incorrect++
		}
	}
	if duplicateContribution {
		incorrect++
	}
	metricsMatch := metricValuesMatch(semantics.Metrics, map[string]int64{
		"incorrect_reduction_count":        incorrect,
		"duplicate_checkpoint_apply_count": duplicateApplies,
		"time_to_first_reduction_ms":       eventSequenceDeltaMS(bundle, 1, firstEventSequence(bundle, protocol.EventCheckpointAccepted)),
		"final_makespan_ms":                eventSequenceDeltaMS(bundle, 1, uint64(len(bundle.CausalEvents))),
		"history_bytes_per_item":           orchestrationHistoryBytesPerItem(bundle),
	})
	correct := len(ordinals) == len(required) && finalContinuations == 1 && incorrect == 0
	return correct, correct && duplicateApplies == 0 && metricsMatch, int(incorrect + duplicateApplies), metricsMatch
}

func evaluateSupersession(bundle protocol.EvidenceBundle) (bool, bool, int, bool) {
	semantics := bundle.Workload.Semantics
	observation := semantics.Supersession
	if observation == nil {
		return false, false, 0, false
	}
	commit := eventSequence(bundle, observation.CommitEventID)
	cancellation := eventSequence(bundle, observation.CancellationEventID)
	disposition := eventSequence(bundle, observation.ProcessDispositionEventID)
	ordered := commit > 0 && commit < cancellation && cancellation < disposition
	staleAccepted := int64(0)
	for _, event := range bundle.CausalEvents {
		if event.Sequence <= commit || event.WorkItemID != observation.ObsoleteItemID || event.Generation != observation.ObsoleteGeneration ||
			event.CapabilityHash != observation.ObsoleteCapabilityHash ||
			!slices.Contains([]string{protocol.DecisionAccepted, protocol.DecisionReconciled}, event.Decision) {
			continue
		}
		if slices.Contains([]string{
			protocol.EventProgressAccepted, protocol.EventContributionAccepted,
			protocol.EventResultAccepted, protocol.EventContinuationAccepted, protocol.EventOutcomeAccepted, protocol.EventAcknowledged,
		}, event.Kind) {
			staleAccepted++
		}
	}
	for _, action := range bundle.Destination.Actions {
		if eventSequence(bundle, action.EventID) > commit && action.WorkItemID == observation.ObsoleteItemID && action.Generation == observation.ObsoleteGeneration &&
			action.CapabilityHash == observation.ObsoleteCapabilityHash &&
			slices.Contains([]string{protocol.DecisionAccepted, protocol.DecisionReconciled}, action.Decision) {
			staleAccepted++
		}
	}
	replacementResult := false
	for _, event := range bundle.CausalEvents {
		if event.Sequence > commit && event.WorkItemID == observation.ObsoleteItemID &&
			event.Generation == observation.ReplacementGeneration && event.CapabilityHash == observation.ReplacementCapabilityHash &&
			event.Kind == protocol.EventResultAccepted && event.Decision == protocol.DecisionAccepted {
			replacementResult = true
		}
	}
	required := slices.Clone(bundle.Workload.RequiredItemIDs)
	slices.Sort(required)
	continuationComplete := false
	for _, continuation := range semantics.Continuations {
		if continuation.Decision == protocol.DecisionAccepted && continuation.Applied && slices.Equal(continuation.Members, required) {
			continuationComplete = true
		}
	}
	authorityMatches := bundle.Authority.CurrentGeneration == observation.ReplacementGeneration &&
		bundle.Authority.CurrentCapabilityHash == observation.ReplacementCapabilityHash
	metricsMatch := metricValuesMatch(semantics.Metrics, map[string]int64{
		"stale_action_accept_count":   staleAccepted,
		"cancellation_propagation_ms": eventSequenceDeltaMS(bundle, commit, disposition),
		"replacement_recovery_ms":     eventSequenceDeltaMS(bundle, commit, replacementResultSequence(bundle)),
		"wasted_compute_ms":           eventSequenceDeltaMS(bundle, cancellation, disposition),
		"wasted_cost_units":           supersessionWastedCost(bundle, observation, commit),
	})
	correct := ordered && replacementResult && continuationComplete && authorityMatches
	return correct, correct && staleAccepted == 0 && metricsMatch, int(staleAccepted), metricsMatch
}

func evaluateDestructive(bundle protocol.EvidenceBundle) (bool, bool, int, bool) {
	semantics := bundle.Workload.Semantics
	observation := semantics.Destructive
	if observation == nil {
		return false, false, 0, false
	}
	acceptedApplies := int64(0)
	invariantViolations := int64(0)
	acceptedReceipt := ""
	for _, delivery := range observation.Deliveries {
		if delivery.OperationID != observation.OperationID || delivery.ExpectedVersion != observation.ExpectedPriorVersion {
			invariantViolations++
		}
		switch delivery.Decision {
		case protocol.DecisionAccepted:
			if !delivery.Applied || delivery.PreviousVersion != observation.ExpectedPriorVersion ||
				delivery.ResultingVersion != observation.ExpectedPriorVersion+1 {
				invariantViolations++
			}
			acceptedApplies++
			if acceptedReceipt == "" {
				acceptedReceipt = delivery.ReceiptID
			} else if acceptedReceipt != delivery.ReceiptID {
				invariantViolations++
			}
		case protocol.DecisionReconciled:
			if delivery.Applied || acceptedReceipt == "" || delivery.ReceiptID != acceptedReceipt ||
				delivery.PreviousVersion != observation.ExpectedPriorVersion || delivery.ResultingVersion != observation.ExpectedPriorVersion+1 {
				invariantViolations++
			}
		}
	}
	if acceptedApplies != 1 || observation.FinalVersion != observation.ExpectedPriorVersion+1 ||
		observation.OutcomeReceiptID != acceptedReceipt {
		invariantViolations++
	}
	continuationComplete := false
	for _, continuation := range semantics.Continuations {
		if continuation.Decision == protocol.DecisionAccepted && continuation.Applied &&
			continuation.Value == int64(observation.FinalVersion) {
			continuationComplete = true
		}
	}
	metricsMatch := metricValuesMatch(semantics.Metrics, map[string]int64{
		"accepted_destructive_apply_count": acceptedApplies,
		"invariant_violation_count":        invariantViolations,
		"physical_delivery_count":          int64(len(observation.Deliveries)),
		"recovery_delay_ms":                recoveryDelayMS(bundle),
		"end_to_end_latency_ms":            eventSequenceDeltaMS(bundle, 1, uint64(len(bundle.CausalEvents))),
	})
	correct := continuationComplete && invariantViolations == 0
	return correct, correct && metricsMatch, int(invariantViolations), metricsMatch
}

func eventSequence(bundle protocol.EvidenceBundle, eventID string) uint64 {
	for _, event := range bundle.CausalEvents {
		if event.EventID == eventID {
			return event.Sequence
		}
	}
	return 0
}

func lastEventSequence(bundle protocol.EvidenceBundle, kind string) uint64 {
	for index := len(bundle.CausalEvents) - 1; index >= 0; index-- {
		if bundle.CausalEvents[index].Kind == kind {
			return bundle.CausalEvents[index].Sequence
		}
	}
	return 0
}

func replacementResultSequence(bundle protocol.EvidenceBundle) uint64 {
	for _, event := range bundle.CausalEvents {
		if event.Kind == protocol.EventResultAccepted && event.Generation == 2 {
			return event.Sequence
		}
	}
	return 0
}

func supersessionWastedCost(bundle protocol.EvidenceBundle, observation *protocol.SupersessionObservation, commit uint64) int64 {
	value := int64(0)
	for _, request := range bundle.Dependency.Requests {
		if request.WorkItemID == observation.ObsoleteItemID && eventSequence(bundle, request.EventID) > commit {
			value += request.CostUnits
		}
	}
	return value
}

func orchestrationHistoryBytesPerItem(bundle protocol.EvidenceBundle) int64 {
	count, err := protocol.NativeExportByteCount(bundle.NativeHistory.Export)
	if err != nil || bundle.Manifest.Fanout < 1 {
		return -1
	}
	return int64(count / bundle.Manifest.Fanout)
}

func metricValuesMatch(metrics []protocol.Metric, want map[string]int64) bool {
	if len(metrics) != len(want) {
		return false
	}
	for _, metric := range metrics {
		if value, ok := want[metric.Name]; !ok || value != metric.Value {
			return false
		}
	}
	return true
}

func reductionValue(members []string, ordinals map[string]int) (int64, bool) {
	value := int64(0)
	seen := make(map[string]bool, len(members))
	for _, member := range members {
		ordinal, exists := ordinals[member]
		if !exists || seen[member] {
			return 0, false
		}
		seen[member] = true
		value += int64(ordinal)
	}
	return value, true
}

func reductionThresholds(count int) []int {
	values := []int{1, (count + 3) / 4, (count + 1) / 2, (3*count + 3) / 4, count}
	result := values[:0]
	for _, value := range values {
		if len(result) == 0 || result[len(result)-1] != value {
			result = append(result, value)
		}
	}
	return result
}

func VerifyRun(root, directory string) (protocol.EvidenceBundle, protocol.Verdict, error) {
	bundle, err := evidence.LoadRun(root, directory)
	if err != nil {
		return protocol.EvidenceBundle{}, protocol.Verdict{}, err
	}
	derived := Evaluate(bundle)
	if !reflect.DeepEqual(derived, bundle.Verdict) {
		return bundle, derived, fmt.Errorf("%w: stored verdict differs from independent reconstruction: stored=%+v derived=%+v",
			protocol.ErrInvalidEvidence, bundle.Verdict, derived)
	}
	return bundle, derived, nil
}

func invalidVerdict(runID, reason string) protocol.Verdict {
	return protocol.Verdict{
		ProtocolVersion: protocol.PublicationProtocolVersion,
		RunID:           runID,
		Admission:       protocol.AdmissionInvalid,
		ReasonCodes:     []string{reason},
		Correctness:     protocol.OutcomeUnresolved,
		Safety:          protocol.OutcomeUnresolved,
		Liveness:        protocol.OutcomeUnresolved,
		Diagnosability:  protocol.OutcomeFail,
		Oracle:          protocol.OracleProtocolVersion,
	}
}

func validMembership(bundle protocol.EvidenceBundle) bool {
	required := make(map[string]bool, len(bundle.Workload.RequiredItemIDs))
	for _, item := range bundle.Workload.RequiredItemIDs {
		required[item] = true
	}
	if len(required) != bundle.Manifest.Fanout {
		return false
	}
	eventItems := make(map[string]bool, len(required))
	for _, event := range bundle.CausalEvents {
		eventItems[event.WorkItemID] = true
	}
	for item := range required {
		if !eventItems[item] {
			return false
		}
	}
	accepted := make(map[string]bool, len(bundle.Workload.AcceptedResultItemIDs))
	for _, item := range bundle.Workload.AcceptedResultItemIDs {
		if !required[item] || accepted[item] {
			return false
		}
		accepted[item] = true
	}
	return true
}

func validExactBoundary(bundle protocol.EvidenceBundle) bool {
	boundary := bundle.FaultBoundary
	if bundle.Manifest.Probe == protocol.ProbeUnfaulted {
		if boundary.Injected {
			return false
		}
		for _, event := range bundle.CausalEvents {
			if event.Kind == protocol.EventBarrierReached || event.Kind == protocol.EventFaultCommitted {
				return false
			}
		}
		return true
	}
	if !boundary.Injected || boundary.ExpectedBoundary != bundle.Manifest.Boundary {
		return false
	}
	events := make(map[string]protocol.CausalEvent, len(bundle.CausalEvents))
	for _, event := range bundle.CausalEvents {
		events[event.EventID] = event
	}
	barrier, barrierFound := events[boundary.BarrierEventID]
	fault, faultFound := events[boundary.FaultEventID]
	if !barrierFound || !faultFound || barrier.Kind != protocol.EventBarrierReached || fault.Kind != protocol.EventFaultCommitted ||
		barrier.ProcessIdentity != boundary.TargetProcessIdentity || barrier.Sequence >= fault.Sequence ||
		!slices.Contains(fault.ParentEventIDs, barrier.EventID) {
		return false
	}
	recovered := false
	for _, event := range bundle.CausalEvents {
		if event.Kind == protocol.EventRecoveryObserved && event.Sequence > fault.Sequence {
			recovered = true
			break
		}
	}
	if !recovered {
		return false
	}
	for _, observation := range bundle.ProcessObservations.Observations {
		if observation.EventID == barrier.EventID && observation.ProcessIdentity == boundary.TargetProcessIdentity {
			return true
		}
	}
	return false
}

func destinationSafe(bundle protocol.EvidenceBundle) bool {
	type appliedEffect struct {
		workItemID string
		receiptID  string
	}
	seenEffects := make(map[string]appliedEffect)
	for _, action := range bundle.Destination.Actions {
		if action.Decision == protocol.DecisionRejected {
			continue
		}
		if action.Generation != bundle.Authority.CurrentGeneration || action.CapabilityHash != bundle.Authority.CurrentCapabilityHash {
			return false
		}
		prior, exists := seenEffects[action.LogicalEffectID]
		switch action.Decision {
		case protocol.DecisionAccepted:
			if exists {
				return false
			}
			seenEffects[action.LogicalEffectID] = appliedEffect{workItemID: action.WorkItemID, receiptID: action.ReceiptID}
		case protocol.DecisionReconciled:
			if !exists || prior.workItemID != action.WorkItemID || prior.receiptID != action.ReceiptID {
				return false
			}
		default:
			return false
		}
	}
	return true
}
