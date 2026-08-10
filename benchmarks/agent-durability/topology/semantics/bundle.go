package semantics

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"slices"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/oracle"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
)

func (r *EpisodeRuntime) BuildBundle(output ParentOutput, workflowError string, native protocol.NativeHistory) (protocol.EvidenceBundle, error) {
	if native.EventCount < 1 || len(native.Export) == 0 {
		return protocol.EvidenceBundle{}, fmt.Errorf("%w: native history before bundle", protocol.ErrInvalidEvidence)
	}
	if r.spec.Case.Suite() == protocol.SuiteRecoveryDynamics {
		return r.buildRecoveryBundle(output, workflowError, native)
	}
	finalAuthority := r.Input().InitialAuthority
	if r.spec.Case == protocol.CaseQueuedExecutingSupersession {
		finalAuthority = r.Input().ReplacementAuthority
	}
	identity := protocol.Identity{
		ProtocolVersion: protocol.PublicationProtocolVersion, RunID: r.spec.RunID, PairID: r.spec.PairID,
		ScheduleBlockID: r.spec.ScheduleBlockID, TrackerBeadID: r.manifest.TrackerBeadID, Topology: r.spec.Topology,
		Case: r.spec.Case, Boundary: r.spec.Boundary, Probe: r.spec.Probe, Fanout: r.spec.Fanout,
		LogicalOperationID: r.spec.LogicalOperationID, WorkItemID: "item-001",
		Generation: finalAuthority.Generation, CapabilityHash: finalAuthority.CapabilityHash,
		ParentWorkflowID: r.parentWorkflow, ParentRunID: r.parentRun, ActivityID: "caller/outcome-acknowledgement",
		ActivityAttempt: 1, WorkerID: "topology-benchmark-caller", WorkerPID: os.Getpid(),
		ProcessIdentity: fmt.Sprintf("caller:pid:%d", os.Getpid()),
	}
	outcomeDecision := protocol.DecisionAccepted
	if workflowError != "" {
		outcomeDecision = protocol.DecisionFailed
	}
	outcomeID := r.appendEvent(identity, protocol.EventOutcomeAccepted, outcomeDecision, map[string]string{"workflow_error": workflowError})
	if r.spec.Probe != protocol.ProbeUnfaulted && r.spec.Case == protocol.CaseDestructiveTransition &&
		r.spec.Boundary == "activity-result-recorded-before-outcome-acknowledgement" {
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
		if err := r.redeliverDestructiveAfterCallerLoss(identity); err != nil {
			return protocol.EvidenceBundle{}, err
		}
	}
	r.mu.Lock()
	if r.faultCommitted && !r.recoverySeen {
		r.appendEventLocked(identity, protocol.EventRecoveryObserved, protocol.DecisionObserved, nil, r.fault.FaultEventID)
		r.recoverySeen = true
	}
	r.mu.Unlock()
	r.appendEvent(identity, protocol.EventAcknowledged, protocol.DecisionAccepted, nil)

	r.mu.Lock()
	events := slices.Clone(r.events)
	processes := slices.Clone(r.processes)
	requests := slices.Clone(r.requests)
	actions := slices.Clone(r.destinationActions)
	contributions := slices.Clone(r.contributions)
	checkpoints := cloneCheckpoints(r.checkpoints)
	continuations := cloneContinuations(r.continuations)
	supersession := cloneSupersession(r.supersession)
	destructive := cloneDestructive(r.destructive)
	fault := r.fault
	terminalFailure := r.terminalFailure
	r.mu.Unlock()
	if !fault.Injected {
		fault = protocol.FaultBoundary{RunID: r.spec.RunID}
	}
	native.RunID = r.spec.RunID

	required := make([]string, r.spec.Fanout)
	for index := range required {
		required[index] = fmt.Sprintf("item-%03d", index+1)
	}
	accepted := acceptedResultItems(events, required)
	expectedOutput, actualOutput := logicalOutputs(r.spec, output, workflowError)
	semantics := protocol.OrchestrationSemantics{
		Contributions: contributions, Checkpoints: checkpoints, Continuations: continuations,
		Supersession: supersession, Destructive: destructive,
	}
	semantics.Metrics = deriveMetrics(r.spec, events, semantics, actions, requests, native)
	prohibited := prohibitedCount(r.spec, semantics, events, actions)
	terminalExpected := r.spec.Case == protocol.CaseJoinBarrier && r.spec.Boundary == "required-item-terminal-failure-before-join"

	authority := protocol.AuthorityState{
		RunID: r.spec.RunID, CurrentGeneration: 1, CurrentCapabilityHash: r.Input().InitialAuthority.CapabilityHash,
		Epochs: []protocol.AuthorityEpoch{{Generation: 1, CapabilityHash: r.Input().InitialAuthority.CapabilityHash, State: protocol.AuthorityActive}},
	}
	if r.spec.Case == protocol.CaseQueuedExecutingSupersession {
		authority = protocol.AuthorityState{
			RunID: r.spec.RunID, CurrentGeneration: 2, CurrentCapabilityHash: r.Input().ReplacementAuthority.CapabilityHash,
			Epochs: []protocol.AuthorityEpoch{
				{Generation: 1, CapabilityHash: r.Input().InitialAuthority.CapabilityHash, State: protocol.AuthorityRevoked},
				{Generation: 2, CapabilityHash: r.Input().ReplacementAuthority.CapabilityHash, State: protocol.AuthorityActive},
			},
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
	}{r.spec.Case, r.spec.Boundary, r.spec.Probe, r.spec.Fanout, r.spec.LogicalOperationID, r.Input().InitialAuthority, r.Input().ReplacementAuthority})
	timing := timingEvents(events)
	bundle := protocol.EvidenceBundle{
		Manifest: r.manifest, CausalEvents: events, Lineage: lineage, Authority: authority,
		Destination: protocol.DestinationState{RunID: r.spec.RunID, Actions: actions},
		Dependency:  protocol.DependencyState{RunID: r.spec.RunID, Requests: requests},
		Workload: protocol.WorkloadState{
			RunID: r.spec.RunID, RequiredItemIDs: required, AcceptedResultItemIDs: accepted,
			ExpectedLogicalOutput: expectedOutput, ActualLogicalOutput: actualOutput, ProhibitedActionCount: prohibited,
			TerminalFailureExpected: terminalExpected, TerminalFailureObserved: terminalFailure,
			LivenessSatisfied: workflowError == "" || terminalExpected && terminalFailure, Semantics: semantics,
		},
		FaultBoundary: fault, NativeHistory: native,
		ProcessObservations: protocol.ProcessObservations{RunID: r.spec.RunID, Observations: processes},
		EffectiveInput: protocol.EffectiveInput{
			RunID: r.spec.RunID, PairID: r.spec.PairID, ScheduleBlockID: r.spec.ScheduleBlockID,
			Topology: r.spec.Topology, Case: r.spec.Case, Boundary: r.spec.Boundary, Probe: r.spec.Probe, Fanout: r.spec.Fanout,
			WorkloadSHA256: inputHash, ActivityOptionsSHA256: orchestrationActivityOptionsSHA256(),
			HostEnvelopeSHA256: hashString(runtime.GOOS + "/" + runtime.GOARCH), AgentBinarySHA256: r.agentSHA256,
			DestinationProtocolSHA256: hashString("topology-memory-destination-v1"),
			BarrierControllerSHA256:   hashString("topology-exact-loopback-barrier-v1"), SourceSHA256: r.sourceSHA256,
		},
		Timing: timing,
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

func orchestrationActivityOptionsSHA256() string {
	return hashString("work-and-effect-v2:default-1m:30s:10s:held-supersession-3m:2m:10s:wait-cancel:4:50ms:2x:1s")
}

func (r *EpisodeRuntime) redeliverDestructiveAfterCallerLoss(identity protocol.Identity) error {
	r.mu.Lock()
	observation := cloneDestructive(r.destructive)
	r.mu.Unlock()
	if observation == nil {
		return fmt.Errorf("%w: caller redelivery lacks destructive observation", protocol.ErrInvalidEvidence)
	}
	input := DestructiveActivityInput{
		ProtocolVersion: protocol.PublicationProtocolVersion, PairID: r.spec.PairID,
		OperationID: observation.OperationID, Boundary: r.spec.Boundary, Probe: r.spec.Probe,
		ItemID: "item-001", Authority: r.Input().InitialAuthority, ExpectedVersion: observation.ExpectedPriorVersion,
	}
	result, err := r.destination.ApplyDestructive(DestructiveRequest{
		EventID: "pending-caller-redelivery", ItemID: input.ItemID, OperationID: input.OperationID,
		Authority: input.Authority, ExpectedVersion: input.ExpectedVersion, Attempt: 2, Probe: input.Probe,
	})
	if err != nil {
		return err
	}
	identity.ActivityID = "caller/destructive-redelivery"
	identity.ActivityAttempt = 2
	r.recordDestructiveDelivery(identity, input, 2, result)
	return nil
}

func acceptedResultItems(events []protocol.CausalEvent, required []string) []string {
	requiredSet := make(map[string]bool, len(required))
	for _, item := range required {
		requiredSet[item] = true
	}
	seen := make(map[string]bool, len(required))
	for _, event := range events {
		if event.Kind == protocol.EventResultAccepted && event.Decision == protocol.DecisionAccepted && requiredSet[event.WorkItemID] {
			seen[event.WorkItemID] = true
		}
	}
	result := make([]string, 0, len(seen))
	for _, item := range required {
		if seen[item] {
			result = append(result, item)
		}
	}
	return result
}

func logicalOutputs(spec EpisodeSpec, output ParentOutput, workflowError string) (string, string) {
	switch spec.Case {
	case protocol.CaseJoinBarrier:
		if spec.Boundary == "required-item-terminal-failure-before-join" {
			return "terminal-failure", map[bool]string{true: "terminal-failure", false: "missing-terminal-failure"}[workflowError != ""]
		}
		return fmt.Sprintf("joined:%d", spec.Fanout), fmt.Sprintf("joined:%d", len(output.Results))
	case protocol.CaseIncrementalPartialReduction:
		expected := int64(spec.Fanout * (spec.Fanout + 1) / 2)
		return fmt.Sprintf("reduction:%d", expected), fmt.Sprintf("reduction:%d", output.ReductionValue)
	case protocol.CaseQueuedExecutingSupersession:
		return "replacement-generation:2", fmt.Sprintf("replacement-generation:%d", output.Supersession.Generation)
	case protocol.CaseDestructiveTransition:
		return "destructive-version:1", fmt.Sprintf("destructive-version:%d", output.Destructive.ResultingVersion)
	default:
		return "unsupported", "unsupported"
	}
}

func deriveMetrics(spec EpisodeSpec, events []protocol.CausalEvent, semantics protocol.OrchestrationSemantics,
	actions []protocol.DestinationAction, requests []protocol.DependencyRequest, native protocol.NativeHistory,
) []protocol.Metric {
	endToEnd := eventDeltaMS(events, events[0].EventID, events[len(events)-1].EventID)
	historyBytes, _ := protocol.NativeExportByteCount(native.Export)
	historyPerItem := int64(historyBytes / spec.Fanout)
	switch spec.Case {
	case protocol.CaseJoinBarrier:
		accepted, premature := joinCounts(spec.Fanout, events, semantics.Continuations)
		lastResult, continuation := lastEvent(events, protocol.EventResultAccepted), firstEvent(events, protocol.EventContinuationAccepted)
		return []protocol.Metric{
			{Name: "premature_continuation_count", Unit: "count", Value: premature},
			{Name: "accepted_continuation_count", Unit: "count", Value: accepted},
			{Name: "join_lag_after_last_required_result_ms", Unit: "ms", Value: nonnegative(eventDeltaMS(events, lastResult, continuation))},
			{Name: "end_to_end_latency_ms", Unit: "ms", Value: endToEnd},
			{Name: "history_bytes_per_item", Unit: "bytes_per_item", Value: historyPerItem},
		}
	case protocol.CaseIncrementalPartialReduction:
		incorrect, duplicates := reductionCounts(spec.Fanout, semantics)
		return []protocol.Metric{
			{Name: "incorrect_reduction_count", Unit: "count", Value: incorrect},
			{Name: "duplicate_checkpoint_apply_count", Unit: "count", Value: duplicates},
			{Name: "time_to_first_reduction_ms", Unit: "ms", Value: nonnegative(eventDeltaMS(events, events[0].EventID, firstEvent(events, protocol.EventCheckpointAccepted)))},
			{Name: "final_makespan_ms", Unit: "ms", Value: endToEnd},
			{Name: "history_bytes_per_item", Unit: "bytes_per_item", Value: historyPerItem},
		}
	case protocol.CaseQueuedExecutingSupersession:
		stale := staleAcceptCount(semantics.Supersession, events, actions)
		commit, cancellation, disposition := "", "", ""
		if semantics.Supersession != nil {
			commit, cancellation, disposition = semantics.Supersession.CommitEventID, semantics.Supersession.CancellationEventID, semantics.Supersession.ProcessDispositionEventID
		}
		return []protocol.Metric{
			{Name: "stale_action_accept_count", Unit: "count", Value: stale},
			{Name: "cancellation_propagation_ms", Unit: "ms", Value: nonnegative(eventDeltaMS(events, commit, disposition))},
			{Name: "replacement_recovery_ms", Unit: "ms", Value: nonnegative(eventDeltaMS(events, commit, replacementResultEvent(events)))},
			{Name: "wasted_compute_ms", Unit: "ms", Value: nonnegative(eventDeltaMS(events, cancellation, disposition))},
			{Name: "wasted_cost_units", Unit: "cost_units", Value: staleCost(requests, events, commit)},
		}
	case protocol.CaseDestructiveTransition:
		applies, violations := destructiveCounts(semantics.Destructive)
		return []protocol.Metric{
			{Name: "accepted_destructive_apply_count", Unit: "count", Value: applies},
			{Name: "invariant_violation_count", Unit: "count", Value: violations},
			{Name: "physical_delivery_count", Unit: "count", Value: int64(len(semantics.Destructive.Deliveries))},
			{Name: "recovery_delay_ms", Unit: "ms", Value: recoveryDelay(events)},
			{Name: "end_to_end_latency_ms", Unit: "ms", Value: endToEnd},
		}
	}
	return nil
}

func prohibitedCount(spec EpisodeSpec, semantics protocol.OrchestrationSemantics, events []protocol.CausalEvent, actions []protocol.DestinationAction) int {
	switch spec.Case {
	case protocol.CaseJoinBarrier:
		_, premature := joinCounts(spec.Fanout, events, semantics.Continuations)
		return int(premature)
	case protocol.CaseIncrementalPartialReduction:
		incorrect, duplicate := reductionCounts(spec.Fanout, semantics)
		return int(incorrect + duplicate)
	case protocol.CaseQueuedExecutingSupersession:
		return int(staleAcceptCount(semantics.Supersession, events, actions))
	case protocol.CaseDestructiveTransition:
		_, violations := destructiveCounts(semantics.Destructive)
		return int(violations)
	}
	return 0
}

func joinCounts(fanout int, events []protocol.CausalEvent, continuations []protocol.ContinuationObservation) (int64, int64) {
	resultSequence := make(map[string]uint64, fanout)
	for _, event := range events {
		if event.Kind == protocol.EventResultAccepted && event.Decision == protocol.DecisionAccepted {
			resultSequence[event.WorkItemID] = event.Sequence
		}
	}
	accepted, premature := int64(0), int64(0)
	for _, continuation := range continuations {
		if continuation.Decision != protocol.DecisionAccepted || !continuation.Applied {
			continue
		}
		accepted++
		sequence := sequenceFor(events, continuation.EventID)
		if len(continuation.Members) != fanout {
			premature++
			continue
		}
		for _, member := range continuation.Members {
			if resultSequence[member] == 0 || resultSequence[member] >= sequence {
				premature++
				break
			}
		}
	}
	return accepted, premature
}

func reductionCounts(fanout int, semantics protocol.OrchestrationSemantics) (int64, int64) {
	ordinals := make(map[string]int, fanout)
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
	incorrect, duplicate := int64(0), int64(0)
	acceptedByID := make(map[string]int)
	acceptedCardinality := make(map[int]bool)
	receipts := make(map[string]string)
	thresholds := reductionThresholds(fanout)
	for _, checkpoint := range semantics.Checkpoints {
		value, valid := semanticReductionValue(checkpoint.Members, ordinals)
		if !valid || value != checkpoint.Value || !slices.Contains(thresholds, checkpoint.Cardinality) {
			incorrect++
		}
		if checkpoint.Decision == protocol.DecisionAccepted && checkpoint.Applied {
			acceptedByID[checkpoint.CheckpointID]++
			if acceptedByID[checkpoint.CheckpointID] > 1 {
				duplicate++
			}
			if acceptedCardinality[checkpoint.Cardinality] {
				duplicate++
			}
			acceptedCardinality[checkpoint.Cardinality] = true
			receipts[checkpoint.CheckpointID] = checkpoint.ReceiptID
		}
		if checkpoint.Decision == protocol.DecisionReconciled && receipts[checkpoint.CheckpointID] != checkpoint.ReceiptID {
			incorrect++
		}
	}
	for _, threshold := range thresholds {
		if !acceptedCardinality[threshold] {
			incorrect++
		}
	}
	expected := int64(fanout * (fanout + 1) / 2)
	finals := 0
	for _, continuation := range semantics.Continuations {
		if continuation.Decision == protocol.DecisionAccepted && continuation.Applied {
			finals++
			if len(continuation.Members) != fanout || continuation.Value != expected {
				incorrect++
			}
		}
	}
	if finals != 1 {
		incorrect++
	}
	if duplicateContribution {
		incorrect++
	}
	return incorrect, duplicate
}

func semanticReductionValue(members []string, ordinals map[string]int) (int64, bool) {
	seen := make(map[string]bool, len(members))
	value := int64(0)
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

func staleAcceptCount(observation *protocol.SupersessionObservation, events []protocol.CausalEvent, actions []protocol.DestinationAction) int64 {
	if observation == nil {
		return 0
	}
	commit := sequenceFor(events, observation.CommitEventID)
	count := int64(0)
	for _, event := range events {
		if event.Sequence > commit && event.WorkItemID == observation.ObsoleteItemID && event.Generation == observation.ObsoleteGeneration &&
			event.CapabilityHash == observation.ObsoleteCapabilityHash &&
			slices.Contains([]string{protocol.EventProgressAccepted, protocol.EventContributionAccepted, protocol.EventResultAccepted}, event.Kind) &&
			slices.Contains([]string{protocol.DecisionAccepted, protocol.DecisionReconciled}, event.Decision) {
			count++
		}
	}
	for _, action := range actions {
		if sequenceFor(events, action.EventID) > commit && action.WorkItemID == observation.ObsoleteItemID && action.Generation == observation.ObsoleteGeneration &&
			action.CapabilityHash == observation.ObsoleteCapabilityHash &&
			slices.Contains([]string{protocol.DecisionAccepted, protocol.DecisionReconciled}, action.Decision) {
			count++
		}
	}
	return count
}

func destructiveCounts(observation *protocol.DestructiveObservation) (int64, int64) {
	if observation == nil {
		return 0, 1
	}
	applies, violations, receipt := int64(0), int64(0), ""
	for _, delivery := range observation.Deliveries {
		if delivery.OperationID != observation.OperationID || delivery.ExpectedVersion != observation.ExpectedPriorVersion {
			violations++
		}
		switch delivery.Decision {
		case protocol.DecisionAccepted:
			if !delivery.Applied || delivery.PreviousVersion != observation.ExpectedPriorVersion ||
				delivery.ResultingVersion != observation.ExpectedPriorVersion+1 {
				violations++
			}
			applies++
			if receipt == "" {
				receipt = delivery.ReceiptID
			} else if receipt != delivery.ReceiptID {
				violations++
			}
		case protocol.DecisionReconciled:
			if delivery.Applied || receipt == "" || delivery.ReceiptID != receipt ||
				delivery.PreviousVersion != observation.ExpectedPriorVersion || delivery.ResultingVersion != observation.ExpectedPriorVersion+1 {
				violations++
			}
		}
	}
	if applies != 1 || observation.FinalVersion != observation.ExpectedPriorVersion+1 || observation.OutcomeReceiptID != receipt {
		violations++
	}
	return applies, violations
}

func cloneCheckpoints(values []protocol.CheckpointObservation) []protocol.CheckpointObservation {
	result := slices.Clone(values)
	for index := range result {
		result[index].Members = slices.Clone(result[index].Members)
	}
	return result
}

func cloneContinuations(values []protocol.ContinuationObservation) []protocol.ContinuationObservation {
	result := slices.Clone(values)
	for index := range result {
		result[index].Members = slices.Clone(result[index].Members)
	}
	return result
}

func cloneSupersession(value *protocol.SupersessionObservation) *protocol.SupersessionObservation {
	if value == nil {
		return nil
	}
	result := *value
	return &result
}

func cloneDestructive(value *protocol.DestructiveObservation) *protocol.DestructiveObservation {
	if value == nil {
		return nil
	}
	result := *value
	result.Deliveries = slices.Clone(value.Deliveries)
	return &result
}

func timingEvents(events []protocol.CausalEvent) []protocol.TimingEvent {
	first, last := events[0], events[len(events)-1]
	return []protocol.TimingEvent{
		{Sequence: 1, Kind: first.Kind, TimestampUTC: first.TimestampUTC, MonotonicOffsetNS: first.MonotonicOffsetNS},
		{Sequence: 2, Kind: last.Kind, TimestampUTC: last.TimestampUTC, MonotonicOffsetNS: last.MonotonicOffsetNS},
	}
}

func firstEvent(events []protocol.CausalEvent, kind string) string {
	for _, event := range events {
		if event.Kind == kind {
			return event.EventID
		}
	}
	return ""
}

func lastEvent(events []protocol.CausalEvent, kind string) string {
	for index := len(events) - 1; index >= 0; index-- {
		if events[index].Kind == kind {
			return events[index].EventID
		}
	}
	return ""
}

func sequenceFor(events []protocol.CausalEvent, eventID string) uint64 {
	for _, event := range events {
		if event.EventID == eventID {
			return event.Sequence
		}
	}
	return 0
}

func eventDeltaMS(events []protocol.CausalEvent, firstID, lastID string) int64 {
	var first, last int64
	for _, event := range events {
		if event.EventID == firstID {
			first = event.MonotonicOffsetNS
		}
		if event.EventID == lastID {
			last = event.MonotonicOffsetNS
		}
	}
	return (last - first) / int64(time.Millisecond)
}

func recoveryDelay(events []protocol.CausalEvent) int64 {
	return nonnegative(eventDeltaMS(events, firstEvent(events, protocol.EventFaultCommitted), firstEvent(events, protocol.EventRecoveryObserved)))
}

func replacementResultEvent(events []protocol.CausalEvent) string {
	for _, event := range events {
		if event.Kind == protocol.EventResultAccepted && event.Generation == 2 {
			return event.EventID
		}
	}
	return ""
}

func staleCost(requests []protocol.DependencyRequest, events []protocol.CausalEvent, commitID string) int64 {
	commit := sequenceFor(events, commitID)
	value := int64(0)
	for _, request := range requests {
		if sequenceFor(events, request.EventID) > commit && request.WorkItemID == "item-001" {
			value += request.CostUnits
		}
	}
	return value
}

func nonnegative(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}

func hashJSON(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return hashBytes(data)
}

func hashString(value string) string { return hashBytes([]byte(value)) }

func hashBytes(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}
