package calibration

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"strconv"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/evidence"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
)

const (
	recoveryOwner      = "recovery-owner"
	recoveryGeneration = uint64(1)
)

type recoveryBuilder struct {
	config      Config
	runID       string
	episodeID   string
	base        time.Time
	events      []protocol.CausalEvent
	authority   protocol.AuthorityState
	destination protocol.DestinationState
	dependency  protocol.DependencyState
	workload    protocol.WorkloadState
	fault       protocol.FaultBoundary
	processes   []protocol.ProcessObservation
	native      []protocol.NativeRecord
	capability  string
	ready       map[string]string
}

type recoveryAttempt struct {
	item          protocol.WorkItem
	attemptID     string
	parentAttempt string
	ordinal       int
	layer         protocol.RetryLayer
	cause         string
	generation    uint64
	capability    string
	worker        string
	process       string
}

func runRecovery(ctx context.Context, config Config, runID string) (string, error) {
	b := newRecoveryBuilder(config, runID)
	switch config.Case {
	case protocol.CaseLayeredRetryAmplification:
		b.retryAmplification()
	case protocol.CaseOutageBacklogRecovery:
		b.outageBacklog()
	case protocol.CaseBackpressureOverload:
		b.backpressure()
	case protocol.CasePoisonWorkIsolation:
		b.poisonIsolation()
	case protocol.CaseSilentProgress:
		b.silentProgress()
	default:
		return "", fmt.Errorf("%w: recovery case %q", protocol.ErrInvalidEvidence, config.Case)
	}
	b.finalize()
	return evidence.WriteRun(ctx, config.Root, b.bundle())
}

func newRecoveryBuilder(config Config, runID string) *recoveryBuilder {
	base := time.Date(2026, 8, 8, 4, config.Trial, 0, 0, time.UTC)
	capability := hashString("recovery-capability-" + runID)
	return &recoveryBuilder{
		config: config, runID: runID, episodeID: "episode-" + runID, base: base, capability: capability,
		destination: protocol.DestinationState{DestinationID: "recovery-calibration-destination"},
		dependency: protocol.DependencyState{DependencyID: "scripted-recovery-dependency", Transitions: []protocol.DependencyTransition{{
			Sequence: 1, Time: base.Format(time.RFC3339Nano), State: protocol.DependencyHealthy,
		}}},
		workload: protocol.WorkloadState{EpisodeID: "episode-" + runID},
		ready:    make(map[string]string),
		native:   append([]protocol.NativeRecord(nil), config.Native...),
	}
}

func (b *recoveryBuilder) retryAmplification() {
	item := b.addItem(1, false, protocol.WorkItemSucceeded, "retry-target")
	b.startAuthority(item)
	attempts := 1
	if b.config.Probe == protocol.ProbeProtected {
		attempts = 4
	} else if b.config.Probe == protocol.ProbeUnsafe {
		attempts = 16
	}
	var previous string
	var faultAfter protocol.CausalEvent
	for ordinal := 1; ordinal <= attempts; ordinal++ {
		layer := []protocol.RetryLayer{protocol.RetryLayerWorkflow, protocol.RetryLayerActivity, protocol.RetryLayerClient, protocol.RetryLayerAgent}[(ordinal-1)%4]
		attempt := b.attempt(item, ordinal, previous, layer, retryCause(ordinal), recoveryGeneration, b.capability)
		outcome := "500"
		if ordinal == 1 && attempts > 1 {
			outcome = "accepted_then_timeout_script_activated"
		} else if ordinal == attempts {
			outcome = "ok"
		} else if ordinal%3 == 0 {
			outcome = "429"
		} else if ordinal%2 == 0 {
			outcome = "timeout"
		}
		_, finished := b.request(attempt, outcome, b.base.Add(time.Duration(ordinal)*time.Millisecond), b.base.Add(time.Duration(ordinal+1)*time.Millisecond))
		b.attemptFinished(attempt, outcome)
		if ordinal == 1 && attempts > 1 {
			faultAfter = b.controller(protocol.EventFaultCommitted, protocol.DecisionAccepted, item, map[string]string{"fault": "layered_failure_script"})
			_ = finished
		}
		previous = attempt.attemptID
	}
	if attempts > 1 {
		before := b.controller(protocol.EventRecoveryObserved, protocol.DecisionAccepted, item, map[string]string{"dependency": "healthy"})
		b.markFault("failure-script-after-first-accepted-request", faultAfter, before)
	}
	b.acceptOutcome(item, previous, attempts, recoveryGeneration, b.capability)
}

func (b *recoveryBuilder) outageBacklog() {
	cohort := 8
	if b.config.Probe != protocol.ProbeUnfaulted {
		cohort = 12
	}
	items := b.addItems(cohort, nil, protocol.WorkItemSucceeded, "outage-cohort")
	b.startAuthority(items[0])
	if b.config.Probe == protocol.ProbeUnfaulted {
		for index, item := range items {
			attempt := b.attempt(item, 1, "", protocol.RetryLayerActivity, "", recoveryGeneration, b.capability)
			start := b.base.Add(time.Duration(index*3+1) * time.Millisecond)
			b.request(attempt, "ok", start, start.Add(2*time.Millisecond))
			b.attemptFinished(attempt, "ok")
			b.acceptOutcome(item, attempt.attemptID, 1, recoveryGeneration, b.capability)
		}
		return
	}

	steady := b.attempt(items[0], 1, "", protocol.RetryLayerActivity, "", recoveryGeneration, b.capability)
	b.request(steady, "ok", b.base.Add(time.Millisecond), b.base.Add(2*time.Millisecond))
	b.attemptFinished(steady, "ok")
	b.acceptOutcome(items[0], steady.attemptID, 1, recoveryGeneration, b.capability)
	after := b.controller(protocol.EventFaultCommitted, protocol.DecisionAccepted, items[1], map[string]string{"dependency": "outage"})
	b.transition(protocol.DependencyOutage, after.Time)

	parents := make(map[string]string, cohort-1)
	for index, item := range items[1:] {
		attempt := b.attempt(item, 1, "", protocol.RetryLayerActivity, "", recoveryGeneration, b.capability)
		start := b.base.Add(time.Duration(index+5) * time.Millisecond)
		b.request(attempt, "outage", start, start.Add(time.Millisecond))
		b.attemptFinished(attempt, "outage")
		parents[item.WorkItemID] = attempt.attemptID
		b.controller(protocol.EventBarrierReached, protocol.DecisionBlocked, item, map[string]string{"queue_depth": strconv.Itoa(cohort - index - 1)})
	}
	before := b.controller(protocol.EventRecoveryObserved, protocol.DecisionAccepted, items[1], map[string]string{"dependency": "recovering", "queue_depth": strconv.Itoa(cohort - 1)})
	b.transition(protocol.DependencyRecovering, before.Time)
	b.markFault("outage-backlog-target-before-restoration", after, before)

	for index, item := range items[1:] {
		attempt := b.attempt(item, 2, parents[item.WorkItemID], protocol.RetryLayerActivity, "dependency_restored", recoveryGeneration, b.capability)
		startOffset := 40 * time.Millisecond
		duration := 20 * time.Millisecond
		if b.config.Probe == protocol.ProbeProtected {
			startOffset += time.Duration(index*6+(index*int(b.config.Trial))%3) * time.Millisecond
			duration = 3 * time.Millisecond
		}
		start := b.base.Add(startOffset)
		b.request(attempt, "ok", start, start.Add(duration))
		b.attemptFinished(attempt, "ok")
		b.acceptOutcome(item, attempt.attemptID, 2, recoveryGeneration, b.capability)
		b.controller(protocol.EventRecoveryObserved, protocol.DecisionObserved, item, map[string]string{"queue_depth": strconv.Itoa(cohort - index - 2)})
	}
}

func (b *recoveryBuilder) backpressure() {
	cohort := 10
	if b.config.Probe != protocol.ProbeUnfaulted {
		cohort = 100
	}
	admitted := cohort
	if b.config.Probe == protocol.ProbeProtected {
		admitted = 20
	}
	items := make([]protocol.WorkItem, 0, cohort)
	for index := 0; index < cohort; index++ {
		state := protocol.WorkItemSucceeded
		if index >= admitted {
			state = protocol.WorkItemRejected
		}
		items = append(items, b.addItem(index+1, false, state, "offered-load"))
	}
	b.startAuthority(items[0])
	var after protocol.CausalEvent
	if b.config.Probe != protocol.ProbeUnfaulted {
		after = b.controller(protocol.EventFaultCommitted, protocol.DecisionAccepted, items[0], map[string]string{"load_multiple": "100", "submitted": strconv.Itoa(cohort)})
	}
	for index, item := range items {
		if index >= admitted {
			b.controller(protocol.EventActionRejected, protocol.DecisionRejected, item, map[string]string{"action": "admission", "reason": "queue_capacity"})
			continue
		}
		attempt := b.attempt(item, 1, "", protocol.RetryLayerActivity, "", recoveryGeneration, b.capability)
		start := b.base.Add(10 * time.Millisecond)
		duration := 30 * time.Millisecond
		if b.config.Probe != protocol.ProbeUnsafe {
			start = b.base.Add(time.Duration(10+(index/4)*5) * time.Millisecond)
			duration = 4 * time.Millisecond
		}
		b.request(attempt, "ok", start, start.Add(duration))
		b.attemptFinished(attempt, "ok")
		b.acceptOutcome(item, attempt.attemptID, 1, recoveryGeneration, b.capability)
	}
	if b.config.Probe != protocol.ProbeUnfaulted {
		before := b.controller(protocol.EventRecoveryObserved, protocol.DecisionAccepted, items[0], map[string]string{"admitted": strconv.Itoa(admitted)})
		b.markFault("offered-load-gate-release", after, before)
	}
}

func (b *recoveryBuilder) poisonIsolation() {
	poison := map[int]bool{1: true, 2: true}
	items := b.addItems(10, poison, protocol.WorkItemSucceeded, "mixed-cohort")
	b.startAuthority(items[0])
	if b.config.Probe == protocol.ProbeUnfaulted {
		for index, item := range items {
			attempt := b.attempt(item, 1, "", protocol.RetryLayerActivity, "", recoveryGeneration, b.capability)
			start := b.base.Add(time.Duration(index+1) * time.Millisecond)
			b.request(attempt, "ok", start, start.Add(time.Millisecond))
			b.attemptFinished(attempt, "ok")
			b.acceptOutcome(item, attempt.attemptID, 1, recoveryGeneration, b.capability)
		}
		return
	}
	after := b.controller(protocol.EventFaultCommitted, protocol.DecisionAccepted, items[0], map[string]string{"poison": "released"})
	poisonAttempts := 3
	if b.config.Probe == protocol.ProbeUnsafe {
		poisonAttempts = 8
	}
	for index := 0; index < 2; index++ {
		item := items[index]
		var parent string
		for ordinal := 1; ordinal <= poisonAttempts; ordinal++ {
			attempt := b.attempt(item, ordinal, parent, protocol.RetryLayerActivity, retryCause(ordinal), recoveryGeneration, b.capability)
			start := b.base.Add(time.Duration(index*poisonAttempts+ordinal) * time.Millisecond)
			b.request(attempt, "deterministic_failure", start, start.Add(time.Millisecond))
			b.attemptFinished(attempt, "deterministic_failure")
			parent = attempt.attemptID
		}
		b.workload.Items[index].State = protocol.WorkItemQuarantined
	}
	before := b.controller(protocol.EventRecoveryObserved, protocol.DecisionAccepted, items[2], map[string]string{"healthy_pool": "released"})
	b.markFault("registered-poison-failure-release", after, before)
	for index, item := range items[2:] {
		attempt := b.attempt(item, 1, "", protocol.RetryLayerActivity, "", recoveryGeneration, b.capability)
		startOffset := 15 + index
		if b.config.Probe == protocol.ProbeUnsafe {
			startOffset = 90 + index*2
		}
		start := b.base.Add(time.Duration(startOffset) * time.Millisecond)
		b.request(attempt, "ok", start, start.Add(time.Millisecond))
		b.attemptFinished(attempt, "ok")
		b.acceptOutcome(item, attempt.attemptID, 1, recoveryGeneration, b.capability)
	}
}

func (b *recoveryBuilder) silentProgress() {
	items := []protocol.WorkItem{
		b.addItem(1, false, protocol.WorkItemSucceeded, "wedged-executor"),
		b.addItem(2, false, protocol.WorkItemSucceeded, "legitimate-wait"),
	}
	b.startAuthority(items[0])
	wedge := b.attempt(items[0], 1, "", protocol.RetryLayerActivity, "", recoveryGeneration, b.capability)
	b.controllerForAttempt(protocol.EventProgressAccepted, protocol.DecisionAccepted, wedge, map[string]string{"progress": "registered"})
	legitimate := b.attempt(items[1], 1, "", protocol.RetryLayerActivity, "", recoveryGeneration, b.capability)
	b.controllerForAttempt(protocol.EventProgressAccepted, protocol.DecisionAccepted, legitimate, map[string]string{"progress": "declared_durable_wait"})
	if b.config.Probe == protocol.ProbeUnfaulted {
		b.request(wedge, "ok", b.base.Add(3*time.Millisecond), b.base.Add(4*time.Millisecond))
		b.attemptFinished(wedge, "ok")
		b.acceptOutcome(items[0], wedge.attemptID, 1, recoveryGeneration, b.capability)
		b.request(legitimate, "ok", b.base.Add(4*time.Millisecond), b.base.Add(5*time.Millisecond))
		b.attemptFinished(legitimate, "ok")
		b.acceptOutcome(items[1], legitimate.attemptID, 1, recoveryGeneration, b.capability)
		return
	}
	after := b.controllerForAttempt(protocol.EventFaultCommitted, protocol.DecisionAccepted, wedge, map[string]string{"barrier": "silent_progress", "process_alive": "true"})
	if b.config.Probe == protocol.ProbeUnsafe {
		before := b.controllerForAttempt(protocol.EventRecoveryObserved, protocol.DecisionFailed, wedge, map[string]string{"detection": "missing", "process_alive": "true"})
		b.markFault("silent-progress-after-first-heartbeat", after, before)
		b.workload.Items[0].State = protocol.WorkItemRunning
		b.request(legitimate, "ok", b.base.Add(20*time.Millisecond), b.base.Add(21*time.Millisecond))
		b.attemptFinished(legitimate, "ok")
		b.acceptOutcome(items[1], legitimate.attemptID, 1, recoveryGeneration, b.capability)
		return
	}
	before := b.controllerForAttempt(protocol.EventRecoveryObserved, protocol.DecisionAccepted, wedge, map[string]string{"detection": "progress_deadline", "process_alive": "true"})
	b.markFault("silent-progress-after-first-heartbeat", after, before)
	b.authority.Epochs[0].State = protocol.OwnerEpochRevoked
	capability2 := hashString("replacement-capability-" + b.runID)
	ownerEvent := b.add(protocol.CausalEvent{
		Kind: protocol.EventOwnerChanged, Decision: protocol.DecisionAccepted, LogicalOperationID: items[0].LogicalOperationID,
		WorkItemID: items[0].WorkItemID, ActorID: recoveryOwner, Generation: 2, CapabilityHash: capability2,
	})
	b.authority.CurrentGeneration = 2
	b.authority.CurrentCapabilityHash = capability2
	b.authority.Epochs = append(b.authority.Epochs, protocol.OwnerEpoch{
		OwnerID: recoveryOwner, Generation: 2, CapabilityHash: capability2, State: protocol.OwnerEpochActive, Sequence: ownerEvent.Sequence,
	})
	replacement := b.attempt(items[0], 2, wedge.attemptID, protocol.RetryLayerActivity, "progress_deadline", 2, capability2)
	b.request(replacement, "ok", b.base.Add(15*time.Millisecond), b.base.Add(16*time.Millisecond))
	b.attemptFinished(replacement, "ok")
	b.acceptOutcome(items[0], replacement.attemptID, 2, 2, capability2)
	stale := b.controllerForAttempt(protocol.EventActionRejected, protocol.DecisionRejected, wedge, map[string]string{"action": "publish", "reason": "stale_generation"})
	stale.LogicalEffectID = "effect-silent-progress"
	stale.PhysicalAttemptID = "stale-publish-" + b.runID
	b.events[stale.Sequence-1] = stale
	b.destination.Attempts = append(b.destination.Attempts, protocol.DestinationAttempt{
		LogicalOperationID: items[0].LogicalOperationID, LogicalEffectID: stale.LogicalEffectID, PhysicalAttemptID: stale.PhysicalAttemptID,
		OwnerID: recoveryOwner, Generation: 1, CapabilityHash: b.capability, EventID: stale.EventID,
		Decision: protocol.DecisionRejected, Applied: false,
	})
	b.request(legitimate, "ok", b.base.Add(17*time.Millisecond), b.base.Add(18*time.Millisecond))
	b.attemptFinished(legitimate, "ok")
	b.acceptOutcome(items[1], legitimate.attemptID, 1, 2, capability2)
}

func (b *recoveryBuilder) addItems(count int, poison map[int]bool, state protocol.WorkItemState, role string) []protocol.WorkItem {
	items := make([]protocol.WorkItem, 0, count)
	for index := 1; index <= count; index++ {
		items = append(items, b.addItem(index, poison[index], state, role))
	}
	return items
}

func (b *recoveryBuilder) addItem(index int, poison bool, state protocol.WorkItemState, role string) protocol.WorkItem {
	item := protocol.WorkItem{
		WorkItemID: fmt.Sprintf("item-%03d-%s", index, b.runID), LogicalOperationID: fmt.Sprintf("operation-%03d-%s", index, b.runID),
		Poison: poison, State: state,
	}
	b.workload.Items = append(b.workload.Items, item)
	b.workload.ExpectedWorkItems++
	ready := b.add(protocol.CausalEvent{
		Kind: protocol.EventOperationReady, Decision: protocol.DecisionObserved, LogicalOperationID: item.LogicalOperationID,
		WorkItemID: item.WorkItemID, Details: map[string]string{"role": role, "state": "submitted"},
	})
	b.ready[item.WorkItemID] = ready.EventID
	return item
}

func (b *recoveryBuilder) startAuthority(item protocol.WorkItem) {
	event := b.add(protocol.CausalEvent{
		Kind: protocol.EventOwnerChanged, Decision: protocol.DecisionAccepted, LogicalOperationID: item.LogicalOperationID,
		WorkItemID: item.WorkItemID, ActorID: recoveryOwner, Generation: recoveryGeneration, CapabilityHash: b.capability,
	})
	b.authority = protocol.AuthorityState{
		LogicalOperationID: item.LogicalOperationID, CurrentOwnerID: recoveryOwner, CurrentGeneration: recoveryGeneration,
		CurrentCapabilityHash: b.capability, CurrentOwnerAlive: true,
		Epochs: []protocol.OwnerEpoch{{
			OwnerID: recoveryOwner, Generation: recoveryGeneration, CapabilityHash: b.capability,
			State: protocol.OwnerEpochActive, Sequence: event.Sequence,
		}},
	}
}

func (b *recoveryBuilder) attempt(item protocol.WorkItem, ordinal int, parent string, layer protocol.RetryLayer, cause string, generation uint64, capability string) recoveryAttempt {
	attempt := recoveryAttempt{
		item: item, attemptID: fmt.Sprintf("attempt-%s-%02d", item.WorkItemID, ordinal), parentAttempt: parent,
		ordinal: ordinal, layer: layer, cause: cause, generation: generation, capability: capability,
		worker: fmt.Sprintf("worker-%d", (ordinal-1)%4+1), process: fmt.Sprintf("pid:%d:start:fixture-%s-%02d", 1000+len(b.processes), item.WorkItemID, ordinal),
	}
	event := b.addAttemptEvent(protocol.EventAttemptStarted, protocol.DecisionAccepted, attempt, nil)
	b.processes = append(b.processes, protocol.ProcessObservation{
		EventID: event.EventID, OwnerID: recoveryOwner, Generation: generation, WorkerID: attempt.worker,
		ProcessIdentity: attempt.process, State: "completed",
	})
	return attempt
}

func (b *recoveryBuilder) request(attempt recoveryAttempt, outcome string, startedAt, finishedAt time.Time) (protocol.CausalEvent, protocol.CausalEvent) {
	requestID := "request-" + attempt.attemptID
	started := b.addAttemptEvent(protocol.EventRequestStarted, protocol.DecisionObserved, attempt, map[string]string{"outcome": "pending"})
	started.DependencyRequestID = requestID
	b.events[started.Sequence-1] = started
	decision := protocol.DecisionFailed
	if outcome == "ok" || outcome == "accepted_then_timeout_script_activated" {
		decision = protocol.DecisionAccepted
	}
	finished := b.addAttemptEvent(protocol.EventRequestFinished, decision, attempt, map[string]string{"outcome": outcome})
	finished.DependencyRequestID = requestID
	b.events[finished.Sequence-1] = finished
	b.dependency.Requests = append(b.dependency.Requests, protocol.DependencyRequest{
		RequestID: requestID, LogicalOperationID: attempt.item.LogicalOperationID, WorkItemID: attempt.item.WorkItemID,
		AttemptID: attempt.attemptID, ParentAttemptID: attempt.parentAttempt, RetryLayer: attempt.layer, RetryOrdinal: attempt.ordinal,
		StartedAt: startedAt.UTC().Format(time.RFC3339Nano), FinishedAt: finishedAt.UTC().Format(time.RFC3339Nano), Outcome: outcome, CostUnits: 1,
	})
	return started, finished
}

func (b *recoveryBuilder) attemptFinished(attempt recoveryAttempt, outcome string) protocol.CausalEvent {
	decision := protocol.DecisionFailed
	if outcome == "ok" {
		decision = protocol.DecisionAccepted
	}
	return b.addAttemptEvent(protocol.EventAttemptFinished, decision, attempt, map[string]string{"outcome": outcome})
}

func (b *recoveryBuilder) acceptOutcome(item protocol.WorkItem, attemptID string, ordinal int, generation uint64, capability string) {
	parent := ""
	cause := ""
	if ordinal > 1 {
		parent = fmt.Sprintf("attempt-%s-%02d", item.WorkItemID, ordinal-1)
		cause = "retry"
	}
	attempt := recoveryAttempt{
		item: item, attemptID: attemptID, parentAttempt: parent, ordinal: ordinal, layer: protocol.RetryLayerActivity,
		cause: cause, generation: generation, capability: capability, worker: "outcome-worker", process: "outcome-process",
	}
	// Preserve the retry identity recorded when this attempt started.
	for _, event := range b.events {
		if event.AttemptID == attemptID {
			attempt.parentAttempt, attempt.ordinal, attempt.layer, attempt.cause = event.ParentAttemptID, event.RetryOrdinal, event.RetryLayer, event.RetryCause
			attempt.worker, attempt.process = event.WorkerID, event.ProcessIdentity
			break
		}
	}
	b.addAttemptEvent(protocol.EventOutcomeAccepted, protocol.DecisionAccepted, attempt, map[string]string{"action": "outcome"})
	b.addAttemptEvent(protocol.EventAcknowledged, protocol.DecisionAccepted, attempt, map[string]string{"action": "acknowledgement"})
}

func (b *recoveryBuilder) controller(kind, decision string, item protocol.WorkItem, details map[string]string) protocol.CausalEvent {
	return b.add(protocol.CausalEvent{
		Kind: kind, Decision: decision, LogicalOperationID: item.LogicalOperationID, WorkItemID: item.WorkItemID, Details: details,
	})
}

func (b *recoveryBuilder) controllerForAttempt(kind, decision string, attempt recoveryAttempt, details map[string]string) protocol.CausalEvent {
	return b.addAttemptEvent(kind, decision, attempt, details)
}

func (b *recoveryBuilder) addAttemptEvent(kind, decision string, attempt recoveryAttempt, details map[string]string) protocol.CausalEvent {
	return b.add(protocol.CausalEvent{
		Kind: kind, Decision: decision, LogicalOperationID: attempt.item.LogicalOperationID, WorkItemID: attempt.item.WorkItemID,
		AttemptID: attempt.attemptID, ParentAttemptID: attempt.parentAttempt, RetryLayer: attempt.layer, RetryOrdinal: attempt.ordinal,
		RetryCause: attempt.cause, ActorID: recoveryOwner, Generation: attempt.generation, CapabilityHash: attempt.capability,
		WorkerID: attempt.worker, ProcessIdentity: attempt.process, Details: details,
	})
}

func (b *recoveryBuilder) add(event protocol.CausalEvent) protocol.CausalEvent {
	event.Sequence = uint64(len(b.events) + 1)
	event.EventID = fmt.Sprintf("event-%d", event.Sequence)
	event.Time = b.base.Add(time.Duration(event.Sequence) * time.Millisecond).Format(time.RFC3339Nano)
	event.RunID = b.runID
	if len(event.ParentEventIDs) == 0 && len(b.events) != 0 {
		event.ParentEventIDs = []string{b.events[len(b.events)-1].EventID}
	}
	b.events = append(b.events, event)
	return event
}

func (b *recoveryBuilder) controllerEventTime(event protocol.CausalEvent) time.Time {
	parsed, _ := time.Parse(time.RFC3339Nano, event.Time)
	return parsed
}

func (b *recoveryBuilder) transition(state protocol.DependencyStatus, eventTime string) {
	b.dependency.Transitions = append(b.dependency.Transitions, protocol.DependencyTransition{
		Sequence: uint64(len(b.dependency.Transitions) + 1), Time: eventTime, State: state,
	})
}

func (b *recoveryBuilder) markFault(point string, after, before protocol.CausalEvent) {
	afterTime := b.controllerEventTime(after)
	beforeTime := b.controllerEventTime(before)
	b.fault = protocol.FaultBoundary{
		Point: point, Triggered: true, AfterSequence: after.Sequence, AfterEventID: after.EventID,
		BeforeSequence: before.Sequence, BeforeEventID: before.EventID,
		TriggeredAt: afterTime.Add(beforeTime.Sub(afterTime) / 2).Format(time.RFC3339Nano),
	}
}

func (b *recoveryBuilder) finalize() {
	b.authority.AcceptedActions = nil
	for _, event := range b.events {
		if event.Decision == protocol.DecisionAccepted &&
			(event.Kind == protocol.EventActionAccepted || event.Kind == protocol.EventOutcomeAccepted || event.Kind == protocol.EventAcknowledged) {
			b.authority.AcceptedActions = append(b.authority.AcceptedActions, protocol.AcceptedAction{
				Kind: event.Details["action"], OwnerID: event.ActorID, Generation: event.Generation,
				CapabilityHash: event.CapabilityHash, EventID: event.EventID,
			})
		}
	}
	for index := range b.native {
		b.native[index].Sequence = uint64(index + 1)
	}
	for _, event := range b.events {
		data, _ := json.Marshal(event)
		b.native = append(b.native, protocol.NativeRecord{
			Sequence: uint64(len(b.native) + 1), Time: event.Time, Kind: "causal_event_copy", Detail: string(data),
		})
	}
}

func (b *recoveryBuilder) bundle() evidence.Bundle {
	adapterID := b.config.AdapterID
	if adapterID == "" {
		adapterID = "recovery-calibration-v2"
	}
	adapterVersion := b.config.AdapterVersion
	if adapterVersion == "" {
		adapterVersion = "source-sha256:" + hashString("recovery-calibration-v2")
	}
	agentHash := b.config.AgentBinarySHA256
	if agentHash == "" {
		agentHash = hashString("recovery-calibration-agent")
	}
	systemID := b.config.SystemID
	if systemID == "" {
		systemID = "calibration"
	}
	runtimeID := b.config.Runtime
	if runtimeID == "" {
		runtimeID = runtime.GOOS + "/" + runtime.GOARCH
	}
	settings := map[string]string{
		"case": string(b.config.Case), "probe": string(b.config.Probe), "qps_window_ms": "10",
		"retry_budget": "4", "retry_concurrency_limit": "4", "admission_capacity": "20",
		"poison_attempt_budget": "3", "healthy_latency_bound_ms": "50", "progress_deadline_ms": "25",
	}
	for name, value := range b.config.Settings {
		settings[name] = value
	}
	return evidence.Bundle{
		Identity: evidence.RunIdentity{
			RunID: b.runID, Case: b.config.Case, Probe: b.config.Probe, Trial: b.config.Trial,
			EpisodeID: b.episodeID, Seed: int64(b.config.Trial), CohortSize: len(b.workload.Items),
		},
		Events: b.events, Authority: b.authority, Destination: b.destination, Dependency: b.dependency,
		Workload: b.workload, Fault: b.fault, Processes: b.processes, Native: b.native,
		Input: protocol.EffectiveInput{
			AdapterID: adapterID, AdapterVersion: adapterVersion,
			AgentBinarySHA256: agentHash, SystemID: systemID, Runtime: runtimeID,
			AuthorityProtocol: protocol.AuthorityProtocol, DependencyProtocol: protocol.DependencyProtocol,
			FailureProtocol: protocol.FailureProtocol, OracleProtocol: protocol.OracleProtocol,
			DestinationID: b.destination.DestinationID, OracleVisibility: protocol.OracleVisibility(),
			HostLimits: map[string]int64{"workers": 4, "connections": 8, "queue_capacity": 20},
			Settings:   settings,
		},
	}
}

func retryCause(ordinal int) string {
	if ordinal == 1 {
		return ""
	}
	return "dependency_failure"
}
