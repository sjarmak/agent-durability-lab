package publication

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/evidence"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/oracle"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
)

const (
	defaultOwner      = "durable-owner"
	defaultGeneration = uint64(1)
)

type NativeIdentity struct {
	WorkerID        string
	ProcessIdentity string
}

type EpisodeRuntimeConfig struct {
	Request           EpisodeRequest
	Plan              EpisodePlan
	SystemID          string
	AdapterID         string
	AdapterVersion    string
	AgentBinarySHA256 string
	Clock             Clock
	Timing            *TimingRecorder
	Settings          map[string]string
}

type runtimeAttempt struct {
	item       protocol.WorkItem
	id         string
	parent     string
	ordinal    int
	layer      protocol.RetryLayer
	cause      string
	owner      string
	generation uint64
	capability string
	worker     string
	process    string
}

type EpisodeRuntime struct {
	mu           sync.Mutex
	config       EpisodeRuntimeConfig
	runID        string
	episodeID    string
	clock        Clock
	lastTime     time.Time
	events       []protocol.CausalEvent
	authority    protocol.AuthorityState
	destination  protocol.DestinationState
	dependency   protocol.DependencyState
	workload     protocol.WorkloadState
	fault        protocol.FaultBoundary
	processes    []protocol.ProcessObservation
	items        map[int]int
	attempts     map[string]runtimeAttempt
	faultAfter   protocol.CausalEvent
	capability   string
	finished     bool
	acknowledged bool
	abaBarrier   chan struct{}
	abaRelease   chan bool
	abaStale     runtimeAttempt
	abaCurrent   runtimeAttempt
	abaStarted   protocol.CausalEvent
	abaTriggered time.Time
}

func NewEpisodeRuntime(config EpisodeRuntimeConfig) (*EpisodeRuntime, error) {
	if err := config.Plan.Validate(); err != nil {
		return nil, err
	}
	if config.Request.Case != config.Plan.Case || config.Request.Probe != config.Plan.Probe || config.Request.Slot != config.Plan.Trial ||
		config.SystemID == "" || config.AdapterID == "" || config.Timing == nil || !validSourceHash(config.AdapterVersion) ||
		!validDigest(config.AgentBinarySHA256) {
		return nil, invalid("episode runtime configuration")
	}
	if config.Clock == nil {
		config.Clock = wallClock{}
	}
	runID := observedRunID(config.SystemID, config.Request.PairID)
	capabilityGeneration := uint64(1)
	owner := defaultOwner
	if config.Request.Case == protocol.CaseABAReacquisition {
		capabilityGeneration = 7
		owner = "A"
	}
	capability := digest(fmt.Sprintf("capability/%s/%d", runID, capabilityGeneration))
	r := &EpisodeRuntime{
		config: config, runID: runID, episodeID: "episode-" + runID, clock: config.Clock, capability: capability,
		destination: protocol.DestinationState{DestinationID: "observed-destination-" + runID},
		dependency:  protocol.DependencyState{DependencyID: "observed-dependency-" + runID},
		items:       make(map[int]int, len(config.Plan.Items)), attempts: make(map[string]runtimeAttempt),
		abaBarrier: make(chan struct{}), abaRelease: make(chan bool, 1),
	}
	r.config.Timing.Record(protocol.EventOperationReady, "", map[string]string{"source": "system_submission"})
	r.mu.Lock()
	transitionTime := r.nextTimeLocked()
	r.dependency.Transitions = append(r.dependency.Transitions, protocol.DependencyTransition{
		Sequence: 1, Time: transitionTime.Format(time.RFC3339Nano), State: protocol.DependencyHealthy,
	})
	for _, input := range config.Plan.Items {
		item := protocol.WorkItem{
			WorkItemID:         fmt.Sprintf("item-%03d-%s", input.Index, runID),
			LogicalOperationID: fmt.Sprintf("operation-%03d-%s", input.Index, runID),
			Poison:             input.Poison, State: protocol.WorkItemSubmitted,
		}
		r.items[input.Index] = len(r.workload.Items)
		r.workload.Items = append(r.workload.Items, item)
		r.addLocked(protocol.CausalEvent{
			Kind: protocol.EventOperationReady, Decision: protocol.DecisionObserved,
			LogicalOperationID: item.LogicalOperationID, WorkItemID: item.WorkItemID,
			Details: map[string]string{"role": input.Role, "state": "submitted"},
		})
	}
	r.workload.EpisodeID = r.episodeID
	r.workload.ExpectedWorkItems = len(r.workload.Items)
	first := r.workload.Items[0]
	ownerEvent := r.addLocked(protocol.CausalEvent{
		Kind: protocol.EventOwnerChanged, Decision: protocol.DecisionAccepted,
		LogicalOperationID: first.LogicalOperationID, WorkItemID: first.WorkItemID,
		ActorID: owner, Generation: capabilityGeneration, CapabilityHash: capability,
	})
	r.authority = protocol.AuthorityState{
		LogicalOperationID: first.LogicalOperationID, CurrentOwnerID: owner,
		CurrentGeneration: capabilityGeneration, CurrentCapabilityHash: capability, CurrentOwnerAlive: true,
		Epochs: []protocol.OwnerEpoch{{
			OwnerID: owner, Generation: capabilityGeneration, CapabilityHash: capability,
			State: protocol.OwnerEpochActive, Sequence: ownerEvent.Sequence,
		}},
	}
	r.mu.Unlock()
	return r, nil
}

func (r *EpisodeRuntime) BeginABA(ctx context.Context, identity NativeIdentity) error {
	if r.config.Request.Case != protocol.CaseABAReacquisition || r.config.Request.Probe == protocol.ProbeUnfaulted {
		return invalid("ABA delayed request")
	}
	r.mu.Lock()
	item := r.itemLocked(1)
	attempt := runtimeAttempt{
		item: item, id: "attempt-aba-g7", ordinal: 1, layer: protocol.RetryLayerActivity,
		owner: "A", generation: 7, capability: r.authority.Epochs[0].CapabilityHash,
		worker: identity.WorkerID, process: identity.ProcessIdentity,
	}
	if attempt.worker == "" || attempt.process == "" {
		r.mu.Unlock()
		return invalid("ABA delayed identity")
	}
	started := r.addAttemptEventLocked(protocol.EventAttemptStarted, protocol.DecisionAccepted, attempt, nil)
	r.attempts[attempt.id] = attempt
	r.processes = append(r.processes, protocol.ProcessObservation{
		EventID: started.EventID, OwnerID: attempt.owner, Generation: attempt.generation,
		WorkerID: attempt.worker, ProcessIdentity: attempt.process, State: "delayed",
	})
	requestStarted := r.addAttemptEventLocked(protocol.EventRequestStarted, protocol.DecisionObserved, attempt, map[string]string{"outcome": "pending"})
	requestStarted.DependencyRequestID = "request-aba-g7"
	r.events[requestStarted.Sequence-1] = requestStarted
	r.abaStarted = requestStarted
	barrier := r.addAttemptEventLocked(protocol.EventBarrierReached, protocol.DecisionBlocked, attempt, map[string]string{"barrier": "g7-delayed"})
	r.faultAfter = barrier
	r.abaStale = attempt
	r.mu.Unlock()
	r.config.Timing.Record(protocol.EventBarrierReached, "g7-delayed-until-g9-current", map[string]string{"generation": "7"})
	r.config.Timing.Record(protocol.EventFaultCommitted, "g7-delayed-until-g9-current", map[string]string{"authority_change": "A7-to-B8"})
	close(r.abaBarrier)

	var accepted bool
	select {
	case <-ctx.Done():
		return ctx.Err()
	case accepted = <-r.abaRelease:
	}
	r.completeABAStale(accepted)
	return nil
}

func (r *EpisodeRuntime) WaitABABarrier(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.abaBarrier:
		return nil
	}
}

func (r *EpisodeRuntime) AdvanceABA(generation uint64, identity NativeIdentity) error {
	if generation != 8 && generation != 9 {
		return invalid("ABA generation")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.itemLocked(1)
	owner := "B"
	state := protocol.OwnerEpochCompleted
	parent, ordinal, cause := "attempt-aba-g7", 2, "owner_replaced"
	if generation == 8 {
		r.authority.Epochs[0].State = protocol.OwnerEpochObsolete
	} else {
		if len(r.authority.Epochs) != 2 {
			return invalid("ABA generation 8 missing")
		}
		owner, state, parent, ordinal, cause = "A", protocol.OwnerEpochActive, "attempt-aba-g8", 3, "owner_reacquired"
	}
	capability := digest(fmt.Sprintf("capability/%s/%d", r.runID, generation))
	ownerEvent := r.addLocked(protocol.CausalEvent{
		Kind: protocol.EventOwnerChanged, Decision: protocol.DecisionAccepted,
		LogicalOperationID: item.LogicalOperationID, WorkItemID: item.WorkItemID,
		ActorID: owner, Generation: generation, CapabilityHash: capability,
	})
	if generation == 8 {
		r.abaTriggered = mustEventTime(ownerEvent)
	}
	attempt := runtimeAttempt{
		item: item, id: fmt.Sprintf("attempt-aba-g%d", generation), parent: parent, ordinal: ordinal,
		layer: protocol.RetryLayerActivity, cause: cause, owner: owner, generation: generation,
		capability: capability, worker: identity.WorkerID, process: identity.ProcessIdentity,
	}
	if attempt.worker == "" || attempt.process == "" {
		return invalid("ABA replacement identity")
	}
	started := r.addAttemptEventLocked(protocol.EventAttemptStarted, protocol.DecisionAccepted, attempt, nil)
	r.attempts[attempt.id] = attempt
	r.processes = append(r.processes, protocol.ProcessObservation{
		EventID: started.EventID, OwnerID: owner, Generation: generation,
		WorkerID: attempt.worker, ProcessIdentity: attempt.process, State: map[uint64]string{8: "completed", 9: "running"}[generation],
	})
	r.authority.Epochs = append(r.authority.Epochs, protocol.OwnerEpoch{
		OwnerID: owner, Generation: generation, CapabilityHash: capability, State: state, Sequence: ownerEvent.Sequence,
	})
	if generation == 8 {
		r.addAttemptEventLocked(protocol.EventAttemptFinished, protocol.DecisionAccepted, attempt, map[string]string{"outcome": "epoch_completed"})
		return nil
	}
	r.authority.CurrentOwnerID = owner
	r.authority.CurrentGeneration = generation
	r.authority.CurrentCapabilityHash = capability
	r.abaCurrent = attempt
	r.config.Timing.Record(protocol.EventRecoveryObserved, "g7-delayed-until-g9-current", map[string]string{"generation": "9"})
	return nil
}

func (r *EpisodeRuntime) CompleteABACurrent(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	attempt := r.abaCurrent
	if attempt.id == "" {
		r.mu.Unlock()
		return invalid("ABA current attempt")
	}
	started := r.addAttemptEventLocked(protocol.EventRequestStarted, protocol.DecisionObserved, attempt, map[string]string{"outcome": "pending"})
	started.DependencyRequestID = "request-aba-g9"
	r.events[started.Sequence-1] = started
	r.mu.Unlock()
	timer := time.NewTimer(time.Millisecond)
	select {
	case <-ctx.Done():
		if !timer.Stop() {
			<-timer.C
		}
		return ctx.Err()
	case <-timer.C:
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	finished := r.addAttemptEventLocked(protocol.EventRequestFinished, protocol.DecisionAccepted, attempt, map[string]string{"outcome": "accepted"})
	finished.DependencyRequestID = "request-aba-g9"
	r.events[finished.Sequence-1] = finished
	r.dependency.Requests = append(r.dependency.Requests, protocol.DependencyRequest{
		RequestID: "request-aba-g9", LogicalOperationID: attempt.item.LogicalOperationID, WorkItemID: attempt.item.WorkItemID,
		AttemptID: attempt.id, ParentAttemptID: attempt.parent, RetryLayer: attempt.layer, RetryOrdinal: attempt.ordinal,
		StartedAt: started.Time, FinishedAt: finished.Time, Outcome: "accepted", CostUnits: 1,
	})
	effect := r.addAttemptEventLocked(protocol.EventActionAccepted, protocol.DecisionAccepted, attempt, map[string]string{"action": "effect"})
	effect.LogicalEffectID = "effect-aba"
	effect.PhysicalAttemptID = "physical-g9-" + r.runID
	r.events[effect.Sequence-1] = effect
	r.destination.Attempts = append(r.destination.Attempts, protocol.DestinationAttempt{
		LogicalOperationID: attempt.item.LogicalOperationID, LogicalEffectID: effect.LogicalEffectID,
		PhysicalAttemptID: effect.PhysicalAttemptID, OwnerID: attempt.owner, Generation: attempt.generation,
		CapabilityHash: attempt.capability, EventID: effect.EventID, Decision: protocol.DecisionAccepted, Applied: true,
	})
	r.addAttemptEventLocked(protocol.EventAttemptFinished, protocol.DecisionAccepted, attempt, map[string]string{"outcome": "ok"})
	r.setItemStateLocked(1, protocol.WorkItemSucceeded)
	r.acceptOutcomeLocked(attempt)
	return nil
}

func (r *EpisodeRuntime) ReleaseABA(staleAccepted bool) {
	r.abaRelease <- staleAccepted
}

func (r *EpisodeRuntime) completeABAStale(accepted bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	attempt := r.abaStale
	decision := protocol.DecisionRejected
	kind := protocol.EventActionRejected
	outcome := "stale_rejected"
	if accepted {
		decision, kind, outcome = protocol.DecisionAccepted, protocol.EventActionAccepted, "stale_accepted"
	}
	finished := r.addAttemptEventLocked(protocol.EventRequestFinished, decision, attempt, map[string]string{"outcome": outcome})
	finished.DependencyRequestID = "request-aba-g7"
	r.events[finished.Sequence-1] = finished
	r.dependency.Requests = append(r.dependency.Requests, protocol.DependencyRequest{
		RequestID: "request-aba-g7", LogicalOperationID: attempt.item.LogicalOperationID, WorkItemID: attempt.item.WorkItemID,
		AttemptID: attempt.id, RetryLayer: attempt.layer, RetryOrdinal: attempt.ordinal,
		StartedAt: r.abaStarted.Time, FinishedAt: finished.Time, Outcome: outcome, CostUnits: 1,
	})
	effect := r.addAttemptEventLocked(kind, decision, attempt, map[string]string{"action": "effect"})
	effect.LogicalEffectID = "effect-aba"
	effect.PhysicalAttemptID = "physical-g7-" + r.runID
	r.events[effect.Sequence-1] = effect
	r.destination.Attempts = append(r.destination.Attempts, protocol.DestinationAttempt{
		LogicalOperationID: attempt.item.LogicalOperationID, LogicalEffectID: effect.LogicalEffectID,
		PhysicalAttemptID: effect.PhysicalAttemptID, OwnerID: attempt.owner, Generation: attempt.generation,
		CapabilityHash: attempt.capability, EventID: effect.EventID, Decision: decision, Applied: accepted,
	})
	for _, action := range []string{"completion", "acknowledgement", "stop"} {
		r.addAttemptEventLocked(kind, decision, attempt, map[string]string{"action": action})
	}
	if accepted {
		r.authority.CurrentOwnerAlive = false
	}
	triggered := r.abaTriggered
	if !triggered.After(mustEventTime(r.faultAfter)) || !triggered.Before(mustEventTime(finished)) {
		triggered = mustEventTime(r.faultAfter).Add(mustEventTime(finished).Sub(mustEventTime(r.faultAfter)) / 2)
	}
	r.fault = protocol.FaultBoundary{
		Point: "g7-delayed-until-g9-current", Triggered: true,
		AfterSequence: r.faultAfter.Sequence, AfterEventID: r.faultAfter.EventID,
		BeforeSequence: finished.Sequence, BeforeEventID: finished.EventID,
		TriggeredAt: triggered.Format(time.RFC3339Nano),
	}
}

func (r *EpisodeRuntime) RunWork(ctx context.Context, work WorkSpec, identity NativeIdentity) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	switch work.Kind {
	case WorkRequest:
		return r.runRequest(ctx, work, identity)
	case WorkFault:
		r.commitFault(work.Item)
	case WorkRecovery:
		r.observeRecovery(work.Item)
	case WorkRejectAdmission:
		r.rejectAdmission(work.Item)
	case WorkSilentProgress:
		r.observeSilent(work.Item, identity)
	case WorkLegitimateWait:
		r.observeLegitimateWait(work.Item)
	case WorkReplaceSilent:
		r.replaceSilent(work.Item, identity)
	default:
		return invalid("runtime work kind")
	}
	return nil
}

func (r *EpisodeRuntime) runRequest(ctx context.Context, work WorkSpec, identity NativeIdentity) error {
	r.mu.Lock()
	attempt, err := r.startAttemptLocked(work, identity)
	if err != nil {
		r.mu.Unlock()
		return err
	}
	requestID := "request-" + attempt.id
	started := r.addAttemptEventLocked(protocol.EventRequestStarted, protocol.DecisionObserved, attempt, map[string]string{"outcome": "pending"})
	started.DependencyRequestID = requestID
	r.events[started.Sequence-1] = started
	r.mu.Unlock()

	if duration := serviceDuration(work); duration > 0 {
		timer := time.NewTimer(duration)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	decision := protocol.DecisionFailed
	if work.Outcome == "ok" || work.Outcome == "accepted_then_timeout_script_activated" {
		decision = protocol.DecisionAccepted
	}
	finished := r.addAttemptEventLocked(protocol.EventRequestFinished, decision, attempt, map[string]string{"outcome": work.Outcome})
	finished.DependencyRequestID = requestID
	r.events[finished.Sequence-1] = finished
	r.dependency.Requests = append(r.dependency.Requests, protocol.DependencyRequest{
		RequestID: requestID, LogicalOperationID: attempt.item.LogicalOperationID, WorkItemID: attempt.item.WorkItemID,
		AttemptID: attempt.id, ParentAttemptID: attempt.parent, RetryLayer: attempt.layer, RetryOrdinal: attempt.ordinal,
		StartedAt: started.Time, FinishedAt: finished.Time, Outcome: work.Outcome, CostUnits: 1,
	})
	finishDecision := protocol.DecisionFailed
	if work.Outcome == "ok" {
		finishDecision = protocol.DecisionAccepted
	}
	r.addAttemptEventLocked(protocol.EventAttemptFinished, finishDecision, attempt, map[string]string{"outcome": work.Outcome})
	if work.Outcome == "ok" {
		r.setItemStateLocked(work.Item, protocol.WorkItemSucceeded)
		r.acceptOutcomeLocked(attempt)
	}
	return nil
}

func (r *EpisodeRuntime) startAttemptLocked(work WorkSpec, identity NativeIdentity) (runtimeAttempt, error) {
	index, ok := r.items[work.Item]
	if !ok || identity.WorkerID == "" || identity.ProcessIdentity == "" {
		return runtimeAttempt{}, invalid("runtime attempt identity")
	}
	id := attemptID(work.Item, work.Ordinal)
	if _, exists := r.attempts[id]; exists {
		return runtimeAttempt{}, invalid("duplicate runtime attempt")
	}
	if work.Ordinal > 1 {
		if _, exists := r.attempts[work.ParentID]; !exists {
			return runtimeAttempt{}, invalid("runtime retry parent")
		}
	}
	attempt := runtimeAttempt{
		item: r.workload.Items[index], id: id, parent: work.ParentID, ordinal: work.Ordinal,
		layer: work.RetryLayer, cause: work.RetryCause, owner: r.authority.CurrentOwnerID,
		generation: r.authority.CurrentGeneration, capability: r.authority.CurrentCapabilityHash,
		worker: identity.WorkerID, process: identity.ProcessIdentity,
	}
	event := r.addAttemptEventLocked(protocol.EventAttemptStarted, protocol.DecisionAccepted, attempt, nil)
	r.attempts[id] = attempt
	r.processes = append(r.processes, protocol.ProcessObservation{
		EventID: event.EventID, OwnerID: attempt.owner, Generation: attempt.generation,
		WorkerID: attempt.worker, ProcessIdentity: attempt.process, State: "attempt-delivered",
	})
	r.setItemStateLocked(work.Item, protocol.WorkItemRunning)
	return attempt, nil
}

func (r *EpisodeRuntime) acceptOutcomeLocked(attempt runtimeAttempt) {
	r.addAttemptEventLocked(protocol.EventOutcomeAccepted, protocol.DecisionAccepted, attempt, map[string]string{"action": "outcome"})
	r.addAttemptEventLocked(protocol.EventAcknowledged, protocol.DecisionAccepted, attempt, map[string]string{"action": "acknowledgement"})
}

func (r *EpisodeRuntime) commitFault(itemIndex int) {
	r.config.Timing.Record(protocol.EventBarrierReached, r.faultPoint(), map[string]string{"controller": "named-transition"})
	r.mu.Lock()
	item := r.itemLocked(itemIndex)
	r.faultAfter = r.addLocked(protocol.CausalEvent{
		Kind: protocol.EventFaultCommitted, Decision: protocol.DecisionAccepted,
		LogicalOperationID: item.LogicalOperationID, WorkItemID: item.WorkItemID,
		Details: map[string]string{"fault": r.faultPoint()},
	})
	if r.config.Request.Case == protocol.CaseOutageBacklogRecovery {
		r.transitionLocked(protocol.DependencyOutage, r.faultAfter.Time)
	}
	r.mu.Unlock()
	r.config.Timing.Record(protocol.EventFaultCommitted, r.faultPoint(), map[string]string{"after_event_id": r.faultAfter.EventID})
}

func (r *EpisodeRuntime) observeRecovery(itemIndex int) {
	r.mu.Lock()
	item := r.itemLocked(itemIndex)
	decision := protocol.DecisionAccepted
	details := map[string]string{"dependency": "healthy"}
	if r.config.Request.Case == protocol.CaseSilentProgress {
		if r.config.Request.Probe == protocol.ProbeUnsafe {
			decision = protocol.DecisionFailed
			details = map[string]string{"detection": "missing", "process_alive": "true"}
		} else {
			details = map[string]string{"detection": "progress_deadline", "process_alive": "true"}
		}
	}
	before := r.addLocked(protocol.CausalEvent{
		Kind: protocol.EventRecoveryObserved, Decision: decision,
		LogicalOperationID: item.LogicalOperationID, WorkItemID: item.WorkItemID, Details: details,
	})
	if r.config.Request.Case == protocol.CaseOutageBacklogRecovery {
		r.transitionLocked(protocol.DependencyRecovering, before.Time)
	}
	r.markFaultLocked(r.faultPoint(), r.faultAfter, before)
	r.mu.Unlock()
	r.config.Timing.Record(protocol.EventRecoveryObserved, r.faultPoint(), map[string]string{"before_event_id": before.EventID})
}

func (r *EpisodeRuntime) rejectAdmission(itemIndex int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.itemLocked(itemIndex)
	r.addLocked(protocol.CausalEvent{
		Kind: protocol.EventActionRejected, Decision: protocol.DecisionRejected,
		LogicalOperationID: item.LogicalOperationID, WorkItemID: item.WorkItemID,
		Details: map[string]string{"action": "admission", "reason": "queue_capacity"},
	})
	r.setItemStateLocked(itemIndex, protocol.WorkItemRejected)
}

func (r *EpisodeRuntime) observeSilent(itemIndex int, identity NativeIdentity) {
	r.mu.Lock()
	defer r.mu.Unlock()
	work := WorkSpec{Item: itemIndex, Ordinal: 1, RetryLayer: protocol.RetryLayerActivity}
	attempt, err := r.startAttemptLocked(work, identity)
	if err != nil {
		return
	}
	r.addAttemptEventLocked(protocol.EventProgressAccepted, protocol.DecisionAccepted, attempt, map[string]string{"progress": "registered"})
}

func (r *EpisodeRuntime) observeLegitimateWait(itemIndex int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.itemLocked(itemIndex)
	r.addLocked(protocol.CausalEvent{
		Kind: protocol.EventProgressAccepted, Decision: protocol.DecisionAccepted,
		LogicalOperationID: item.LogicalOperationID, WorkItemID: item.WorkItemID,
		Details: map[string]string{"progress": "declared_durable_wait"},
	})
}

func (r *EpisodeRuntime) replaceSilent(itemIndex int, identity NativeIdentity) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.config.Request.Probe != protocol.ProbeProtected {
		return
	}
	r.authority.Epochs[0].State = protocol.OwnerEpochRevoked
	capability := digest("capability/" + r.runID + "/2")
	item := r.itemLocked(itemIndex)
	ownerEvent := r.addLocked(protocol.CausalEvent{
		Kind: protocol.EventOwnerChanged, Decision: protocol.DecisionAccepted,
		LogicalOperationID: item.LogicalOperationID, WorkItemID: item.WorkItemID,
		ActorID: defaultOwner, Generation: 2, CapabilityHash: capability,
	})
	r.authority.CurrentGeneration = 2
	r.authority.CurrentCapabilityHash = capability
	r.authority.Epochs = append(r.authority.Epochs, protocol.OwnerEpoch{
		OwnerID: defaultOwner, Generation: 2, CapabilityHash: capability, State: protocol.OwnerEpochActive, Sequence: ownerEvent.Sequence,
	})
	work := WorkSpec{Item: itemIndex, Ordinal: 2, ParentID: attemptID(itemIndex, 1), RetryLayer: protocol.RetryLayerActivity, RetryCause: "progress_deadline"}
	attempt, err := r.startAttemptLocked(work, identity)
	if err != nil {
		return
	}
	r.addAttemptEventLocked(protocol.EventAttemptFinished, protocol.DecisionAccepted, attempt, map[string]string{"outcome": "ok"})
	r.setItemStateLocked(itemIndex, protocol.WorkItemSucceeded)
	r.acceptOutcomeLocked(attempt)
	stale := r.attempts[attemptID(itemIndex, 1)]
	rejected := r.addAttemptEventLocked(protocol.EventActionRejected, protocol.DecisionRejected, stale, map[string]string{"action": "publish", "reason": "stale_generation"})
	rejected.LogicalEffectID = "effect-silent-progress"
	rejected.PhysicalAttemptID = "stale-publish-" + r.runID
	r.events[rejected.Sequence-1] = rejected
	r.destination.Attempts = append(r.destination.Attempts, protocol.DestinationAttempt{
		LogicalOperationID: stale.item.LogicalOperationID, LogicalEffectID: rejected.LogicalEffectID,
		PhysicalAttemptID: rejected.PhysicalAttemptID, OwnerID: stale.owner, Generation: stale.generation,
		CapabilityHash: stale.capability, EventID: rejected.EventID, Decision: protocol.DecisionRejected, Applied: false,
	})
}

func (r *EpisodeRuntime) Finish(ctx context.Context, root string, native []protocol.NativeRecord) (TimedResult, error) {
	r.mu.Lock()
	if r.finished {
		r.mu.Unlock()
		return TimedResult{}, invalid("episode already finished")
	}
	r.finished = true
	for _, input := range r.config.Plan.Items {
		index := r.items[input.Index]
		if input.Poison && r.workload.Items[index].State != protocol.WorkItemSucceeded {
			r.workload.Items[index].State = protocol.WorkItemQuarantined
		}
	}
	if r.config.Request.Case == protocol.CaseSilentProgress && r.config.Request.Probe == protocol.ProbeUnsafe {
		r.setItemStateLocked(1, protocol.WorkItemRunning)
	}
	r.authority.AcceptedActions = nil
	for _, event := range r.events {
		if event.Decision == protocol.DecisionAccepted &&
			(event.Kind == protocol.EventActionAccepted || event.Kind == protocol.EventOutcomeAccepted || event.Kind == protocol.EventAcknowledged) {
			r.authority.AcceptedActions = append(r.authority.AcceptedActions, protocol.AcceptedAction{
				Kind: event.Details["action"], OwnerID: event.ActorID, Generation: event.Generation,
				CapabilityHash: event.CapabilityHash, EventID: event.EventID,
			})
		}
	}
	bundle := evidence.Bundle{
		Identity: evidence.RunIdentity{
			RunID: r.runID, Case: r.config.Request.Case, Probe: r.config.Request.Probe, Trial: r.config.Request.Slot,
			EpisodeID: r.episodeID, Seed: int64(r.config.Request.PairIndex), CohortSize: len(r.workload.Items),
		},
		Events: append([]protocol.CausalEvent(nil), r.events...), Authority: r.authority,
		Destination: r.destination, Dependency: r.dependency, Workload: r.workload, Fault: r.fault,
		Processes: append([]protocol.ProcessObservation(nil), r.processes...), Native: append([]protocol.NativeRecord(nil), native...),
		Input: protocol.EffectiveInput{
			AdapterID: r.config.AdapterID, AdapterVersion: r.config.AdapterVersion,
			AgentBinarySHA256: r.config.AgentBinarySHA256, SystemID: r.config.SystemID, Runtime: runtime.GOOS + "/" + runtime.GOARCH,
			AuthorityProtocol: protocol.AuthorityProtocol, DependencyProtocol: protocol.DependencyProtocol,
			FailureProtocol: protocol.FailureProtocol, OracleProtocol: protocol.OracleProtocol,
			DestinationID: r.destination.DestinationID, OracleVisibility: protocol.OracleVisibility(),
			HostLimits: map[string]int64{"workers": 8, "connections": 8, "queue_capacity": 20},
			Settings: mergeSettings(map[string]string{
				"clock": "go-monotonic-duration-with-utc-anchors", "case": string(r.config.Request.Case),
				"probe": string(r.config.Request.Probe), "retry_budget": "4", "retry_concurrency_limit": "4",
				"admission_capacity": "20", "poison_attempt_budget": "3", "healthy_latency_bound_ms": "1000", "progress_deadline_ms": "25",
			}, r.config.Settings),
		},
	}
	r.mu.Unlock()

	r.Acknowledge()
	runDir, err := evidence.WriteRun(ctx, root, bundle)
	if err != nil {
		return TimedResult{ExecutionID: r.runID, EvidenceDir: runDir}, err
	}
	verdict, err := oracle.EvaluateAndWrite(ctx, runDir)
	if err != nil {
		return TimedResult{ExecutionID: r.runID, EvidenceDir: runDir}, err
	}
	return TimedResult{ExecutionID: r.runID, EvidenceDir: runDir, Verdict: verdict}, nil
}

func (r *EpisodeRuntime) Acknowledge() {
	r.mu.Lock()
	if r.acknowledged {
		r.mu.Unlock()
		return
	}
	r.acknowledged = true
	r.mu.Unlock()
	r.config.Timing.Record(protocol.EventAcknowledged, "", map[string]string{"source": "system_outcome"})
}

func (r *EpisodeRuntime) addAttemptEventLocked(kind, decision string, attempt runtimeAttempt, details map[string]string) protocol.CausalEvent {
	return r.addLocked(protocol.CausalEvent{
		Kind: kind, Decision: decision, LogicalOperationID: attempt.item.LogicalOperationID, WorkItemID: attempt.item.WorkItemID,
		AttemptID: attempt.id, ParentAttemptID: attempt.parent, RetryLayer: attempt.layer, RetryOrdinal: attempt.ordinal,
		RetryCause: attempt.cause, ActorID: attempt.owner, Generation: attempt.generation, CapabilityHash: attempt.capability,
		WorkerID: attempt.worker, ProcessIdentity: attempt.process, Details: cloneDetails(details),
	})
}

func (r *EpisodeRuntime) addLocked(event protocol.CausalEvent) protocol.CausalEvent {
	event.Sequence = uint64(len(r.events) + 1)
	event.EventID = fmt.Sprintf("event-%d", event.Sequence)
	event.Time = r.nextTimeLocked().Format(time.RFC3339Nano)
	event.RunID = r.runID
	if len(event.ParentEventIDs) == 0 && len(r.events) > 0 {
		event.ParentEventIDs = []string{r.events[len(r.events)-1].EventID}
	}
	r.events = append(r.events, event)
	return event
}

func (r *EpisodeRuntime) nextTimeLocked() time.Time {
	now := r.clock.Now().UTC()
	if !r.lastTime.IsZero() && !now.After(r.lastTime) {
		now = r.lastTime.Add(10 * time.Nanosecond)
	}
	r.lastTime = now
	return now
}

func (r *EpisodeRuntime) transitionLocked(state protocol.DependencyStatus, at string) {
	r.dependency.Transitions = append(r.dependency.Transitions, protocol.DependencyTransition{
		Sequence: uint64(len(r.dependency.Transitions) + 1), Time: at, State: state,
	})
}

func (r *EpisodeRuntime) markFaultLocked(point string, after, before protocol.CausalEvent) {
	afterTime, _ := time.Parse(time.RFC3339Nano, after.Time)
	beforeTime, _ := time.Parse(time.RFC3339Nano, before.Time)
	r.fault = protocol.FaultBoundary{
		Point: point, Triggered: true, AfterSequence: after.Sequence, AfterEventID: after.EventID,
		BeforeSequence: before.Sequence, BeforeEventID: before.EventID,
		TriggeredAt: afterTime.Add(beforeTime.Sub(afterTime) / 2).Format(time.RFC3339Nano),
	}
}

func (r *EpisodeRuntime) setItemStateLocked(itemIndex int, state protocol.WorkItemState) {
	if index, ok := r.items[itemIndex]; ok {
		r.workload.Items[index].State = state
	}
}

func (r *EpisodeRuntime) itemLocked(itemIndex int) protocol.WorkItem {
	return r.workload.Items[r.items[itemIndex]]
}

func (r *EpisodeRuntime) faultPoint() string {
	return map[protocol.CaseID]string{
		protocol.CaseLayeredRetryAmplification: "failure-script-after-first-accepted-request",
		protocol.CaseOutageBacklogRecovery:     "outage-backlog-target-before-restoration",
		protocol.CaseBackpressureOverload:      "offered-load-gate-release",
		protocol.CasePoisonWorkIsolation:       "registered-poison-failure-release",
		protocol.CaseSilentProgress:            "silent-progress-after-first-heartbeat",
	}[r.config.Request.Case]
}

func observedRunID(systemID, pairID string) string {
	hash := sha256.Sum256([]byte(ProtocolVersion + "\x00" + systemID + "\x00" + pairID))
	return "observed-" + systemID + "-" + hex.EncodeToString(hash[:16])
}

func digest(value string) string {
	hash := sha256.Sum256([]byte(value))
	return hex.EncodeToString(hash[:])
}

func validDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func validSourceHash(value string) bool {
	const prefix = "source-sha256:"
	return len(value) > len(prefix) && value[:len(prefix)] == prefix && validDigest(value[len(prefix):])
}

func cloneDetails(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func mergeSettings(base, extra map[string]string) map[string]string {
	result := cloneDetails(base)
	for key, value := range extra {
		result[key] = value
	}
	return result
}

func nativeRecord(sequence int, kind, detail string, at time.Time) protocol.NativeRecord {
	return protocol.NativeRecord{Sequence: uint64(sequence), Time: at.UTC().Format(time.RFC3339Nano), Kind: kind, Detail: detail}
}

func mustEventTime(event protocol.CausalEvent) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, event.Time)
	if err != nil {
		panic("validated event time: " + err.Error())
	}
	return parsed
}
