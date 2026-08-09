// Package calibration produces deterministic v2 apparatus evidence. It is not
// evidence about a durability system.
package calibration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"runtime"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/evidence"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
)

type Config struct {
	Root              string
	Case              protocol.CaseID
	Probe             protocol.Probe
	Trial             int
	AdapterID         string
	AdapterVersion    string
	AgentBinarySHA256 string
	SystemID          string
	Runtime           string
	Native            []protocol.NativeRecord
	Settings          map[string]string
}

func Run(ctx context.Context, config Config) (string, error) {
	if config.Root == "" || !config.Case.Valid() || !config.Probe.Valid() || config.Trial < 1 {
		return "", fmt.Errorf("%w: supported case, probe, root, and positive trial are required", protocol.ErrInvalidEvidence)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	runID := fmt.Sprintf("%s-%s-calibration-trial-%d", config.Case, config.Probe, config.Trial)
	if config.Case != protocol.CaseABAReacquisition {
		return runRecovery(ctx, config, runID)
	}
	builder := newBuilder(runID, config.Trial)
	if config.Probe == protocol.ProbeUnfaulted {
		builder.unfaulted()
	} else {
		builder.aba(config.Probe)
	}
	return evidence.WriteRun(ctx, config.Root, builder.bundle(config, runID))
}

type builder struct {
	now         time.Time
	runID       string
	operationID string
	workItemID  string
	episodeID   string
	events      []protocol.CausalEvent
	authority   protocol.AuthorityState
	destination protocol.DestinationState
	dependency  protocol.DependencyState
	workload    protocol.WorkloadState
	fault       protocol.FaultBoundary
	processes   []protocol.ProcessObservation
	native      []protocol.NativeRecord
}

func newBuilder(runID string, trial int) *builder {
	operationID := "operation-" + runID
	workItemID := "item-" + runID
	episodeID := "episode-" + runID
	return &builder{
		now: time.Date(2026, 8, 8, 1, trial, 0, 0, time.UTC), runID: runID,
		operationID: operationID, workItemID: workItemID, episodeID: episodeID,
		destination: protocol.DestinationState{DestinationID: "aba-calibration-destination"},
		dependency: protocol.DependencyState{DependencyID: "aba-calibration-dependency", Transitions: []protocol.DependencyTransition{{
			Sequence: 1, Time: time.Date(2026, 8, 8, 1, trial, 0, 0, time.UTC).Format(time.RFC3339Nano), State: protocol.DependencyHealthy,
		}}},
		workload: protocol.WorkloadState{EpisodeID: episodeID, ExpectedWorkItems: 1, Items: []protocol.WorkItem{{
			WorkItemID: workItemID, LogicalOperationID: operationID, State: protocol.WorkItemSucceeded,
		}}},
	}
}

func (b *builder) unfaulted() {
	capability := hashString("capability-g7")
	b.addRoot(protocol.EventOperationReady, protocol.DecisionObserved)
	b.addAttemptStart("attempt-g7", "", 1, "", "A", 7, capability, "worker-1", "pid:107:start:fixture", "event-1")
	effect := b.addAttemptEvent(protocol.EventActionAccepted, protocol.DecisionAccepted, "effect", "attempt-g7", "", 1, "", "A", 7, capability, "event-2")
	b.events[effect-1].LogicalEffectID = "effect-aba"
	b.events[effect-1].PhysicalAttemptID = "physical-g7"
	b.destination.Attempts = append(b.destination.Attempts, protocol.DestinationAttempt{
		LogicalOperationID: b.operationID, LogicalEffectID: "effect-aba", PhysicalAttemptID: "physical-g7",
		OwnerID: "A", Generation: 7, CapabilityHash: capability, EventID: b.eventID(effect), Decision: protocol.DecisionAccepted, Applied: true,
	})
	b.dependency.Requests = append(b.dependency.Requests, protocol.DependencyRequest{
		RequestID: "request-g7", LogicalOperationID: b.operationID, WorkItemID: b.workItemID,
		AttemptID: "attempt-g7", RetryLayer: protocol.RetryLayerActivity, RetryOrdinal: 1,
		StartedAt: b.eventTime(2), FinishedAt: b.eventTime(effect), Outcome: "accepted", CostUnits: 1,
	})
	outcome := b.addAttemptEvent(protocol.EventOutcomeAccepted, protocol.DecisionAccepted, "outcome", "attempt-g7", "", 1, "", "A", 7, capability, b.eventID(effect))
	ack := b.addAttemptEvent(protocol.EventAcknowledged, protocol.DecisionAccepted, "acknowledgement", "attempt-g7", "", 1, "", "A", 7, capability, b.eventID(outcome))
	b.authority = protocol.AuthorityState{
		LogicalOperationID: b.operationID, CurrentOwnerID: "A", CurrentGeneration: 7,
		CurrentCapabilityHash: capability, CurrentOwnerAlive: true,
		Epochs: []protocol.OwnerEpoch{{OwnerID: "A", Generation: 7, CapabilityHash: capability, State: protocol.OwnerEpochActive, Sequence: 2}},
		AcceptedActions: []protocol.AcceptedAction{
			{Kind: "effect", OwnerID: "A", Generation: 7, CapabilityHash: capability, EventID: b.eventID(effect)},
			{Kind: "outcome", OwnerID: "A", Generation: 7, CapabilityHash: capability, EventID: b.eventID(outcome)},
			{Kind: "acknowledgement", OwnerID: "A", Generation: 7, CapabilityHash: capability, EventID: b.eventID(ack)},
		},
	}
	b.processes = []protocol.ProcessObservation{{EventID: "event-2", OwnerID: "A", Generation: 7, WorkerID: "worker-1", ProcessIdentity: "pid:107:start:fixture", State: "running"}}
	b.native = []protocol.NativeRecord{{Sequence: 1, Time: b.eventTime(2), Kind: "attempt_started", Detail: "generation 7"}, {Sequence: 2, Time: b.eventTime(ack), Kind: "acknowledged", Detail: "current generation"}}
}

func (b *builder) aba(probe protocol.Probe) {
	cap7 := hashString("capability-g7")
	cap8 := hashString("capability-g8")
	cap9 := hashString("capability-g9")
	b.addRoot(protocol.EventOperationReady, protocol.DecisionObserved)
	b.addAttemptStart("attempt-g7", "", 1, "", "A", 7, cap7, "worker-1", "pid:107:start:fixture", "event-1")
	requestStarted := b.addAttemptEvent(protocol.EventRequestStarted, protocol.DecisionObserved, "request", "attempt-g7", "", 1, "", "A", 7, cap7, "event-2")
	b.events[requestStarted-1].DependencyRequestID = "request-g7"
	barrier := b.addAttemptEvent(protocol.EventBarrierReached, protocol.DecisionBlocked, "delay", "attempt-g7", "", 1, "", "A", 7, cap7, b.eventID(requestStarted))
	b.addOwnerChanged("B", 8, cap8, b.eventID(barrier))
	b.addAttemptStart("attempt-g8", "attempt-g7", 2, "owner_replaced", "B", 8, cap8, "worker-2", "pid:208:start:fixture", "event-5")
	b.addAttemptEvent(protocol.EventAttemptFinished, protocol.DecisionAccepted, "epoch_completed", "attempt-g8", "attempt-g7", 2, "owner_replaced", "B", 8, cap8, "event-6")
	b.addOwnerChanged("A", 9, cap9, "event-7")
	b.addAttemptStart("attempt-g9", "attempt-g8", 3, "owner_reacquired", "A", 9, cap9, "worker-3", "pid:309:start:fixture", "event-8")

	staleDecision := protocol.DecisionRejected
	if probe == protocol.ProbeUnsafe {
		staleDecision = protocol.DecisionAccepted
	}
	requestFinished := b.addAttemptEvent(protocol.EventRequestFinished, staleDecision, "request", "attempt-g7", "", 1, "", "A", 7, cap7, "event-4", "event-9")
	b.events[requestFinished-1].DependencyRequestID = "request-g7"
	b.dependency.Requests = append(b.dependency.Requests, protocol.DependencyRequest{
		RequestID: "request-g7", LogicalOperationID: b.operationID, WorkItemID: b.workItemID,
		AttemptID: "attempt-g7", RetryLayer: protocol.RetryLayerActivity, RetryOrdinal: 1,
		StartedAt: b.eventTime(requestStarted), FinishedAt: b.eventTime(requestFinished), Outcome: map[bool]string{true: "stale_accepted", false: "stale_rejected"}[probe == protocol.ProbeUnsafe], CostUnits: 1,
	})
	staleEffectKind := protocol.EventActionRejected
	if probe == protocol.ProbeUnsafe {
		staleEffectKind = protocol.EventActionAccepted
	}
	staleEffect := b.addAttemptEvent(staleEffectKind, staleDecision, "effect", "attempt-g7", "", 1, "", "A", 7, cap7, b.eventID(requestFinished))
	b.events[staleEffect-1].LogicalEffectID = "effect-aba"
	b.events[staleEffect-1].PhysicalAttemptID = "physical-g7"
	b.destination.Attempts = append(b.destination.Attempts, protocol.DestinationAttempt{
		LogicalOperationID: b.operationID, LogicalEffectID: "effect-aba", PhysicalAttemptID: "physical-g7",
		OwnerID: "A", Generation: 7, CapabilityHash: cap7, EventID: b.eventID(staleEffect), Decision: staleDecision, Applied: probe == protocol.ProbeUnsafe,
	})
	staleCompletion := b.addAttemptEvent(map[bool]string{true: protocol.EventActionAccepted, false: protocol.EventActionRejected}[probe == protocol.ProbeUnsafe], staleDecision, "completion", "attempt-g7", "", 1, "", "A", 7, cap7, b.eventID(staleEffect))
	staleAck := b.addAttemptEvent(map[bool]string{true: protocol.EventActionAccepted, false: protocol.EventActionRejected}[probe == protocol.ProbeUnsafe], staleDecision, "acknowledgement", "attempt-g7", "", 1, "", "A", 7, cap7, b.eventID(staleCompletion))
	staleStop := b.addAttemptEvent(map[bool]string{true: protocol.EventActionAccepted, false: protocol.EventActionRejected}[probe == protocol.ProbeUnsafe], staleDecision, "stop", "attempt-g7", "", 1, "", "A", 7, cap7, b.eventID(staleAck))

	currentRequest := b.addAttemptEvent(protocol.EventRequestStarted, protocol.DecisionObserved, "request", "attempt-g9", "attempt-g8", 3, "owner_reacquired", "A", 9, cap9, "event-9")
	b.events[currentRequest-1].DependencyRequestID = "request-g9"
	currentFinished := b.addAttemptEvent(protocol.EventRequestFinished, protocol.DecisionAccepted, "request", "attempt-g9", "attempt-g8", 3, "owner_reacquired", "A", 9, cap9, b.eventID(currentRequest))
	b.events[currentFinished-1].DependencyRequestID = "request-g9"
	b.dependency.Requests = append(b.dependency.Requests, protocol.DependencyRequest{
		RequestID: "request-g9", LogicalOperationID: b.operationID, WorkItemID: b.workItemID,
		AttemptID: "attempt-g9", ParentAttemptID: "attempt-g8", RetryLayer: protocol.RetryLayerActivity, RetryOrdinal: 3,
		StartedAt: b.eventTime(currentRequest), FinishedAt: b.eventTime(currentFinished), Outcome: "accepted", CostUnits: 1,
	})
	currentEffect := b.addAttemptEvent(protocol.EventActionAccepted, protocol.DecisionAccepted, "effect", "attempt-g9", "attempt-g8", 3, "owner_reacquired", "A", 9, cap9, b.eventID(currentFinished))
	b.events[currentEffect-1].LogicalEffectID = "effect-aba"
	b.events[currentEffect-1].PhysicalAttemptID = "physical-g9"
	b.destination.Attempts = append(b.destination.Attempts, protocol.DestinationAttempt{
		LogicalOperationID: b.operationID, LogicalEffectID: "effect-aba", PhysicalAttemptID: "physical-g9",
		OwnerID: "A", Generation: 9, CapabilityHash: cap9, EventID: b.eventID(currentEffect), Decision: protocol.DecisionAccepted, Applied: true,
	})
	currentOutcome := b.addAttemptEvent(protocol.EventOutcomeAccepted, protocol.DecisionAccepted, "outcome", "attempt-g9", "attempt-g8", 3, "owner_reacquired", "A", 9, cap9, b.eventID(currentEffect))
	currentAck := b.addAttemptEvent(protocol.EventAcknowledged, protocol.DecisionAccepted, "acknowledgement", "attempt-g9", "attempt-g8", 3, "owner_reacquired", "A", 9, cap9, b.eventID(currentOutcome))

	accepted := []protocol.AcceptedAction{
		{Kind: "effect", OwnerID: "A", Generation: 9, CapabilityHash: cap9, EventID: b.eventID(currentEffect)},
		{Kind: "outcome", OwnerID: "A", Generation: 9, CapabilityHash: cap9, EventID: b.eventID(currentOutcome)},
		{Kind: "acknowledgement", OwnerID: "A", Generation: 9, CapabilityHash: cap9, EventID: b.eventID(currentAck)},
	}
	if probe == protocol.ProbeUnsafe {
		accepted = append([]protocol.AcceptedAction{
			{Kind: "effect", OwnerID: "A", Generation: 7, CapabilityHash: cap7, EventID: b.eventID(staleEffect)},
			{Kind: "completion", OwnerID: "A", Generation: 7, CapabilityHash: cap7, EventID: b.eventID(staleCompletion)},
			{Kind: "acknowledgement", OwnerID: "A", Generation: 7, CapabilityHash: cap7, EventID: b.eventID(staleAck)},
			{Kind: "stop", OwnerID: "A", Generation: 7, CapabilityHash: cap7, EventID: b.eventID(staleStop)},
		}, accepted...)
	}
	b.authority = protocol.AuthorityState{
		LogicalOperationID: b.operationID, CurrentOwnerID: "A", CurrentGeneration: 9,
		CurrentCapabilityHash: cap9, CurrentOwnerAlive: probe != protocol.ProbeUnsafe,
		Epochs: []protocol.OwnerEpoch{
			{OwnerID: "A", Generation: 7, CapabilityHash: cap7, State: protocol.OwnerEpochObsolete, Sequence: 2},
			{OwnerID: "B", Generation: 8, CapabilityHash: cap8, State: protocol.OwnerEpochCompleted, Sequence: 5},
			{OwnerID: "A", Generation: 9, CapabilityHash: cap9, State: protocol.OwnerEpochActive, Sequence: 8},
		},
		AcceptedActions: accepted,
	}
	b.fault = protocol.FaultBoundary{
		Point: "g7-delayed-until-g9-current", Triggered: true,
		AfterSequence: barrier, AfterEventID: b.eventID(barrier), BeforeSequence: requestFinished, BeforeEventID: b.eventID(requestFinished),
		TriggeredAt: b.now.Add(time.Duration(barrier)*time.Millisecond + time.Microsecond).Format(time.RFC3339Nano),
	}
	b.processes = []protocol.ProcessObservation{
		{EventID: "event-2", OwnerID: "A", Generation: 7, WorkerID: "worker-1", ProcessIdentity: "pid:107:start:fixture", State: "delayed"},
		{EventID: "event-6", OwnerID: "B", Generation: 8, WorkerID: "worker-2", ProcessIdentity: "pid:208:start:fixture", State: "completed"},
		{EventID: "event-9", OwnerID: "A", Generation: 9, WorkerID: "worker-3", ProcessIdentity: "pid:309:start:fixture", State: map[bool]string{true: "stopped_by_stale_request", false: "running"}[probe == protocol.ProbeUnsafe]},
	}
	b.native = []protocol.NativeRecord{
		{Sequence: 1, Time: b.eventTime(barrier), Kind: "request_delayed", Detail: "A generation 7"},
		{Sequence: 2, Time: b.eventTime(8), Kind: "owner_reacquired", Detail: "A generation 9"},
		{Sequence: 3, Time: b.eventTime(currentAck), Kind: "run_completed", Detail: string(probe)},
	}
}

func (b *builder) bundle(config Config, runID string) evidence.Bundle {
	adapterID := config.AdapterID
	if adapterID == "" {
		adapterID = "calibration-v2"
	}
	adapterVersion := config.AdapterVersion
	if adapterVersion == "" {
		adapterVersion = "source-sha256:" + hashString("aba-calibration-v2")
	}
	agentHash := config.AgentBinarySHA256
	if agentHash == "" {
		agentHash = hashString("calibration-fixture-agent")
	}
	systemID := config.SystemID
	if systemID == "" {
		systemID = "calibration"
	}
	runtimeID := config.Runtime
	if runtimeID == "" {
		runtimeID = runtime.GOOS + "/" + runtime.GOARCH
	}
	settings := map[string]string{"case": string(config.Case), "probe": string(config.Probe), "clock": "deterministic"}
	for name, value := range config.Settings {
		settings[name] = value
	}
	native := append([]protocol.NativeRecord(nil), config.Native...)
	native = append(native, b.native...)
	for index := range native {
		native[index].Sequence = uint64(index + 1)
	}
	return evidence.Bundle{
		Identity: evidence.RunIdentity{
			RunID: runID, Case: config.Case, Probe: config.Probe, Trial: config.Trial,
			EpisodeID: b.episodeID, Seed: int64(config.Trial), CohortSize: 1,
		},
		Events: b.events, Authority: b.authority, Destination: b.destination, Dependency: b.dependency,
		Workload: b.workload, Fault: b.fault, Processes: b.processes, Native: native,
		Input: protocol.EffectiveInput{
			AdapterID: adapterID, AdapterVersion: adapterVersion,
			AgentBinarySHA256: agentHash,
			SystemID:          systemID, Runtime: runtimeID,
			AuthorityProtocol: protocol.AuthorityProtocol, DependencyProtocol: protocol.DependencyProtocol,
			FailureProtocol: protocol.FailureProtocol, OracleProtocol: protocol.OracleProtocol,
			DestinationID: b.destination.DestinationID, OracleVisibility: protocol.OracleVisibility(),
			HostLimits: map[string]int64{"workers": 3, "processes": 3},
			Settings:   settings,
		},
	}
}

func (b *builder) addRoot(kind, decision string) uint64 {
	return b.add(protocol.CausalEvent{Kind: kind, Decision: decision})
}

func (b *builder) addOwnerChanged(owner string, generation uint64, capability string, parent string) uint64 {
	return b.add(protocol.CausalEvent{
		Kind: protocol.EventOwnerChanged, Decision: protocol.DecisionAccepted, ParentEventIDs: []string{parent},
		ActorID: owner, Generation: generation, CapabilityHash: capability,
	})
}

func (b *builder) addAttemptStart(attempt, parentAttempt string, ordinal int, cause, owner string, generation uint64, capability, worker, process, parentEvent string) uint64 {
	return b.add(protocol.CausalEvent{
		Kind: protocol.EventAttemptStarted, Decision: protocol.DecisionAccepted, ParentEventIDs: []string{parentEvent},
		AttemptID: attempt, ParentAttemptID: parentAttempt, RetryLayer: protocol.RetryLayerActivity,
		RetryOrdinal: ordinal, RetryCause: cause, ActorID: owner, Generation: generation, CapabilityHash: capability,
		WorkerID: worker, ProcessIdentity: process,
	})
}

func (b *builder) addAttemptEvent(kind, decision, action, attempt, parentAttempt string, ordinal int, cause, owner string, generation uint64, capability string, parents ...string) uint64 {
	return b.add(protocol.CausalEvent{
		Kind: kind, Decision: decision, ParentEventIDs: parents,
		AttemptID: attempt, ParentAttemptID: parentAttempt, RetryLayer: protocol.RetryLayerActivity,
		RetryOrdinal: ordinal, RetryCause: cause, ActorID: owner, Generation: generation, CapabilityHash: capability,
		Details: map[string]string{"action": action},
	})
}

func (b *builder) add(event protocol.CausalEvent) uint64 {
	sequence := uint64(len(b.events) + 1)
	event.Sequence = sequence
	event.EventID = b.eventID(sequence)
	event.Time = b.eventTime(sequence)
	event.RunID = b.runID
	event.LogicalOperationID = b.operationID
	event.WorkItemID = b.workItemID
	b.events = append(b.events, event)
	return sequence
}

func (b *builder) eventID(sequence uint64) string { return fmt.Sprintf("event-%d", sequence) }

func (b *builder) eventTime(sequence uint64) string {
	return b.now.Add(time.Duration(sequence) * time.Millisecond).Format(time.RFC3339Nano)
}

func hashString(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
