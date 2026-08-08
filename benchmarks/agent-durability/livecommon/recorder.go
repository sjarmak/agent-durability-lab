package livecommon

import (
	"fmt"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/evidence"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

type recorder struct {
	identity    evidence.RunIdentity
	events      []protocol.Event
	authority   protocol.AuthorityState
	destination protocol.DestinationState
	fault       protocol.FaultBoundary
	processes   []protocol.ProcessObservation
	lastTime    time.Time
}

func newRecorder(identity evidence.RunIdentity) *recorder {
	return &recorder{
		identity: identity,
		authority: protocol.AuthorityState{
			SessionID: identity.SessionID, ActiveGeneration: 1,
			ConcurrentOwnerCount: 1, CurrentOwnerAlive: true,
		},
		destination: protocol.DestinationState{DestinationID: "live-common-destination"},
	}
}

func (r *recorder) add(kind, actor string, generation uint64, process, logicalEffect, attempt, decision string) uint64 {
	now := time.Now().UTC()
	if !now.After(r.lastTime) {
		now = r.lastTime.Add(time.Nanosecond)
	}
	r.lastTime = now
	sequence := uint64(len(r.events) + 1)
	r.events = append(r.events, protocol.Event{
		Sequence: sequence, Time: now.Format(time.RFC3339Nano), Kind: kind,
		SessionID: r.identity.SessionID, ActorID: actor, Generation: generation,
		ProcessIdentity: process, LogicalEffectID: logicalEffect,
		PhysicalAttemptID: attempt, Decision: decision,
	})
	return sequence
}

func (r *recorder) register(actor string, generation uint64, process, decision, state string) uint64 {
	sequence := r.add(protocol.EventExecutorRegistered, actor, generation, process, "", "", decision)
	r.processes = append(r.processes, protocol.ProcessObservation{
		Sequence: sequence, ActorID: actor, Generation: generation,
		ProcessIdentity: process, State: state,
	})
	if generation > r.authority.ActiveGeneration {
		r.authority.ActiveGeneration = generation
	}
	return sequence
}

func (r *recorder) effect(kind, actor string, generation uint64, process, attempt, decision string, applied bool) uint64 {
	sequence := r.add(kind, actor, generation, process, "effect-1", attempt, decision)
	r.destination.Attempts = append(r.destination.Attempts, protocol.DestinationAttempt{
		LogicalEffectID: "effect-1", PhysicalAttemptID: attempt,
		Generation: generation, Sequence: sequence, Applied: applied,
	})
	if kind == protocol.EventEffectAccepted && decision == "accepted" {
		r.authority.AcceptedActions = append(r.authority.AcceptedActions, protocol.AcceptedAction{
			Kind: "effect", Generation: generation, Sequence: sequence,
		})
	}
	return sequence
}

func (r *recorder) outcome(actor string, generation uint64, process string) uint64 {
	sequence := r.add(protocol.EventOutcomeAccepted, actor, generation, process, "", "", "accepted")
	r.authority.AcceptedOutcomes = append(r.authority.AcceptedOutcomes, protocol.AcceptedAction{
		Kind: "outcome", Generation: generation, Sequence: sequence,
	})
	return sequence
}

func (r *recorder) acceptedStaleAction(kind, action, actor string, generation uint64, process string) uint64 {
	sequence := r.add(kind, actor, generation, process, "", "", "accepted")
	r.authority.AcceptedActions = append(r.authority.AcceptedActions, protocol.AcceptedAction{
		Kind: action, Generation: generation, Sequence: sequence,
	})
	return sequence
}

func (r *recorder) markFault(point, actor, process string, after uint64) {
	faultTime := time.Now().UTC()
	if !faultTime.After(r.lastTime) {
		faultTime = r.lastTime.Add(time.Nanosecond)
	}
	r.lastTime = faultTime
	r.fault = protocol.FaultBoundary{
		Point: point, Triggered: true, AfterSequence: after,
		BeforeSequence: after + 1, ActorID: actor, ProcessIdentity: process,
		TriggeredAt: faultTime.Format(time.RFC3339Nano),
	}
}

func (r *recorder) bundle(native []protocol.NativeRecord, input protocol.EffectiveInput) evidence.Bundle {
	return evidence.Bundle{
		Identity: r.identity, Events: r.events, Authority: r.authority,
		Destination: r.destination, Fault: r.fault, Processes: r.processes,
		Native: native, Input: input,
	}
}

func (r *recorder) requireNextFaultEvent() error {
	if !r.fault.Triggered {
		return nil
	}
	if r.fault.BeforeSequence != uint64(len(r.events)) {
		return fmt.Errorf("fault boundary expected event %d, observed %d", r.fault.BeforeSequence, len(r.events))
	}
	return nil
}
