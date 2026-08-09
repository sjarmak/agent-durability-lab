package abalive

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
	"github.com/sjarmak/temporal_projects/internal/workstore"
)

type recorder struct {
	mu          sync.Mutex
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
	attempts    map[string]bool
	requestOpen map[string]requestStart
	requestDone map[string]requestResult
	lastTime    time.Time
}

type requestStart struct {
	eventID string
	time    string
}

type requestResult struct {
	finishSequence uint64
	finishEventID  string
}

func newRecorder(runID string) *recorder {
	operationID := "operation-" + runID
	workItemID := "item-" + runID
	episodeID := "episode-" + runID
	return &recorder{
		runID: runID, operationID: operationID, workItemID: workItemID, episodeID: episodeID,
		destination: protocol.DestinationState{DestinationID: "aba-live-destination"},
		dependency: protocol.DependencyState{DependencyID: "aba-live-authority-api", Transitions: []protocol.DependencyTransition{{
			Sequence: 1, Time: time.Now().UTC().Format(time.RFC3339Nano), State: protocol.DependencyHealthy,
		}}},
		workload: protocol.WorkloadState{EpisodeID: episodeID, ExpectedWorkItems: 1, Items: []protocol.WorkItem{{
			WorkItemID: workItemID, LogicalOperationID: operationID, State: protocol.WorkItemRunning,
		}}},
		attempts: make(map[string]bool), requestOpen: make(map[string]requestStart), requestDone: make(map[string]requestResult),
	}
}

func (r *recorder) root() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.addLocked(protocol.CausalEvent{Kind: protocol.EventOperationReady, Decision: protocol.DecisionObserved}, nil)
}

func (r *recorder) ownerChanged(owner string, generation uint64, capabilityHash string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for index := range r.authority.Epochs {
		if r.authority.Epochs[index].State == protocol.OwnerEpochActive {
			state := protocol.OwnerEpochObsolete
			if r.authority.Epochs[index].OwnerID == "B" && r.authority.Epochs[index].Generation == 8 {
				state = protocol.OwnerEpochCompleted
			}
			r.authority.Epochs[index].State = state
		}
	}
	event := r.addLocked(protocol.CausalEvent{
		Kind: protocol.EventOwnerChanged, Decision: protocol.DecisionAccepted,
		ActorID: owner, Generation: generation, CapabilityHash: capabilityHash,
	}, nil)
	r.authority.LogicalOperationID = r.operationID
	r.authority.CurrentOwnerID = owner
	r.authority.CurrentGeneration = generation
	r.authority.CurrentCapabilityHash = capabilityHash
	r.authority.CurrentOwnerAlive = true
	r.authority.Epochs = append(r.authority.Epochs, protocol.OwnerEpoch{
		OwnerID: owner, Generation: generation, CapabilityHash: capabilityHash,
		State: protocol.OwnerEpochActive, Sequence: event.Sequence,
	})
	r.nativeLocked(event.Time, "authority_transition", fmt.Sprintf("owner=%s generation=%d capability_hash=%s", owner, generation, capabilityHash))
}

func (r *recorder) ensureAttempt(request ActionRequest) {
	if r.attempts[request.AttemptID] {
		return
	}
	event := r.addLocked(protocol.CausalEvent{
		Kind: protocol.EventAttemptStarted, Decision: protocol.DecisionAccepted,
		AttemptID: request.AttemptID, ParentAttemptID: request.ParentAttemptID,
		RetryLayer: protocol.RetryLayerActivity, RetryOrdinal: request.RetryOrdinal, RetryCause: request.RetryCause,
		ActorID: request.OwnerID, Generation: request.Generation, CapabilityHash: workstore.HashToken(request.Capability),
		WorkerID: request.WorkerID, ProcessIdentity: request.ProcessIdentity,
	}, nil)
	r.attempts[request.AttemptID] = true
	r.processes = append(r.processes, protocol.ProcessObservation{
		EventID: event.EventID, OwnerID: request.OwnerID, Generation: request.Generation,
		WorkerID: request.WorkerID, ProcessIdentity: request.ProcessIdentity, State: "running",
	})
	r.nativeLocked(event.Time, "client_registered", fmt.Sprintf("owner=%s generation=%d process=%s", request.OwnerID, request.Generation, request.ProcessIdentity))
}

func (r *recorder) requestStarted(request ActionRequest) protocol.CausalEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureAttempt(request)
	event := r.addLocked(r.eventForRequest(protocol.EventRequestStarted, protocol.DecisionObserved, request), nil)
	r.requestOpen[request.RequestID] = requestStart{eventID: event.EventID, time: event.Time}
	return event
}

func (r *recorder) barrier(request ActionRequest, parent string) protocol.CausalEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	event := r.addLocked(r.eventForRequest(protocol.EventBarrierReached, protocol.DecisionBlocked, request), []string{parent})
	r.nativeLocked(event.Time, "exact_barrier", fmt.Sprintf("request=%s owner=%s generation=%d", request.RequestID, request.OwnerID, request.Generation))
	return event
}

func (r *recorder) finish(request ActionRequest, accepted bool, reason, explicitParent string) requestResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	decision := protocol.DecisionRejected
	if accepted {
		decision = protocol.DecisionAccepted
	}
	start, found := r.requestOpen[request.RequestID]
	if !found {
		return requestResult{}
	}
	parents := []string{start.eventID}
	if explicitParent != "" && explicitParent != start.eventID {
		parents = append(parents, explicitParent)
	}
	finish := r.addLocked(r.eventForRequest(protocol.EventRequestFinished, decision, request), parents)
	actionKind := protocol.EventActionRejected
	if accepted {
		switch request.Action {
		case ActionOutcome:
			actionKind = protocol.EventOutcomeAccepted
		case ActionAcknowledgement:
			actionKind = protocol.EventAcknowledged
		case ActionEpochComplete:
			actionKind = protocol.EventAttemptFinished
		default:
			actionKind = protocol.EventActionAccepted
		}
	}
	action := r.addLocked(r.eventForRequest(actionKind, decision, request), []string{finish.EventID})
	capabilityHash := workstore.HashToken(request.Capability)
	if request.Action == ActionEffect {
		action.LogicalEffectID = "effect-aba"
		action.PhysicalAttemptID = request.RequestID
		r.events[action.Sequence-1] = action
		r.destination.Attempts = append(r.destination.Attempts, protocol.DestinationAttempt{
			LogicalOperationID: r.operationID, LogicalEffectID: "effect-aba", PhysicalAttemptID: request.RequestID,
			OwnerID: request.OwnerID, Generation: request.Generation, CapabilityHash: capabilityHash,
			EventID: action.EventID, Decision: decision, Applied: accepted,
		})
	}
	if accepted && request.Action != ActionEpochComplete {
		r.authority.AcceptedActions = append(r.authority.AcceptedActions, protocol.AcceptedAction{
			Kind: string(request.Action), OwnerID: request.OwnerID, Generation: request.Generation,
			CapabilityHash: capabilityHash, EventID: action.EventID,
		})
	}
	r.dependency.Requests = append(r.dependency.Requests, protocol.DependencyRequest{
		RequestID: request.RequestID, LogicalOperationID: r.operationID, WorkItemID: r.workItemID,
		AttemptID: request.AttemptID, ParentAttemptID: request.ParentAttemptID,
		RetryLayer: protocol.RetryLayerActivity, RetryOrdinal: request.RetryOrdinal,
		StartedAt: start.time, FinishedAt: finish.Time, Outcome: reason, CostUnits: 1,
	})
	if request.Action == ActionStop && accepted {
		r.authority.CurrentOwnerAlive = false
		for index := range r.processes {
			if r.processes[index].Generation == r.authority.CurrentGeneration {
				r.processes[index].State = "stopped_by_stale_request"
			}
		}
	}
	if request.Action == ActionAcknowledgement && accepted && request.Generation == r.authority.CurrentGeneration {
		r.workload.Items[0].State = protocol.WorkItemSucceeded
	}
	r.nativeLocked(action.Time, "authority_response", fmt.Sprintf("request=%s action=%s accepted=%t reason=%s", request.RequestID, request.Action, accepted, reason))
	result := requestResult{finishSequence: finish.Sequence, finishEventID: finish.EventID}
	r.requestDone[request.RequestID] = result
	return result
}

func (r *recorder) clientCompleted(owner string, generation uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for index := range r.processes {
		if r.processes[index].OwnerID == owner && r.processes[index].Generation == generation && r.processes[index].State == "running" {
			r.processes[index].State = "completed"
		}
	}
}

func (r *recorder) eventForRequest(kind, decision string, request ActionRequest) protocol.CausalEvent {
	return protocol.CausalEvent{
		Kind: kind, Decision: decision, AttemptID: request.AttemptID, ParentAttemptID: request.ParentAttemptID,
		RetryLayer: protocol.RetryLayerActivity, RetryOrdinal: request.RetryOrdinal, RetryCause: request.RetryCause,
		ActorID: request.OwnerID, Generation: request.Generation, CapabilityHash: workstore.HashToken(request.Capability),
		WorkerID: request.WorkerID, ProcessIdentity: request.ProcessIdentity, DependencyRequestID: request.RequestID,
		Details: map[string]string{"action": string(request.Action)},
	}
}

func (r *recorder) addLocked(event protocol.CausalEvent, parents []string) protocol.CausalEvent {
	sequence := uint64(len(r.events) + 1)
	event.Sequence = sequence
	event.EventID = fmt.Sprintf("event-%d", sequence)
	now := time.Now().UTC()
	if now.Before(r.lastTime) {
		now = r.lastTime
	}
	event.Time = now.Format(time.RFC3339Nano)
	event.RunID = r.runID
	event.LogicalOperationID = r.operationID
	event.WorkItemID = r.workItemID
	if len(parents) != 0 {
		event.ParentEventIDs = append([]string(nil), parents...)
	} else if len(r.events) != 0 {
		event.ParentEventIDs = []string{r.events[len(r.events)-1].EventID}
	}
	r.events = append(r.events, event)
	r.lastTime = now
	return event
}

func (r *recorder) nativeLocked(eventTime, kind, detail string) {
	r.native = append(r.native, protocol.NativeRecord{
		Sequence: uint64(len(r.native) + 1), Time: eventTime, Kind: kind, Detail: detail,
	})
}

func (r *recorder) markFault(after protocol.CausalEvent, triggeredAt time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !triggeredAt.After(parseTime(after.Time)) {
		triggeredAt = parseTime(after.Time).Add(time.Nanosecond)
	}
	r.fault = protocol.FaultBoundary{
		Point: "g7-delayed-until-g9-current", Triggered: true,
		AfterSequence: after.Sequence, AfterEventID: after.EventID, TriggeredAt: triggeredAt.UTC().Format(time.RFC3339Nano),
	}
}

func (r *recorder) finishFault(requestID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	result, found := r.requestDone[requestID]
	if !found {
		return fmt.Errorf("%w: delayed request has no finish event", protocol.ErrInvalidEvidence)
	}
	r.fault.BeforeSequence = result.finishSequence
	r.fault.BeforeEventID = result.finishEventID
	return nil
}

func (r *recorder) snapshot() recordedEvidence {
	r.mu.Lock()
	defer r.mu.Unlock()
	return recordedEvidence{
		events: append([]protocol.CausalEvent(nil), r.events...), authority: r.authority,
		destination: r.destination, dependency: r.dependency, workload: r.workload, fault: r.fault,
		processes: append([]protocol.ProcessObservation(nil), r.processes...), native: append([]protocol.NativeRecord(nil), r.native...),
	}
}

type recordedEvidence struct {
	events      []protocol.CausalEvent
	authority   protocol.AuthorityState
	destination protocol.DestinationState
	dependency  protocol.DependencyState
	workload    protocol.WorkloadState
	fault       protocol.FaultBoundary
	processes   []protocol.ProcessObservation
	native      []protocol.NativeRecord
}

func (r recordedEvidence) nativeJSON() []protocol.NativeRecord {
	result := append([]protocol.NativeRecord(nil), r.native...)
	for _, event := range r.events {
		data, err := json.Marshal(event)
		if err != nil {
			continue
		}
		result = append(result, protocol.NativeRecord{
			Sequence: uint64(len(result) + 1), Time: event.Time, Kind: "causal_event_copy", Detail: string(data),
		})
	}
	return result
}

func parseTime(value string) time.Time {
	parsed, _ := time.Parse(time.RFC3339Nano, value)
	return parsed
}
