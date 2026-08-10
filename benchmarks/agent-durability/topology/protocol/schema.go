package protocol

import (
	"fmt"
	"slices"
	"time"
)

type Identity struct {
	ProtocolVersion    string   `json:"protocol_version"`
	RunID              string   `json:"run_id"`
	PairID             string   `json:"pair_id"`
	ScheduleBlockID    string   `json:"schedule_block_id"`
	TrackerBeadID      string   `json:"tracker_bead_id"`
	Topology           Topology `json:"topology"`
	Case               CaseID   `json:"case_id"`
	Boundary           string   `json:"boundary_id"`
	Probe              Probe    `json:"probe"`
	Fanout             int      `json:"fanout"`
	LogicalOperationID string   `json:"logical_operation_id"`
	WorkItemID         string   `json:"work_item_id"`
	Generation         uint64   `json:"generation"`
	CapabilityHash     string   `json:"capability_hash"`
	ParentWorkflowID   string   `json:"parent_workflow_id"`
	ParentRunID        string   `json:"parent_run_id"`
	ChildWorkflowID    string   `json:"child_workflow_id,omitempty"`
	ChildRunID         string   `json:"child_run_id,omitempty"`
	ActivityID         string   `json:"activity_id"`
	ActivityAttempt    int      `json:"activity_attempt"`
	WorkerID           string   `json:"worker_id"`
	WorkerPID          int      `json:"worker_pid"`
	ProcessIdentity    string   `json:"process_identity"`
}

func (i Identity) Validate() error {
	if i.ProtocolVersion != PublicationProtocolVersion || i.RunID == "" || i.PairID == "" || i.ScheduleBlockID == "" ||
		i.TrackerBeadID == "" || !i.Topology.Valid() || !i.Case.Valid() || i.Boundary == "" || !i.Probe.Valid() ||
		!slices.Contains([]int{8, 32, 128}, i.Fanout) || i.LogicalOperationID == "" || i.WorkItemID == "" ||
		i.Generation == 0 || !validSHA256(i.CapabilityHash) || i.ParentWorkflowID == "" || i.ParentRunID == "" ||
		i.ActivityID == "" || i.ActivityAttempt < 1 || i.WorkerID == "" || i.WorkerPID < 1 || i.ProcessIdentity == "" {
		return invalid("incomplete stable identity")
	}
	if i.Topology == TopologyDirectActivity && (i.ChildWorkflowID != "" || i.ChildRunID != "") {
		return invalid("direct Activity identity names a Child Workflow")
	}
	if i.Topology == TopologyChildWorkflow && (i.ChildWorkflowID == "") != (i.ChildRunID == "") {
		return invalid("partial Child Workflow identity")
	}
	if i.Probe == ProbeUnfaulted && i.Boundary != UnfaultedBoundary || i.Probe != ProbeUnfaulted && i.Boundary == UnfaultedBoundary {
		return invalid("identity boundary and probe")
	}
	return nil
}

const (
	EventInputRegistered         = "input_registered"
	EventParentWorkflowStarted   = "parent_workflow_started"
	EventChildWorkflowStarted    = "child_workflow_started"
	EventActivityScheduled       = "activity_scheduled"
	EventActivityStarted         = "activity_started"
	EventProcessStarted          = "process_started"
	EventBarrierReached          = "barrier_reached"
	EventFaultCommitted          = "fault_committed"
	EventDependencyStarted       = "dependency_request_started"
	EventDependencyFinished      = "dependency_request_finished"
	EventDependencyStateChanged  = "dependency_state_changed"
	EventAdmissionDecided        = "admission_decided"
	EventProgressAccepted        = "progress_accepted"
	EventProgressDeadlineCreated = "progress_deadline_created"
	EventAuthorityRevoked        = "authority_revoked"
	EventRetryBudgetExhausted    = "retry_budget_exhausted"
	EventItemQuarantined         = "item_quarantined"
	EventEffectAccepted          = "effect_accepted"
	EventEffectRejected          = "effect_rejected"
	EventContributionAccepted    = "contribution_accepted"
	EventCheckpointAccepted      = "checkpoint_accepted"
	EventResultAccepted          = "result_accepted"
	EventContinuationAccepted    = "continuation_accepted"
	EventSupersessionCommitted   = "supersession_committed"
	EventCancellationRequested   = "cancellation_requested"
	EventProcessDisposed         = "process_disposed"
	EventDestructiveAccepted     = "destructive_accepted"
	EventDestructiveReconciled   = "destructive_reconciled"
	EventOutcomeAccepted         = "outcome_accepted"
	EventRecoveryObserved        = "recovery_observed"
	EventAcknowledged            = "acknowledged"
)

const (
	DecisionObserved   = "observed"
	DecisionAccepted   = "accepted"
	DecisionRejected   = "rejected"
	DecisionBlocked    = "blocked"
	DecisionFailed     = "failed"
	DecisionReconciled = "reconciled"
)

type CausalEvent struct {
	Identity
	Sequence          uint64            `json:"sequence"`
	EventID           string            `json:"event_id"`
	ParentEventIDs    []string          `json:"parent_event_ids,omitempty"`
	TimestampUTC      string            `json:"timestamp_utc"`
	MonotonicOffsetNS int64             `json:"monotonic_offset_ns"`
	Kind              string            `json:"kind"`
	Decision          string            `json:"decision"`
	Details           map[string]string `json:"details,omitempty"`
}

func (e CausalEvent) Validate() error {
	if err := e.Identity.Validate(); err != nil {
		return err
	}
	if e.Sequence == 0 || e.EventID == "" || e.TimestampUTC == "" || e.MonotonicOffsetNS < 0 ||
		!validEventKind(e.Kind) || !validDecision(e.Decision) {
		return invalid("causal event")
	}
	parsed, err := time.Parse(time.RFC3339Nano, e.TimestampUTC)
	if err != nil {
		return invalid("event UTC timestamp")
	}
	_, offset := parsed.Zone()
	if offset != 0 {
		return invalid("event timestamp is not UTC")
	}
	seenParents := make(map[string]bool, len(e.ParentEventIDs))
	for _, parent := range e.ParentEventIDs {
		if parent == "" || seenParents[parent] {
			return invalid("causal parent identity")
		}
		seenParents[parent] = true
	}
	return nil
}

func ValidateCausalEvents(events []CausalEvent) error {
	if len(events) == 0 {
		return invalid("causal events are required")
	}
	root := events[0]
	seen := make(map[string]bool, len(events))
	type itemWorkflowIdentity struct {
		childWorkflowID string
		childRunID      string
	}
	type itemAuthorityIdentity struct {
		workItemID string
		generation uint64
	}
	items := make(map[itemAuthorityIdentity]itemWorkflowIdentity)
	childIdentityObserved := false
	var previousTime time.Time
	var previousOffset int64
	for index, event := range events {
		if err := event.Validate(); err != nil {
			return err
		}
		if event.Sequence != uint64(index+1) || seen[event.EventID] || !sameRunIdentity(root.Identity, event.Identity) {
			return invalid("causal event sequence, identity, or run")
		}
		itemIdentity := itemWorkflowIdentity{childWorkflowID: event.ChildWorkflowID, childRunID: event.ChildRunID}
		key := itemAuthorityIdentity{workItemID: event.WorkItemID, generation: event.Generation}
		if itemIdentity.childWorkflowID != "" {
			childIdentityObserved = true
			if prior, ok := items[key]; ok && prior != itemIdentity {
				return invalid(fmt.Sprintf(
					"work item Child Workflow identity changed for %s generation %d: %s/%s -> %s/%s",
					event.WorkItemID, event.Generation, prior.childWorkflowID, prior.childRunID,
					itemIdentity.childWorkflowID, itemIdentity.childRunID,
				))
			}
			items[key] = itemIdentity
		}
		if index == 0 {
			if event.Kind != EventInputRegistered || len(event.ParentEventIDs) != 0 {
				return invalid("causal root")
			}
		} else {
			if len(event.ParentEventIDs) == 0 {
				return invalid("non-root event lacks causal parent")
			}
			for _, parent := range event.ParentEventIDs {
				if !seen[parent] {
					return invalid("missing or forward causal parent")
				}
			}
		}
		eventTime, _ := time.Parse(time.RFC3339Nano, event.TimestampUTC)
		if index > 0 && (eventTime.Before(previousTime) || event.MonotonicOffsetNS < previousOffset) {
			return invalid("causal time moved backwards")
		}
		seen[event.EventID] = true
		previousTime, previousOffset = eventTime, event.MonotonicOffsetNS
	}
	if events[len(events)-1].Kind != EventAcknowledged {
		return invalid("causal acknowledgement")
	}
	if root.Topology == TopologyChildWorkflow && !childIdentityObserved {
		return invalid("child topology lacks observed Child Workflow identity")
	}
	return nil
}

func ValidateAttemptTransition(first, next Identity) error {
	if err := first.Validate(); err != nil {
		return err
	}
	if err := next.Validate(); err != nil {
		return err
	}
	if !sameLogicalAttemptIdentity(first, next) || next.ActivityAttempt != first.ActivityAttempt+1 {
		return invalid("Activity attempt changed durable logical identity")
	}
	return nil
}

func sameRunIdentity(first, next Identity) bool {
	return first.ProtocolVersion == next.ProtocolVersion && first.RunID == next.RunID && first.PairID == next.PairID &&
		first.ScheduleBlockID == next.ScheduleBlockID && first.TrackerBeadID == next.TrackerBeadID && first.Topology == next.Topology &&
		first.Case == next.Case && first.Boundary == next.Boundary && first.Probe == next.Probe && first.Fanout == next.Fanout &&
		first.LogicalOperationID == next.LogicalOperationID && first.ParentWorkflowID == next.ParentWorkflowID && first.ParentRunID == next.ParentRunID
}

func sameLogicalAttemptIdentity(first, next Identity) bool {
	return sameRunIdentity(first, next) && first.WorkItemID == next.WorkItemID && first.ChildWorkflowID == next.ChildWorkflowID &&
		first.ChildRunID == next.ChildRunID && first.ActivityID == next.ActivityID && first.Generation == next.Generation &&
		first.CapabilityHash == next.CapabilityHash
}

func validEventKind(kind string) bool {
	return slices.Contains([]string{
		EventInputRegistered,
		EventParentWorkflowStarted,
		EventChildWorkflowStarted,
		EventActivityScheduled,
		EventActivityStarted,
		EventProcessStarted,
		EventBarrierReached,
		EventFaultCommitted,
		EventDependencyStarted,
		EventDependencyFinished,
		EventDependencyStateChanged,
		EventAdmissionDecided,
		EventProgressAccepted,
		EventProgressDeadlineCreated,
		EventAuthorityRevoked,
		EventRetryBudgetExhausted,
		EventItemQuarantined,
		EventEffectAccepted,
		EventEffectRejected,
		EventContributionAccepted,
		EventCheckpointAccepted,
		EventResultAccepted,
		EventContinuationAccepted,
		EventSupersessionCommitted,
		EventCancellationRequested,
		EventProcessDisposed,
		EventDestructiveAccepted,
		EventDestructiveReconciled,
		EventOutcomeAccepted,
		EventRecoveryObserved,
		EventAcknowledged,
	}, kind)
}

func validDecision(decision string) bool {
	return slices.Contains([]string{DecisionObserved, DecisionAccepted, DecisionRejected, DecisionBlocked, DecisionFailed, DecisionReconciled}, decision)
}
