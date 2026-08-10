package protocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"time"
)

const (
	ManifestFile             = "manifest.json"
	CausalEventsFile         = "causal-events.jsonl"
	LineageFile              = "lineage.json"
	AuthorityStateFile       = "authority-state.json"
	DestinationStateFile     = "destination-state.json"
	DependencyStateFile      = "dependency-state.json"
	WorkloadStateFile        = "workload-state.json"
	FaultBoundaryFile        = "fault-boundary.json"
	NativeHistoryFile        = "native-history-or-journal-export.json"
	ProcessObservationsFile  = "process-observations.json"
	EffectiveInputFile       = "effective-input.json"
	VerdictFile              = "verdict.json"
	PublicationTimingFile    = "publication-timing.jsonl"
	PublicationExecutionFile = "publication-execution.json"
	PublicationInventoryFile = "publication-inventory.json"

	OracleProtocolVersion = "independent-topology-causal-oracle-v1"
)

type Manifest struct {
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
	CreatedAtUTC       string   `json:"created_at_utc"`
	RequiredEvidence   []string `json:"required_evidence"`
}

func (m Manifest) Validate() error {
	if m.ProtocolVersion != PublicationProtocolVersion || m.RunID == "" || m.PairID == "" || m.ScheduleBlockID == "" ||
		m.TrackerBeadID == "" || !m.Topology.Valid() || !m.Case.Valid() || m.Boundary == "" || !m.Probe.Valid() ||
		!slices.Contains([]int{8, 32, 128}, m.Fanout) || m.LogicalOperationID == "" || !slices.Equal(m.RequiredEvidence, requiredEvidenceFiles) {
		return invalid("manifest")
	}
	created, err := time.Parse(time.RFC3339Nano, m.CreatedAtUTC)
	if err != nil {
		return invalid("manifest creation time")
	}
	_, offset := created.Zone()
	if offset != 0 {
		return invalid("manifest creation time is not UTC")
	}
	if m.Probe == ProbeUnfaulted && m.Boundary != UnfaultedBoundary || m.Probe != ProbeUnfaulted && m.Boundary == UnfaultedBoundary {
		return invalid("manifest boundary and probe")
	}
	return nil
}

type LineageEdge struct {
	ParentEventID string `json:"parent_event_id"`
	ChildEventID  string `json:"child_event_id"`
}

type Lineage struct {
	RunID string        `json:"run_id"`
	Edges []LineageEdge `json:"edges"`
}

type AuthorityStateValue string

const (
	AuthorityActive    AuthorityStateValue = "active"
	AuthorityObsolete  AuthorityStateValue = "obsolete"
	AuthorityRevoked   AuthorityStateValue = "revoked"
	AuthorityCompleted AuthorityStateValue = "completed"
)

type AuthorityEpoch struct {
	Generation     uint64              `json:"generation"`
	CapabilityHash string              `json:"capability_hash"`
	State          AuthorityStateValue `json:"state"`
}

type AuthorityState struct {
	RunID                 string           `json:"run_id"`
	CurrentGeneration     uint64           `json:"current_generation"`
	CurrentCapabilityHash string           `json:"current_capability_hash"`
	Epochs                []AuthorityEpoch `json:"epochs"`
}

type DestinationAction struct {
	EventID         string `json:"event_id"`
	WorkItemID      string `json:"work_item_id"`
	LogicalEffectID string `json:"logical_effect_id"`
	ReceiptID       string `json:"receipt_id,omitempty"`
	Generation      uint64 `json:"generation"`
	CapabilityHash  string `json:"capability_hash"`
	Decision        string `json:"decision"`
	Applied         bool   `json:"applied"`
}

type DestinationState struct {
	RunID   string              `json:"run_id"`
	Actions []DestinationAction `json:"actions"`
}

type DependencyRequest struct {
	RequestID         string `json:"request_id"`
	EventID           string `json:"event_id"`
	StartedEventID    string `json:"started_event_id,omitempty"`
	ParentRequestID   string `json:"parent_request_id,omitempty"`
	WorkItemID        string `json:"work_item_id"`
	Attempt           int    `json:"activity_attempt"`
	RetryOrdinal      int    `json:"retry_ordinal,omitempty"`
	RetryOwner        string `json:"retry_owner,omitempty"`
	Outcome           string `json:"outcome"`
	CostUnits         int64  `json:"cost_units"`
	StartedOffsetNS   int64  `json:"started_offset_ns,omitempty"`
	FinishedOffsetNS  int64  `json:"finished_offset_ns,omitempty"`
	RetryDelayMS      int64  `json:"retry_delay_ms,omitempty"`
	ServiceMS         int64  `json:"service_ms,omitempty"`
	ConcurrentAtStart int    `json:"concurrent_at_start,omitempty"`
}

type DependencyState struct {
	RunID    string              `json:"run_id"`
	Requests []DependencyRequest `json:"requests"`
}

type WorkloadState struct {
	RunID                   string                 `json:"run_id"`
	RequiredItemIDs         []string               `json:"required_item_ids"`
	AcceptedResultItemIDs   []string               `json:"accepted_result_item_ids"`
	ExpectedLogicalOutput   string                 `json:"expected_logical_output"`
	ActualLogicalOutput     string                 `json:"actual_logical_output"`
	ProhibitedActionCount   int                    `json:"prohibited_action_count"`
	TerminalFailureExpected bool                   `json:"terminal_failure_expected"`
	TerminalFailureObserved bool                   `json:"terminal_failure_observed"`
	LivenessSatisfied       bool                   `json:"liveness_satisfied"`
	Semantics               OrchestrationSemantics `json:"orchestration_semantics"`
	Recovery                *RecoveryDynamics      `json:"recovery_dynamics,omitempty"`
}

// Metric is an observed integer-valued publication metric. Durations are
// represented in whole milliseconds so the evidence format has no NaN or
// floating-point comparison surface.
type Metric struct {
	Name  string `json:"name"`
	Unit  string `json:"unit"`
	Value int64  `json:"value"`
}

type ContributionObservation struct {
	EventID         string `json:"event_id"`
	WorkItemID      string `json:"work_item_id"`
	Ordinal         int    `json:"ordinal"`
	ActivityAttempt int    `json:"activity_attempt"`
	Decision        string `json:"decision"`
}

type CheckpointObservation struct {
	EventID      string   `json:"event_id"`
	CheckpointID string   `json:"checkpoint_id"`
	Cardinality  int      `json:"cardinality"`
	Members      []string `json:"members"`
	Value        int64    `json:"value"`
	ReceiptID    string   `json:"receipt_id,omitempty"`
	Decision     string   `json:"decision"`
	Applied      bool     `json:"applied"`
}

type ContinuationObservation struct {
	EventID        string   `json:"event_id"`
	ContinuationID string   `json:"continuation_id"`
	Members        []string `json:"members"`
	Value          int64    `json:"value"`
	ReceiptID      string   `json:"receipt_id,omitempty"`
	Decision       string   `json:"decision"`
	Applied        bool     `json:"applied"`
}

type SupersessionObservation struct {
	CommitEventID             string `json:"commit_event_id"`
	CancellationEventID       string `json:"cancellation_event_id"`
	ProcessDispositionEventID string `json:"process_disposition_event_id"`
	ObsoleteItemID            string `json:"obsolete_item_id"`
	ObsoleteGeneration        uint64 `json:"obsolete_generation"`
	ObsoleteCapabilityHash    string `json:"obsolete_capability_hash"`
	ReplacementGeneration     uint64 `json:"replacement_generation"`
	ReplacementCapabilityHash string `json:"replacement_capability_hash"`
}

type DestructiveDelivery struct {
	EventID          string `json:"event_id"`
	ActivityAttempt  int    `json:"activity_attempt"`
	OperationID      string `json:"operation_id"`
	ExpectedVersion  uint64 `json:"expected_version"`
	PreviousVersion  uint64 `json:"previous_version"`
	ResultingVersion uint64 `json:"resulting_version"`
	ReceiptID        string `json:"receipt_id,omitempty"`
	Decision         string `json:"decision"`
	Applied          bool   `json:"applied"`
}

type DestructiveObservation struct {
	OperationID          string                `json:"operation_id"`
	ExpectedPriorVersion uint64                `json:"expected_prior_version"`
	FinalVersion         uint64                `json:"final_version"`
	OutcomeReceiptID     string                `json:"outcome_receipt_id"`
	Deliveries           []DestructiveDelivery `json:"deliveries"`
}

type OrchestrationSemantics struct {
	Contributions []ContributionObservation `json:"contributions,omitempty"`
	Checkpoints   []CheckpointObservation   `json:"checkpoints,omitempty"`
	Continuations []ContinuationObservation `json:"continuations,omitempty"`
	Supersession  *SupersessionObservation  `json:"supersession,omitempty"`
	Destructive   *DestructiveObservation   `json:"destructive,omitempty"`
	Metrics       []Metric                  `json:"metrics"`
}

const (
	RecoveryDispositionSucceeded   = "succeeded"
	RecoveryDispositionRejected    = "rejected"
	RecoveryDispositionQuarantined = "quarantined"
	RecoveryDispositionUnresolved  = "unresolved"
)

type RecoveryItemObservation struct {
	WorkItemID       string `json:"work_item_id"`
	Role             string `json:"role"`
	Poison           bool   `json:"poison"`
	Admitted         bool   `json:"admitted"`
	Disposition      string `json:"disposition"`
	ScheduleEventID  string `json:"schedule_event_id"`
	StartEventID     string `json:"start_event_id,omitempty"`
	TerminalEventID  string `json:"terminal_event_id"`
	ActivityAttempts int    `json:"activity_attempts"`
	AgentProcesses   int    `json:"agent_processes"`
	AcceptedEffects  int    `json:"accepted_effects"`
	AcceptedResults  int    `json:"accepted_results"`
	CostUnits        int64  `json:"cost_units"`
}

type RecoveryDynamics struct {
	Items                        []RecoveryItemObservation `json:"items"`
	ParentAcknowledgementEventID string                    `json:"parent_acknowledgement_event_id"`
	Bounds                       []Metric                  `json:"bounds"`
	Metrics                      []Metric                  `json:"metrics"`
}

type FaultBoundary struct {
	RunID                 string `json:"run_id"`
	Injected              bool   `json:"injected"`
	ExpectedBoundary      string `json:"expected_boundary,omitempty"`
	BarrierEventID        string `json:"barrier_event_id,omitempty"`
	FaultEventID          string `json:"fault_event_id,omitempty"`
	TargetProcessIdentity string `json:"target_process_identity,omitempty"`
}

type NativeHistory struct {
	RunID              string          `json:"run_id"`
	Captured           bool            `json:"captured"`
	EventCount         int             `json:"event_count"`
	Export             json.RawMessage `json:"export"`
	HistorySHA256      string          `json:"history_sha256"`
	ReplayCompatible   bool            `json:"replay_compatible"`
	ReplayWorkerSHA256 string          `json:"replay_worker_sha256"`
	ReplayError        string          `json:"replay_error,omitempty"`
}

type ProcessObservation struct {
	EventID         string `json:"event_id"`
	WorkItemID      string `json:"work_item_id"`
	WorkerID        string `json:"worker_id"`
	WorkerPID       int    `json:"worker_pid"`
	ProcessIdentity string `json:"process_identity"`
	State           string `json:"state"`
}

type ProcessObservations struct {
	RunID        string               `json:"run_id"`
	Observations []ProcessObservation `json:"observations"`
}

type EffectiveInput struct {
	RunID                     string   `json:"run_id"`
	PairID                    string   `json:"pair_id"`
	ScheduleBlockID           string   `json:"schedule_block_id"`
	Topology                  Topology `json:"topology"`
	Case                      CaseID   `json:"case_id"`
	Boundary                  string   `json:"boundary_id"`
	Probe                     Probe    `json:"probe"`
	Fanout                    int      `json:"fanout"`
	WorkloadSHA256            string   `json:"workload_sha256"`
	ActivityOptionsSHA256     string   `json:"activity_options_sha256"`
	HostEnvelopeSHA256        string   `json:"host_envelope_sha256"`
	AgentBinarySHA256         string   `json:"agent_binary_sha256"`
	DestinationProtocolSHA256 string   `json:"destination_protocol_sha256"`
	BarrierControllerSHA256   string   `json:"barrier_controller_sha256"`
	SourceSHA256              string   `json:"source_sha256"`
}

func (i EffectiveInput) MatchedWith(other EffectiveInput) bool {
	return i.PairID == other.PairID && i.ScheduleBlockID == other.ScheduleBlockID && i.Case == other.Case &&
		i.Boundary == other.Boundary && i.Probe == other.Probe && i.Fanout == other.Fanout && i.Topology != other.Topology &&
		i.WorkloadSHA256 == other.WorkloadSHA256 && i.ActivityOptionsSHA256 == other.ActivityOptionsSHA256 &&
		i.HostEnvelopeSHA256 == other.HostEnvelopeSHA256 && i.AgentBinarySHA256 == other.AgentBinarySHA256 &&
		i.DestinationProtocolSHA256 == other.DestinationProtocolSHA256 &&
		i.BarrierControllerSHA256 == other.BarrierControllerSHA256 && i.SourceSHA256 == other.SourceSHA256
}

type Admission string

const (
	AdmissionValid   Admission = "valid"
	AdmissionInvalid Admission = "invalid"
)

type Outcome string

const (
	OutcomePass       Outcome = "pass"
	OutcomeFail       Outcome = "fail"
	OutcomeUnresolved Outcome = "unresolved"
)

type Verdict struct {
	ProtocolVersion    string    `json:"protocol_version"`
	RunID              string    `json:"run_id"`
	Admission          Admission `json:"admission"`
	ReasonCodes        []string  `json:"reason_codes,omitempty"`
	Correctness        Outcome   `json:"correctness"`
	Safety             Outcome   `json:"safety"`
	Liveness           Outcome   `json:"liveness"`
	Diagnosability     Outcome   `json:"diagnosability"`
	EfficiencyEligible bool      `json:"efficiency_eligible"`
	Oracle             string    `json:"oracle"`
}

type TimingEvent struct {
	Sequence          int    `json:"sequence"`
	Kind              string `json:"kind"`
	Barrier           string `json:"barrier,omitempty"`
	TimestampUTC      string `json:"timestamp_utc"`
	MonotonicOffsetNS int64  `json:"monotonic_offset_ns"`
}

type PublicationExecution struct {
	ProtocolVersion string   `json:"protocol_version"`
	RunID           string   `json:"run_id"`
	PairID          string   `json:"pair_id"`
	ScheduleBlockID string   `json:"schedule_block_id"`
	Topology        Topology `json:"topology"`
	ReplayVerified  bool     `json:"replay_verified"`
	ExclusionReason string   `json:"exclusion_reason,omitempty"`
}

type EvidenceBundle struct {
	Manifest            Manifest             `json:"manifest"`
	CausalEvents        []CausalEvent        `json:"causal_events"`
	Lineage             Lineage              `json:"lineage"`
	Authority           AuthorityState       `json:"authority_state"`
	Destination         DestinationState     `json:"destination_state"`
	Dependency          DependencyState      `json:"dependency_state"`
	Workload            WorkloadState        `json:"workload_state"`
	FaultBoundary       FaultBoundary        `json:"fault_boundary"`
	NativeHistory       NativeHistory        `json:"native_history"`
	ProcessObservations ProcessObservations  `json:"process_observations"`
	EffectiveInput      EffectiveInput       `json:"effective_input"`
	Verdict             Verdict              `json:"verdict"`
	Timing              []TimingEvent        `json:"timing"`
	Execution           PublicationExecution `json:"execution"`
}

func (b EvidenceBundle) Validate() error {
	if err := b.ValidateRaw(); err != nil {
		return err
	}
	if err := validateVerdict(b.Verdict); err != nil {
		return err
	}
	if b.Verdict.RunID != b.Manifest.RunID {
		return invalid("verdict run identity")
	}
	return nil
}

func (b EvidenceBundle) ValidateRaw() error {
	if err := b.Manifest.Validate(); err != nil {
		return err
	}
	if err := ValidateCausalEvents(b.CausalEvents); err != nil {
		return err
	}
	identity := b.CausalEvents[0].Identity
	manifest := b.Manifest
	if identity.RunID != manifest.RunID || identity.PairID != manifest.PairID || identity.ScheduleBlockID != manifest.ScheduleBlockID ||
		identity.TrackerBeadID != manifest.TrackerBeadID || identity.Topology != manifest.Topology || identity.Case != manifest.Case ||
		identity.Boundary != manifest.Boundary || identity.Probe != manifest.Probe || identity.Fanout != manifest.Fanout ||
		identity.LogicalOperationID != manifest.LogicalOperationID {
		return invalid("manifest and causal identity")
	}
	if err := validateLineage(b.CausalEvents, b.Lineage); err != nil {
		return err
	}
	if err := validateAuthority(b.Authority); err != nil {
		return err
	}
	if err := validateDestination(b.Destination); err != nil {
		return err
	}
	if err := validateDependency(b.Dependency, manifest.Case.Suite() == SuiteRecoveryDynamics); err != nil {
		return err
	}
	if err := validateWorkload(b.Workload, manifest); err != nil {
		return err
	}
	if err := validateFaultBoundary(b.FaultBoundary, manifest); err != nil {
		return err
	}
	if err := validateNativeHistory(b.NativeHistory); err != nil {
		return err
	}
	if err := validateProcessObservations(b.ProcessObservations); err != nil {
		return err
	}
	if err := validateEffectiveInput(b.EffectiveInput, manifest); err != nil {
		return err
	}
	if err := validateTiming(b.Timing); err != nil {
		return err
	}
	if err := validateExecution(b.Execution, manifest); err != nil {
		return err
	}
	if err := validateEvidenceReferences(b); err != nil {
		return err
	}
	for _, runID := range []string{b.Lineage.RunID, b.Authority.RunID, b.Destination.RunID, b.Dependency.RunID, b.Workload.RunID,
		b.FaultBoundary.RunID, b.NativeHistory.RunID, b.ProcessObservations.RunID, b.EffectiveInput.RunID, b.Execution.RunID} {
		if runID != manifest.RunID {
			return invalid("evidence run identity")
		}
	}
	return nil
}

func validateEvidenceReferences(bundle EvidenceBundle) error {
	events := make(map[string]CausalEvent, len(bundle.CausalEvents))
	resultItems := make(map[string]bool, len(bundle.Workload.AcceptedResultItemIDs))
	for _, event := range bundle.CausalEvents {
		events[event.EventID] = event
		if event.Kind == EventResultAccepted && event.Decision == DecisionAccepted {
			resultItems[event.WorkItemID] = true
		}
	}
	for _, item := range bundle.Workload.RequiredItemIDs {
		found := false
		for _, event := range bundle.CausalEvents {
			if event.WorkItemID == item {
				found = true
				break
			}
		}
		if !found {
			return invalid("required item lacks causal event")
		}
	}
	for _, item := range bundle.Workload.AcceptedResultItemIDs {
		if !resultItems[item] {
			return invalid("accepted result lacks causal event")
		}
	}
	for _, action := range bundle.Destination.Actions {
		event, ok := events[action.EventID]
		if !ok || event.WorkItemID != action.WorkItemID || event.Generation != action.Generation || event.CapabilityHash != action.CapabilityHash {
			return invalid(fmt.Sprintf(
				"destination causal reference for %s: found=%t event-item=%s action-item=%s event-generation=%d action-generation=%d event-capability=%s action-capability=%s",
				action.EventID, ok, event.WorkItemID, action.WorkItemID, event.Generation, action.Generation,
				event.CapabilityHash, action.CapabilityHash,
			))
		}
	}
	if err := validateDependencyReferences(bundle.Dependency.Requests, events); err != nil {
		return err
	}
	for _, observation := range bundle.ProcessObservations.Observations {
		event, ok := events[observation.EventID]
		if !ok || event.WorkItemID != observation.WorkItemID || event.WorkerID != observation.WorkerID ||
			event.WorkerPID != observation.WorkerPID || event.ProcessIdentity != observation.ProcessIdentity {
			return invalid("process causal reference")
		}
	}
	for _, contribution := range bundle.Workload.Semantics.Contributions {
		event, ok := events[contribution.EventID]
		if !ok || event.Kind != EventContributionAccepted || event.WorkItemID != contribution.WorkItemID ||
			event.ActivityAttempt != contribution.ActivityAttempt || event.Decision != contribution.Decision {
			return invalid("contribution causal reference")
		}
	}
	for _, checkpoint := range bundle.Workload.Semantics.Checkpoints {
		event, ok := events[checkpoint.EventID]
		if !ok || event.Kind != EventCheckpointAccepted || event.Decision != checkpoint.Decision {
			return invalid("checkpoint causal reference")
		}
	}
	for _, continuation := range bundle.Workload.Semantics.Continuations {
		event, ok := events[continuation.EventID]
		if !ok || event.Kind != EventContinuationAccepted || event.Decision != continuation.Decision {
			return invalid("continuation causal reference")
		}
	}
	if supersession := bundle.Workload.Semantics.Supersession; supersession != nil {
		commit, commitOK := events[supersession.CommitEventID]
		cancellation, cancellationOK := events[supersession.CancellationEventID]
		disposition, dispositionOK := events[supersession.ProcessDispositionEventID]
		if !commitOK || !cancellationOK || !dispositionOK || commit.Kind != EventSupersessionCommitted ||
			cancellation.Kind != EventCancellationRequested || disposition.Kind != EventProcessDisposed ||
			commit.WorkItemID != supersession.ObsoleteItemID || cancellation.WorkItemID != supersession.ObsoleteItemID ||
			disposition.WorkItemID != supersession.ObsoleteItemID {
			return invalid("supersession causal reference")
		}
	}
	if destructive := bundle.Workload.Semantics.Destructive; destructive != nil {
		for _, delivery := range destructive.Deliveries {
			event, ok := events[delivery.EventID]
			wantKind := EventDestructiveAccepted
			if delivery.Decision == DecisionReconciled {
				wantKind = EventDestructiveReconciled
			}
			if delivery.Decision == DecisionRejected {
				wantKind = EventEffectRejected
			}
			if !ok || event.Kind != wantKind || event.ActivityAttempt != delivery.ActivityAttempt || event.Decision != delivery.Decision {
				return invalid("destructive causal reference")
			}
		}
	}
	if recovery := bundle.Workload.Recovery; recovery != nil {
		if err := validateRecoveryReferences(*recovery, events); err != nil {
			return err
		}
	}
	return nil
}

func validateDependencyReferences(requests []DependencyRequest, events map[string]CausalEvent) error {
	lastStartedByItem := make(map[string]CausalEvent)
	for _, request := range requests {
		finished, ok := events[request.EventID]
		if !ok || finished.WorkItemID != request.WorkItemID || finished.ActivityAttempt != request.Attempt {
			return invalid("dependency causal reference")
		}
		if request.StartedEventID == "" {
			continue
		}
		started, ok := events[request.StartedEventID]
		if !ok || started.Kind != EventDependencyStarted || finished.Kind != EventDependencyFinished ||
			started.WorkItemID != request.WorkItemID || started.ActivityAttempt != request.Attempt ||
			started.ActivityID != finished.ActivityID || started.Sequence >= finished.Sequence {
			return invalid("dependency start/finish causal reference")
		}
		if previous, exists := lastStartedByItem[request.WorkItemID]; exists &&
			previous.ActivityID == started.ActivityID && started.ActivityAttempt < previous.ActivityAttempt {
			return invalid("dependency Activity attempt lineage")
		}
		lastStartedByItem[request.WorkItemID] = started
	}
	return nil
}

func validateRecoveryReferences(state RecoveryDynamics, events map[string]CausalEvent) error {
	acknowledgement, ok := events[state.ParentAcknowledgementEventID]
	if !ok || acknowledgement.Kind != EventAcknowledged || acknowledgement.Decision != DecisionAccepted {
		return invalid("recovery acknowledgement causal reference")
	}
	for _, item := range state.Items {
		scheduled, scheduleOK := events[item.ScheduleEventID]
		terminal, terminalOK := events[item.TerminalEventID]
		if !scheduleOK || scheduled.Kind != EventActivityScheduled || scheduled.WorkItemID != item.WorkItemID ||
			!terminalOK || terminal.WorkItemID != item.WorkItemID {
			return invalid("recovery item causal reference")
		}
		if item.Admitted {
			started, startOK := events[item.StartEventID]
			if !startOK || started.Kind != EventActivityStarted || started.WorkItemID != item.WorkItemID ||
				scheduled.Sequence >= started.Sequence || started.Sequence >= terminal.Sequence {
				return invalid("recovery admitted item causal reference")
			}
		} else if scheduled.Sequence >= terminal.Sequence {
			return invalid("recovery rejected item causal order")
		}
		switch item.Disposition {
		case RecoveryDispositionSucceeded:
			if terminal.Kind != EventResultAccepted || terminal.Decision != DecisionAccepted {
				return invalid("recovery success terminal reference")
			}
		case RecoveryDispositionRejected:
			if terminal.Kind != EventAdmissionDecided || terminal.Decision != DecisionRejected {
				return invalid("recovery rejection terminal reference")
			}
		case RecoveryDispositionQuarantined:
			if terminal.Kind != EventItemQuarantined || terminal.Decision != DecisionAccepted {
				return invalid("recovery quarantine terminal reference")
			}
		case RecoveryDispositionUnresolved:
			if terminal.Kind != EventRetryBudgetExhausted || terminal.Decision != DecisionFailed {
				return invalid("recovery unresolved terminal reference")
			}
		default:
			return invalid("recovery terminal disposition")
		}
	}
	return nil
}

func validateLineage(events []CausalEvent, lineage Lineage) error {
	if lineage.RunID == "" {
		return invalid("lineage run identity")
	}
	want := make(map[LineageEdge]bool)
	for _, event := range events {
		for _, parent := range event.ParentEventIDs {
			want[LineageEdge{ParentEventID: parent, ChildEventID: event.EventID}] = true
		}
	}
	if len(lineage.Edges) != len(want) {
		return invalid("lineage edge count")
	}
	seen := make(map[LineageEdge]bool, len(lineage.Edges))
	for _, edge := range lineage.Edges {
		if edge.ParentEventID == "" || edge.ChildEventID == "" || !want[edge] || seen[edge] {
			return invalid("lineage edge")
		}
		seen[edge] = true
	}
	parentsByChild := make(map[string][]string, len(events))
	for _, edge := range lineage.Edges {
		parentsByChild[edge.ChildEventID] = append(parentsByChild[edge.ChildEventID], edge.ParentEventID)
	}
	reachable := make(map[string]bool, len(events))
	stack := []string{events[len(events)-1].EventID}
	for len(stack) > 0 {
		last := len(stack) - 1
		eventID := stack[last]
		stack = stack[:last]
		if reachable[eventID] {
			continue
		}
		reachable[eventID] = true
		stack = append(stack, parentsByChild[eventID]...)
	}
	if len(reachable) != len(events) {
		return invalid("causal event does not lead to acknowledgement")
	}
	return nil
}

func validateAuthority(state AuthorityState) error {
	if state.RunID == "" || state.CurrentGeneration == 0 || !validSHA256(state.CurrentCapabilityHash) || len(state.Epochs) == 0 {
		return invalid("authority state")
	}
	active := 0
	seen := make(map[uint64]bool)
	var previous uint64
	for _, epoch := range state.Epochs {
		if epoch.Generation == 0 || epoch.Generation <= previous || seen[epoch.Generation] || !validSHA256(epoch.CapabilityHash) ||
			!slices.Contains([]AuthorityStateValue{AuthorityActive, AuthorityObsolete, AuthorityRevoked, AuthorityCompleted}, epoch.State) {
			return invalid("authority epoch")
		}
		if epoch.State == AuthorityActive {
			active++
			if epoch.Generation != state.CurrentGeneration || epoch.CapabilityHash != state.CurrentCapabilityHash {
				return invalid("current authority epoch")
			}
		}
		seen[epoch.Generation], previous = true, epoch.Generation
	}
	if active != 1 {
		return invalid("active authority count")
	}
	return nil
}

func validateDestination(state DestinationState) error {
	if state.RunID == "" {
		return invalid("destination run identity")
	}
	seen := make(map[string]bool)
	for _, action := range state.Actions {
		if action.EventID == "" || action.WorkItemID == "" || action.LogicalEffectID == "" || action.Generation == 0 ||
			!validSHA256(action.CapabilityHash) || !slices.Contains([]string{DecisionAccepted, DecisionRejected, DecisionReconciled}, action.Decision) ||
			seen[action.EventID] {
			return invalid("destination action")
		}
		if action.Decision == DecisionAccepted && (!action.Applied || action.ReceiptID == "") ||
			action.Decision == DecisionReconciled && (action.Applied || action.ReceiptID == "") ||
			action.Decision == DecisionRejected && (action.Applied || action.ReceiptID != "") {
			return invalid("destination action disposition")
		}
		seen[action.EventID] = true
	}
	return nil
}

func validateDependency(state DependencyState, recovery bool) error {
	if state.RunID == "" {
		return invalid("dependency run identity")
	}
	seen := make(map[string]bool)
	lastByItem := make(map[string]DependencyRequest)
	for _, request := range state.Requests {
		if request.RequestID == "" || request.EventID == "" || request.WorkItemID == "" || request.Attempt < 1 ||
			request.Outcome == "" || request.CostUnits < 0 || seen[request.RequestID] {
			return invalid("dependency request")
		}
		if recovery {
			if request.StartedEventID == "" || request.RetryOrdinal < 1 || request.RetryOwner == "" ||
				request.StartedOffsetNS < 0 || request.FinishedOffsetNS < request.StartedOffsetNS ||
				request.RetryDelayMS < 0 || request.ServiceMS < 0 || request.ConcurrentAtStart < 1 {
				return invalid("recovery dependency request")
			}
			previous, hasPrevious := lastByItem[request.WorkItemID]
			if !hasPrevious {
				if request.RetryOrdinal != 1 || request.ParentRequestID != "" {
					return invalid("initial dependency request lineage")
				}
			} else if request.RetryOrdinal != previous.RetryOrdinal+1 || request.ParentRequestID != previous.RequestID ||
				request.StartedOffsetNS < previous.FinishedOffsetNS {
				return invalid("retry dependency request lineage")
			}
			lastByItem[request.WorkItemID] = request
		} else if request.StartedEventID != "" || request.ParentRequestID != "" || request.RetryOrdinal != 0 ||
			request.RetryOwner != "" || request.StartedOffsetNS != 0 || request.FinishedOffsetNS != 0 ||
			request.RetryDelayMS != 0 || request.ServiceMS != 0 || request.ConcurrentAtStart != 0 {
			return invalid("orchestration dependency request shape")
		}
		seen[request.RequestID] = true
	}
	return nil
}

func validateWorkload(state WorkloadState, manifest Manifest) error {
	if state.RunID == "" || len(state.RequiredItemIDs) == 0 || len(state.RequiredItemIDs) > manifest.Fanout ||
		state.ExpectedLogicalOutput == "" || state.ProhibitedActionCount < 0 {
		return invalid("workload state")
	}
	required := make(map[string]bool, len(state.RequiredItemIDs))
	for _, item := range state.RequiredItemIDs {
		if item == "" || required[item] {
			return invalid("required work item")
		}
		required[item] = true
	}
	seenResults := make(map[string]bool, len(state.AcceptedResultItemIDs))
	for _, item := range state.AcceptedResultItemIDs {
		if !required[item] || seenResults[item] {
			return invalid("accepted result item")
		}
		seenResults[item] = true
	}
	if manifest.Case.Suite() == SuiteRecoveryDynamics {
		if state.Recovery == nil || !emptyOrchestrationSemantics(state.Semantics) {
			return invalid("recovery workload shape")
		}
		return validateRecoveryDynamics(*state.Recovery, manifest, required)
	}
	if state.Recovery != nil {
		return invalid("orchestration workload has recovery observations")
	}
	return validateOrchestrationSemantics(state.Semantics, manifest.Case)
}

type metricSpec struct {
	name string
	unit string
}

func metricSpecs(benchmarkCase CaseID) []metricSpec {
	switch benchmarkCase {
	case CaseJoinBarrier:
		return []metricSpec{
			{name: "premature_continuation_count", unit: "count"},
			{name: "accepted_continuation_count", unit: "count"},
			{name: "join_lag_after_last_required_result_ms", unit: "ms"},
			{name: "end_to_end_latency_ms", unit: "ms"},
			{name: "history_bytes_per_item", unit: "bytes_per_item"},
		}
	case CaseIncrementalPartialReduction:
		return []metricSpec{
			{name: "incorrect_reduction_count", unit: "count"},
			{name: "duplicate_checkpoint_apply_count", unit: "count"},
			{name: "time_to_first_reduction_ms", unit: "ms"},
			{name: "final_makespan_ms", unit: "ms"},
			{name: "history_bytes_per_item", unit: "bytes_per_item"},
		}
	case CaseQueuedExecutingSupersession:
		return []metricSpec{
			{name: "stale_action_accept_count", unit: "count"},
			{name: "cancellation_propagation_ms", unit: "ms"},
			{name: "replacement_recovery_ms", unit: "ms"},
			{name: "wasted_compute_ms", unit: "ms"},
			{name: "wasted_cost_units", unit: "cost_units"},
		}
	case CaseDestructiveTransition:
		return []metricSpec{
			{name: "accepted_destructive_apply_count", unit: "count"},
			{name: "invariant_violation_count", unit: "count"},
			{name: "physical_delivery_count", unit: "count"},
			{name: "recovery_delay_ms", unit: "ms"},
			{name: "end_to_end_latency_ms", unit: "ms"},
		}
	case CaseCrashRecoveryBoundaries:
		return []metricSpec{
			{name: "agent_process_count", unit: "count"},
			{name: "duplicate_effect_count", unit: "count"},
			{name: "duplicate_result_count", unit: "count"},
			{name: "time_to_recovery_ms", unit: "ms"},
			{name: "schedule_to_start_ms", unit: "ms"},
			{name: "activity_attempt_count", unit: "count"},
			{name: "workflow_task_count", unit: "count"},
			{name: "history_event_count", unit: "count"},
			{name: "history_bytes", unit: "bytes"},
		}
	case CaseLayeredRetryAmplification:
		return []metricSpec{
			{name: "physical_request_count", unit: "count"},
			{name: "amplification_factor", unit: "ratio_milli"},
			{name: "retry_delay_ms", unit: "ms"},
			{name: "active_execution_ms", unit: "ms"},
			{name: "recovery_delay_ms", unit: "ms"},
			{name: "cost_units", unit: "cost_units"},
			{name: "history_event_count", unit: "count"},
			{name: "history_bytes", unit: "bytes"},
		}
	case CaseOutageBacklogHerdRecovery:
		return []metricSpec{
			{name: "peak_qps", unit: "requests_per_second"},
			{name: "peak_retry_concurrency", unit: "count"},
			{name: "backlog_integral_ms", unit: "item_ms"},
			{name: "backlog_drain_p50_ms", unit: "ms"},
			{name: "backlog_drain_p90_ms", unit: "ms"},
			{name: "recovery_delay_ms", unit: "ms"},
			{name: "duplicate_effect_count", unit: "count"},
			{name: "history_event_count", unit: "count"},
			{name: "history_bytes", unit: "bytes"},
		}
	case CaseBackpressureOverload:
		return []metricSpec{
			{name: "schedule_to_start_ms", unit: "ms"},
			{name: "queue_age_ms", unit: "ms"},
			{name: "end_to_end_latency_ms", unit: "ms"},
			{name: "throughput_per_second", unit: "items_per_second_milli"},
			{name: "admission_rejection_fraction", unit: "ratio_milli"},
			{name: "peak_in_flight_count", unit: "count"},
			{name: "history_events_per_item", unit: "events_per_item_milli"},
			{name: "history_bytes_per_item", unit: "bytes_per_item"},
		}
	case CasePoisonWorkIsolation:
		return []metricSpec{
			{name: "poison_attempt_count", unit: "count"},
			{name: "poison_cost_units", unit: "cost_units"},
			{name: "poison_capacity_ms", unit: "ms"},
			{name: "healthy_schedule_to_start_ms", unit: "ms"},
			{name: "healthy_task_latency_ms", unit: "ms"},
			{name: "healthy_completion_fraction", unit: "ratio_milli"},
			{name: "history_event_count", unit: "count"},
			{name: "history_bytes", unit: "bytes"},
		}
	case CaseSilentProgress:
		return []metricSpec{
			{name: "failure_detection_latency_ms", unit: "ms"},
			{name: "false_positive_revocation_count", unit: "count"},
			{name: "stale_action_accept_count", unit: "count"},
			{name: "replacement_recovery_ms", unit: "ms"},
			{name: "healthy_task_latency_ms", unit: "ms"},
			{name: "end_to_end_latency_ms", unit: "ms"},
			{name: "history_event_count", unit: "count"},
			{name: "history_bytes", unit: "bytes"},
		}
	default:
		return nil
	}
}

func MetricsForCase(benchmarkCase CaseID) []Metric {
	specs := metricSpecs(benchmarkCase)
	metrics := make([]Metric, len(specs))
	for index, spec := range specs {
		metrics[index] = Metric{Name: spec.name, Unit: spec.unit}
	}
	return metrics
}

func validateOrchestrationSemantics(state OrchestrationSemantics, benchmarkCase CaseID) error {
	specs := metricSpecs(benchmarkCase)
	if err := validateMetrics(state.Metrics, specs, "orchestration"); err != nil {
		return err
	}
	if len(specs) == 0 {
		return nil
	}

	for _, contribution := range state.Contributions {
		if contribution.EventID == "" || contribution.WorkItemID == "" || contribution.Ordinal < 1 || contribution.ActivityAttempt < 1 ||
			!slices.Contains([]string{DecisionAccepted, DecisionRejected, DecisionReconciled}, contribution.Decision) {
			return invalid("reduction contribution")
		}
	}
	for _, checkpoint := range state.Checkpoints {
		if checkpoint.EventID == "" || checkpoint.CheckpointID == "" || checkpoint.Cardinality < 1 ||
			checkpoint.Cardinality != len(checkpoint.Members) || !validSortedMembers(checkpoint.Members) ||
			!validAppliedDisposition(checkpoint.Decision, checkpoint.Applied, checkpoint.ReceiptID) {
			return invalid("reduction checkpoint")
		}
	}
	for _, continuation := range state.Continuations {
		if continuation.EventID == "" || continuation.ContinuationID == "" || !validSortedMembers(continuation.Members) ||
			!validAppliedDisposition(continuation.Decision, continuation.Applied, continuation.ReceiptID) {
			return invalid("continuation")
		}
	}
	switch benchmarkCase {
	case CaseJoinBarrier:
		if len(state.Checkpoints) != 0 || state.Supersession != nil || state.Destructive != nil {
			return invalid("join semantics")
		}
	case CaseIncrementalPartialReduction:
		if len(state.Contributions) == 0 || len(state.Checkpoints) == 0 || state.Supersession != nil || state.Destructive != nil {
			return invalid("reduction semantics")
		}
	case CaseQueuedExecutingSupersession:
		if len(state.Contributions) != 0 || len(state.Checkpoints) != 0 || state.Supersession == nil || state.Destructive != nil ||
			len(state.Continuations) == 0 {
			return invalid("supersession semantics")
		}
		value := state.Supersession
		if value.CommitEventID == "" || value.CancellationEventID == "" || value.ProcessDispositionEventID == "" || value.ObsoleteItemID == "" ||
			value.ObsoleteGeneration == 0 || !validSHA256(value.ObsoleteCapabilityHash) ||
			value.ReplacementGeneration <= value.ObsoleteGeneration || !validSHA256(value.ReplacementCapabilityHash) ||
			value.ObsoleteCapabilityHash == value.ReplacementCapabilityHash {
			return invalid("supersession observation")
		}
	case CaseDestructiveTransition:
		if len(state.Contributions) != 0 || len(state.Checkpoints) != 0 || state.Supersession != nil || state.Destructive == nil ||
			len(state.Continuations) == 0 {
			return invalid("destructive semantics")
		}
		value := state.Destructive
		if value.OperationID == "" || value.OutcomeReceiptID == "" || len(value.Deliveries) == 0 {
			return invalid("destructive observation")
		}
		for _, delivery := range value.Deliveries {
			if delivery.EventID == "" || delivery.ActivityAttempt < 1 || delivery.OperationID == "" ||
				delivery.ResultingVersion < delivery.PreviousVersion ||
				!validAppliedDisposition(delivery.Decision, delivery.Applied, delivery.ReceiptID) {
				return invalid("destructive delivery")
			}
		}
	}
	return nil
}

func validateMetrics(metrics []Metric, specs []metricSpec, subject string) error {
	if len(metrics) != len(specs) {
		return invalid(subject + " metric coverage")
	}
	wantMetrics := make(map[string]string, len(specs))
	for _, spec := range specs {
		wantMetrics[spec.name] = spec.unit
	}
	seenMetrics := make(map[string]bool, len(metrics))
	for _, metric := range metrics {
		unit, exists := wantMetrics[metric.Name]
		if !exists || unit != metric.Unit || metric.Value < 0 || seenMetrics[metric.Name] {
			return invalid(subject + " metric")
		}
		seenMetrics[metric.Name] = true
	}
	return nil
}

func emptyOrchestrationSemantics(state OrchestrationSemantics) bool {
	return len(state.Contributions) == 0 && len(state.Checkpoints) == 0 && len(state.Continuations) == 0 &&
		state.Supersession == nil && state.Destructive == nil && len(state.Metrics) == 0
}

func validateRecoveryDynamics(state RecoveryDynamics, manifest Manifest, required map[string]bool) error {
	if state.ParentAcknowledgementEventID == "" || len(state.Items) != len(required) {
		return invalid("recovery observation coverage")
	}
	seen := make(map[string]bool, len(state.Items))
	for _, item := range state.Items {
		if !required[item.WorkItemID] || seen[item.WorkItemID] || item.Role == "" || item.ScheduleEventID == "" || item.TerminalEventID == "" ||
			!slices.Contains([]string{
				RecoveryDispositionSucceeded, RecoveryDispositionRejected,
				RecoveryDispositionQuarantined, RecoveryDispositionUnresolved,
			}, item.Disposition) || item.ActivityAttempts < 0 || item.AgentProcesses < 0 || item.AcceptedEffects < 0 ||
			item.AcceptedResults < 0 || item.CostUnits < 0 {
			return invalid("recovery item observation")
		}
		if item.Admitted && item.StartEventID == "" || !item.Admitted && item.Disposition != RecoveryDispositionRejected {
			return invalid("recovery admission observation")
		}
		seen[item.WorkItemID] = true
	}
	boundSpecs := []metricSpec{
		{name: "requests_per_item_max", unit: "count"},
		{name: "retry_concurrency_max", unit: "count"},
		{name: "in_flight_max", unit: "count"},
		{name: "poison_attempts_max", unit: "count"},
		{name: "progress_deadline_ms", unit: "ms"},
	}
	if err := validateMetrics(state.Bounds, boundSpecs, "recovery bound"); err != nil {
		return err
	}
	wantBounds := map[string]int64{
		"requests_per_item_max": 4,
		"retry_concurrency_max": 2,
		"in_flight_max":         8,
		"poison_attempts_max":   3,
		"progress_deadline_ms":  5000,
	}
	for _, bound := range state.Bounds {
		if bound.Value != wantBounds[bound.Name] {
			return invalid("recovery bound value")
		}
	}
	return validateMetrics(state.Metrics, metricSpecs(manifest.Case), "recovery")
}

func validSortedMembers(members []string) bool {
	if len(members) == 0 || !slices.IsSorted(members) {
		return false
	}
	for _, member := range members {
		if member == "" {
			return false
		}
	}
	return true
}

func validAppliedDisposition(decision string, applied bool, receiptID string) bool {
	switch decision {
	case DecisionAccepted:
		return applied && receiptID != ""
	case DecisionReconciled:
		return !applied && receiptID != ""
	case DecisionRejected:
		return !applied && receiptID == ""
	default:
		return false
	}
}

func validateFaultBoundary(boundary FaultBoundary, manifest Manifest) error {
	if boundary.RunID == "" {
		return invalid("fault boundary run identity")
	}
	if !boundary.Injected {
		if manifest.Probe != ProbeUnfaulted || boundary.ExpectedBoundary != "" || boundary.BarrierEventID != "" || boundary.FaultEventID != "" || boundary.TargetProcessIdentity != "" {
			return invalid("unfaulted boundary")
		}
		return nil
	}
	if manifest.Probe == ProbeUnfaulted || boundary.ExpectedBoundary != manifest.Boundary || boundary.BarrierEventID == "" ||
		boundary.FaultEventID == "" || boundary.TargetProcessIdentity == "" {
		return invalid("injected fault boundary")
	}
	return nil
}

func validateNativeHistory(history NativeHistory) error {
	if history.RunID == "" || !history.Captured || history.EventCount < 1 || !validSHA256(history.HistorySHA256) ||
		!validSHA256(history.ReplayWorkerSHA256) {
		return invalid("native history")
	}
	digest, err := NativeExportSHA256(history.Export)
	if err != nil || digest != history.HistorySHA256 {
		return invalid("native history export hash")
	}
	eventCount, _, err := NativeExportEventCounts(history.Export)
	if err != nil || eventCount != history.EventCount {
		return invalid("native history event count")
	}
	if history.ReplayCompatible && history.ReplayError != "" || !history.ReplayCompatible && history.ReplayError == "" {
		return invalid("native history replay result")
	}
	return nil
}

type nativeExecutionHistory struct {
	WorkflowID string          `json:"workflow_id"`
	RunID      string          `json:"run_id"`
	History    json.RawMessage `json:"history"`
}

type nativeHistoryEnvelope struct {
	RunID      string                   `json:"run_id,omitempty"`
	EventCount *int                     `json:"event_count,omitempty"`
	Fixture    bool                     `json:"fixture,omitempty"`
	Parent     *nativeExecutionHistory  `json:"parent,omitempty"`
	Children   []nativeExecutionHistory `json:"children,omitempty"`
}

// NativeExportEventCounts reconstructs total and completed Workflow Task event
// counts from the sealed native-history export. Synthetic fixture envelopes do
// not contain Temporal events and therefore report zero Workflow Tasks.
func NativeExportEventCounts(export json.RawMessage) (int, int, error) {
	decoder := json.NewDecoder(bytes.NewReader(export))
	decoder.DisallowUnknownFields()
	var envelope nativeHistoryEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return 0, 0, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return 0, 0, invalid("native history export trailing JSON")
	}
	if envelope.EventCount != nil {
		if !envelope.Fixture || envelope.RunID == "" || *envelope.EventCount < 1 || envelope.Parent != nil || len(envelope.Children) != 0 {
			return 0, 0, invalid("synthetic native history export")
		}
		return *envelope.EventCount, 0, nil
	}
	if envelope.RunID != "" || envelope.Fixture || envelope.Parent == nil {
		return 0, 0, invalid("Temporal native history export")
	}
	count, workflowTasks, err := executionHistoryEventCounts(*envelope.Parent)
	if err != nil {
		return 0, 0, err
	}
	seen := map[string]bool{envelope.Parent.WorkflowID + "\x00" + envelope.Parent.RunID: true}
	for _, child := range envelope.Children {
		key := child.WorkflowID + "\x00" + child.RunID
		if seen[key] {
			return 0, 0, invalid("duplicate native history execution")
		}
		seen[key] = true
		childCount, childWorkflowTasks, err := executionHistoryEventCounts(child)
		if err != nil {
			return 0, 0, err
		}
		count += childCount
		workflowTasks += childWorkflowTasks
	}
	return count, workflowTasks, nil
}

// NativeExportIsFixture distinguishes deterministic apparatus history from a
// captured Temporal execution after validating the complete envelope.
func NativeExportIsFixture(export json.RawMessage) (bool, error) {
	if _, _, err := NativeExportEventCounts(export); err != nil {
		return false, err
	}
	decoder := json.NewDecoder(bytes.NewReader(export))
	decoder.DisallowUnknownFields()
	var envelope nativeHistoryEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return false, err
	}
	return envelope.EventCount != nil, nil
}

func executionHistoryEventCounts(execution nativeExecutionHistory) (int, int, error) {
	if execution.WorkflowID == "" || execution.RunID == "" || len(execution.History) == 0 {
		return 0, 0, invalid("native history execution identity")
	}
	decoder := json.NewDecoder(bytes.NewReader(execution.History))
	decoder.DisallowUnknownFields()
	var history struct {
		Events []json.RawMessage `json:"events"`
	}
	if err := decoder.Decode(&history); err != nil || len(history.Events) == 0 {
		return 0, 0, invalid("native history execution events")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return 0, 0, invalid("native history execution trailing JSON")
	}
	workflowTasks := 0
	for _, rawEvent := range history.Events {
		var event map[string]json.RawMessage
		if err := json.Unmarshal(rawEvent, &event); err != nil {
			return 0, 0, invalid("native history event")
		}
		if _, ok := event["workflow_task_completed_event_attributes"]; ok {
			workflowTasks++
		}
	}
	return len(history.Events), workflowTasks, nil
}

func NativeExportSHA256(export json.RawMessage) (string, error) {
	compact, err := compactNativeExport(export)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(compact.Bytes())
	return hex.EncodeToString(digest[:]), nil
}

// NativeExportByteCount returns the canonical compact JSON byte count so
// evidence-file indentation cannot change the reported history size.
func NativeExportByteCount(export json.RawMessage) (int, error) {
	compact, err := compactNativeExport(export)
	if err != nil {
		return 0, err
	}
	return compact.Len(), nil
}

func compactNativeExport(export json.RawMessage) (*bytes.Buffer, error) {
	if len(export) == 0 || !json.Valid(export) || bytes.Equal(bytes.TrimSpace(export), []byte("null")) {
		return nil, invalid("native history export")
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, export); err != nil {
		return nil, invalid("native history export")
	}
	return &compact, nil
}

func validateProcessObservations(observations ProcessObservations) error {
	if observations.RunID == "" || len(observations.Observations) == 0 {
		return invalid("process observations")
	}
	for _, observation := range observations.Observations {
		if observation.EventID == "" || observation.WorkItemID == "" || observation.WorkerID == "" || observation.WorkerPID < 1 ||
			observation.ProcessIdentity == "" || observation.State == "" {
			return invalid("process observation")
		}
	}
	return nil
}

func validateEffectiveInput(input EffectiveInput, manifest Manifest) error {
	if input.RunID == "" || input.PairID != manifest.PairID || input.ScheduleBlockID != manifest.ScheduleBlockID ||
		input.Topology != manifest.Topology || input.Case != manifest.Case || input.Boundary != manifest.Boundary ||
		input.Probe != manifest.Probe || input.Fanout != manifest.Fanout {
		return invalid("effective input identity")
	}
	for _, hash := range []string{input.WorkloadSHA256, input.ActivityOptionsSHA256, input.HostEnvelopeSHA256, input.AgentBinarySHA256,
		input.DestinationProtocolSHA256, input.BarrierControllerSHA256, input.SourceSHA256} {
		if !validSHA256(hash) {
			return invalid("effective input hash")
		}
	}
	return nil
}

func validateVerdict(verdict Verdict) error {
	if verdict.ProtocolVersion != PublicationProtocolVersion || verdict.RunID == "" ||
		!slices.Contains([]Admission{AdmissionValid, AdmissionInvalid}, verdict.Admission) ||
		!validOutcome(verdict.Correctness) || !validOutcome(verdict.Safety) || !validOutcome(verdict.Liveness) ||
		!validOutcome(verdict.Diagnosability) || verdict.Oracle != OracleProtocolVersion {
		return invalid("verdict")
	}
	if verdict.Admission == AdmissionInvalid && len(verdict.ReasonCodes) == 0 ||
		verdict.EfficiencyEligible && (verdict.Admission != AdmissionValid || verdict.Correctness != OutcomePass ||
			verdict.Safety != OutcomePass || verdict.Liveness != OutcomePass || verdict.Diagnosability != OutcomePass) {
		return invalid("verdict admission or efficiency")
	}
	return nil
}

func validateTiming(events []TimingEvent) error {
	if len(events) == 0 {
		return invalid("publication timing")
	}
	var previous time.Time
	var previousOffset int64
	for index, event := range events {
		parsed, err := time.Parse(time.RFC3339Nano, event.TimestampUTC)
		if err != nil || event.Sequence != index+1 || event.Kind == "" || event.MonotonicOffsetNS < 0 ||
			(index > 0 && (parsed.Before(previous) || event.MonotonicOffsetNS < previousOffset)) {
			return invalid("publication timing event")
		}
		_, offset := parsed.Zone()
		if offset != 0 {
			return invalid("publication timing UTC")
		}
		previous, previousOffset = parsed, event.MonotonicOffsetNS
	}
	return nil
}

func validateExecution(execution PublicationExecution, manifest Manifest) error {
	if execution.ProtocolVersion != PublicationProtocolVersion || execution.RunID == "" || execution.PairID != manifest.PairID ||
		execution.ScheduleBlockID != manifest.ScheduleBlockID || execution.Topology != manifest.Topology {
		return invalid("publication execution")
	}
	return nil
}

func validOutcome(outcome Outcome) bool {
	return slices.Contains([]Outcome{OutcomePass, OutcomeFail, OutcomeUnresolved}, outcome)
}

func EvidenceFileSetWithoutInventory() []string {
	return slices.Clone(requiredEvidenceFiles[:len(requiredEvidenceFiles)-1])
}
