package protocol

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"time"
)

type Manifest struct {
	ContractVersion string            `json:"contract_version"`
	RunID           string            `json:"run_id"`
	Suite           SuiteID           `json:"suite"`
	Case            CaseID            `json:"case"`
	Probe           Probe             `json:"probe"`
	Trial           int               `json:"trial"`
	EpisodeID       string            `json:"episode_id"`
	Seed            int64             `json:"seed"`
	CohortSize      int               `json:"cohort_size"`
	InputSHA256     string            `json:"effective_input_sha256"`
	EvidenceSHA256  map[string]string `json:"evidence_sha256"`
}

func (m Manifest) Validate() error {
	if m.ContractVersion != ContractVersion || m.RunID == "" || !m.Case.Valid() || m.Suite != m.Case.Suite() ||
		!m.Probe.Valid() || m.Trial < 1 || m.EpisodeID == "" || m.CohortSize < 1 || !validSHA256(m.InputSHA256) {
		return fmt.Errorf("%w: incomplete or unsupported manifest", ErrInvalidEvidence)
	}
	expected := RawEvidenceFiles()[1:]
	if len(m.EvidenceSHA256) != len(expected) {
		return fmt.Errorf("%w: manifest must hash every raw evidence file", ErrInvalidEvidence)
	}
	for _, name := range expected {
		if !validSHA256(m.EvidenceSHA256[name]) {
			return fmt.Errorf("%w: manifest lacks valid hash for %s", ErrInvalidEvidence, name)
		}
	}
	if m.InputSHA256 != m.EvidenceSHA256[EffectiveInputFile] {
		return fmt.Errorf("%w: effective input hash differs from inventory hash", ErrInvalidEvidence)
	}
	return nil
}

type OwnerEpochState string

const (
	OwnerEpochActive    OwnerEpochState = "active"
	OwnerEpochObsolete  OwnerEpochState = "obsolete"
	OwnerEpochCompleted OwnerEpochState = "completed"
	OwnerEpochRevoked   OwnerEpochState = "revoked"
)

type OwnerEpoch struct {
	OwnerID        string          `json:"owner_id"`
	Generation     uint64          `json:"generation"`
	CapabilityHash string          `json:"capability_hash"`
	State          OwnerEpochState `json:"state"`
	Sequence       uint64          `json:"sequence"`
}

type AcceptedAction struct {
	Kind           string `json:"kind"`
	OwnerID        string `json:"owner_id"`
	Generation     uint64 `json:"generation"`
	CapabilityHash string `json:"capability_hash"`
	EventID        string `json:"event_id"`
}

type AuthorityState struct {
	LogicalOperationID    string           `json:"logical_operation_id"`
	CurrentOwnerID        string           `json:"current_owner_id"`
	CurrentGeneration     uint64           `json:"current_generation"`
	CurrentCapabilityHash string           `json:"current_capability_hash"`
	CurrentOwnerAlive     bool             `json:"current_owner_alive"`
	Epochs                []OwnerEpoch     `json:"epochs"`
	AcceptedActions       []AcceptedAction `json:"accepted_actions"`
}

func (s AuthorityState) Validate() error {
	if s.LogicalOperationID == "" || s.CurrentOwnerID == "" || s.CurrentGeneration == 0 ||
		!validSHA256(s.CurrentCapabilityHash) || len(s.Epochs) == 0 {
		return fmt.Errorf("%w: incomplete authority state", ErrInvalidEvidence)
	}
	seen := make(map[uint64]bool, len(s.Epochs))
	active := 0
	foundCurrent := false
	var previousGeneration uint64
	for _, epoch := range s.Epochs {
		if epoch.OwnerID == "" || epoch.Generation == 0 || !validSHA256(epoch.CapabilityHash) || epoch.Sequence == 0 ||
			seen[epoch.Generation] || epoch.Generation <= previousGeneration || !validOwnerEpochState(epoch.State) {
			return fmt.Errorf("%w: invalid authority epoch", ErrInvalidEvidence)
		}
		seen[epoch.Generation] = true
		previousGeneration = epoch.Generation
		if epoch.State == OwnerEpochActive {
			active++
		}
		if epoch.Generation == s.CurrentGeneration && epoch.OwnerID == s.CurrentOwnerID && epoch.CapabilityHash == s.CurrentCapabilityHash && epoch.State == OwnerEpochActive {
			foundCurrent = true
		}
	}
	if active != 1 || !foundCurrent {
		return fmt.Errorf("%w: current authority does not name the one active epoch", ErrInvalidEvidence)
	}
	for _, action := range s.AcceptedActions {
		if action.Kind == "" || action.OwnerID == "" || action.Generation == 0 || !validSHA256(action.CapabilityHash) ||
			action.EventID == "" || !seen[action.Generation] {
			return fmt.Errorf("%w: invalid accepted authority action", ErrInvalidEvidence)
		}
	}
	return nil
}

func validOwnerEpochState(state OwnerEpochState) bool {
	return state == OwnerEpochActive || state == OwnerEpochObsolete || state == OwnerEpochCompleted || state == OwnerEpochRevoked
}

type DestinationAttempt struct {
	LogicalOperationID string `json:"logical_operation_id"`
	LogicalEffectID    string `json:"logical_effect_id"`
	PhysicalAttemptID  string `json:"physical_attempt_id"`
	OwnerID            string `json:"owner_id"`
	Generation         uint64 `json:"generation"`
	CapabilityHash     string `json:"capability_hash"`
	EventID            string `json:"event_id"`
	Decision           string `json:"decision"`
	Applied            bool   `json:"applied"`
}

type DestinationState struct {
	DestinationID string               `json:"destination_id"`
	Attempts      []DestinationAttempt `json:"attempts"`
}

func (s DestinationState) Validate() error {
	if s.DestinationID == "" {
		return fmt.Errorf("%w: destination identity is required", ErrInvalidEvidence)
	}
	seen := make(map[string]bool, len(s.Attempts))
	for _, attempt := range s.Attempts {
		if attempt.LogicalOperationID == "" || attempt.LogicalEffectID == "" || attempt.PhysicalAttemptID == "" ||
			attempt.OwnerID == "" || attempt.Generation == 0 || !validSHA256(attempt.CapabilityHash) || attempt.EventID == "" ||
			(attempt.Decision != DecisionAccepted && attempt.Decision != DecisionRejected) || seen[attempt.PhysicalAttemptID] ||
			(attempt.Applied != (attempt.Decision == DecisionAccepted)) {
			return fmt.Errorf("%w: invalid destination attempt", ErrInvalidEvidence)
		}
		seen[attempt.PhysicalAttemptID] = true
	}
	return nil
}

type DependencyStatus string

const (
	DependencyHealthy    DependencyStatus = "healthy"
	DependencyOutage     DependencyStatus = "outage"
	DependencyRecovering DependencyStatus = "recovering"
)

type DependencyTransition struct {
	Sequence uint64           `json:"sequence"`
	Time     string           `json:"time"`
	State    DependencyStatus `json:"state"`
}

type DependencyRequest struct {
	RequestID          string     `json:"request_id"`
	LogicalOperationID string     `json:"logical_operation_id"`
	WorkItemID         string     `json:"work_item_id"`
	AttemptID          string     `json:"attempt_id"`
	ParentAttemptID    string     `json:"parent_attempt_id,omitempty"`
	RetryLayer         RetryLayer `json:"retry_layer"`
	RetryOrdinal       int        `json:"retry_ordinal"`
	StartedAt          string     `json:"started_at"`
	FinishedAt         string     `json:"finished_at"`
	Outcome            string     `json:"outcome"`
	CostUnits          int64      `json:"cost_units"`
}

type DependencyState struct {
	DependencyID string                 `json:"dependency_id"`
	Transitions  []DependencyTransition `json:"transitions"`
	Requests     []DependencyRequest    `json:"requests"`
}

func (s DependencyState) Validate() error {
	if s.DependencyID == "" || len(s.Transitions) == 0 {
		return fmt.Errorf("%w: dependency identity and transitions are required", ErrInvalidEvidence)
	}
	var previousTransition time.Time
	for index, transition := range s.Transitions {
		parsed, err := parseUTC(transition.Time)
		if transition.Sequence != uint64(index+1) || err != nil || !validDependencyStatus(transition.State) ||
			(!previousTransition.IsZero() && parsed.Before(previousTransition)) {
			return fmt.Errorf("%w: invalid dependency transition", ErrInvalidEvidence)
		}
		previousTransition = parsed
	}
	seen := make(map[string]bool, len(s.Requests))
	for _, request := range s.Requests {
		started, startErr := parseUTC(request.StartedAt)
		finished, finishErr := parseUTC(request.FinishedAt)
		if request.RequestID == "" || request.LogicalOperationID == "" || request.WorkItemID == "" || request.AttemptID == "" ||
			!request.RetryLayer.Valid() || request.RetryOrdinal < 1 || request.Outcome == "" || request.CostUnits < 0 ||
			startErr != nil || finishErr != nil || finished.Before(started) || seen[request.RequestID] {
			return fmt.Errorf("%w: invalid dependency request", ErrInvalidEvidence)
		}
		if request.RetryOrdinal == 1 && request.ParentAttemptID != "" || request.RetryOrdinal > 1 && request.ParentAttemptID == "" {
			return fmt.Errorf("%w: invalid dependency retry parent", ErrInvalidEvidence)
		}
		seen[request.RequestID] = true
	}
	return nil
}

func validDependencyStatus(status DependencyStatus) bool {
	return status == DependencyHealthy || status == DependencyOutage || status == DependencyRecovering
}

type WorkItemState string

const (
	WorkItemSubmitted   WorkItemState = "submitted"
	WorkItemAdmitted    WorkItemState = "admitted"
	WorkItemRejected    WorkItemState = "rejected"
	WorkItemRunning     WorkItemState = "running"
	WorkItemSucceeded   WorkItemState = "succeeded"
	WorkItemFailed      WorkItemState = "failed"
	WorkItemQuarantined WorkItemState = "quarantined"
	WorkItemCanceled    WorkItemState = "canceled"
)

type WorkItem struct {
	WorkItemID         string        `json:"work_item_id"`
	LogicalOperationID string        `json:"logical_operation_id"`
	Poison             bool          `json:"poison"`
	State              WorkItemState `json:"state"`
}

type WorkloadState struct {
	EpisodeID         string     `json:"episode_id"`
	ExpectedWorkItems int        `json:"expected_work_items"`
	Items             []WorkItem `json:"items"`
}

func (s WorkloadState) Validate() error {
	if s.EpisodeID == "" || s.ExpectedWorkItems < 1 || len(s.Items) != s.ExpectedWorkItems {
		return fmt.Errorf("%w: workload count or episode is inconsistent", ErrInvalidEvidence)
	}
	seen := make(map[string]bool, len(s.Items))
	for _, item := range s.Items {
		if item.WorkItemID == "" || item.LogicalOperationID == "" || !validWorkItemState(item.State) || seen[item.WorkItemID] {
			return fmt.Errorf("%w: invalid work item", ErrInvalidEvidence)
		}
		seen[item.WorkItemID] = true
	}
	return nil
}

func validWorkItemState(state WorkItemState) bool {
	return state == WorkItemSubmitted || state == WorkItemAdmitted || state == WorkItemRejected || state == WorkItemRunning ||
		state == WorkItemSucceeded || state == WorkItemFailed || state == WorkItemQuarantined || state == WorkItemCanceled
}

type FaultBoundary struct {
	Point          string `json:"point"`
	Triggered      bool   `json:"triggered"`
	AfterSequence  uint64 `json:"after_sequence,omitempty"`
	AfterEventID   string `json:"after_event_id,omitempty"`
	BeforeSequence uint64 `json:"before_sequence,omitempty"`
	BeforeEventID  string `json:"before_event_id,omitempty"`
	TriggeredAt    string `json:"triggered_at,omitempty"`
}

func (f FaultBoundary) Validate(probe Probe, events []CausalEvent) error {
	if probe == ProbeUnfaulted {
		if f != (FaultBoundary{}) {
			return fmt.Errorf("%w: unfaulted run contains a fault", ErrInvalidEvidence)
		}
		return nil
	}
	if !f.Triggered || f.Point == "" || f.AfterSequence == 0 || f.BeforeSequence <= f.AfterSequence ||
		f.AfterEventID == "" || f.BeforeEventID == "" || f.TriggeredAt == "" || f.BeforeSequence > uint64(len(events)) {
		return fmt.Errorf("%w: fault lacks exact boundary", ErrInvalidEvidence)
	}
	after := events[f.AfterSequence-1]
	before := events[f.BeforeSequence-1]
	triggered, triggerErr := parseUTC(f.TriggeredAt)
	afterTime, afterErr := parseUTC(after.Time)
	beforeTime, beforeErr := parseUTC(before.Time)
	if after.EventID != f.AfterEventID || before.EventID != f.BeforeEventID || triggerErr != nil || afterErr != nil || beforeErr != nil ||
		!triggered.After(afterTime) || !triggered.Before(beforeTime) {
		return fmt.Errorf("%w: fault is not bracketed by declared causal events", ErrInvalidEvidence)
	}
	return nil
}

type ProcessObservation struct {
	EventID         string `json:"event_id"`
	OwnerID         string `json:"owner_id"`
	Generation      uint64 `json:"generation"`
	WorkerID        string `json:"worker_id"`
	ProcessIdentity string `json:"process_identity"`
	State           string `json:"state"`
}

func (o ProcessObservation) Validate() error {
	if o.EventID == "" || o.OwnerID == "" || o.Generation == 0 || o.WorkerID == "" || o.ProcessIdentity == "" || o.State == "" {
		return fmt.Errorf("%w: incomplete process observation", ErrInvalidEvidence)
	}
	return nil
}

type NativeRecord struct {
	Sequence uint64 `json:"sequence"`
	Time     string `json:"time"`
	Kind     string `json:"kind"`
	Detail   string `json:"detail"`
}

func (r NativeRecord) Validate() error {
	if r.Sequence == 0 || r.Kind == "" || r.Detail == "" {
		return fmt.Errorf("%w: incomplete native record", ErrInvalidEvidence)
	}
	if _, err := parseUTC(r.Time); err != nil {
		return err
	}
	return nil
}

type EffectiveInput struct {
	AdapterID          string            `json:"adapter_id"`
	AdapterVersion     string            `json:"adapter_version"`
	AgentBinarySHA256  string            `json:"agent_binary_sha256"`
	SystemID           string            `json:"system_id"`
	Runtime            string            `json:"runtime"`
	AuthorityProtocol  string            `json:"authority_protocol"`
	DependencyProtocol string            `json:"dependency_protocol"`
	FailureProtocol    string            `json:"failure_protocol"`
	OracleProtocol     string            `json:"oracle_protocol"`
	DestinationID      string            `json:"destination_id"`
	OracleVisibility   []string          `json:"oracle_visibility"`
	HostLimits         map[string]int64  `json:"host_limits"`
	Settings           map[string]string `json:"settings"`
}

func (i EffectiveInput) Validate() error {
	versionHash := strings.TrimPrefix(i.AdapterVersion, "source-sha256:")
	if i.AdapterID == "" || i.SystemID == "" || i.Runtime == "" || i.DestinationID == "" || !validSHA256(versionHash) || !validSHA256(i.AgentBinarySHA256) ||
		i.AdapterVersion == versionHash || i.AuthorityProtocol != AuthorityProtocol || i.DependencyProtocol != DependencyProtocol ||
		i.FailureProtocol != FailureProtocol || i.OracleProtocol != OracleProtocol || !slices.Equal(i.OracleVisibility, OracleVisibility()) ||
		len(i.HostLimits) == 0 || len(i.Settings) == 0 {
		return fmt.Errorf("%w: incomplete effective input", ErrInvalidEvidence)
	}
	for name, value := range i.HostLimits {
		if name == "" || value < 1 {
			return fmt.Errorf("%w: invalid host limit", ErrInvalidEvidence)
		}
	}
	return nil
}

func OracleVisibility() []string {
	return []string{AuthorityStateFile, DestinationStateFile, DependencyStateFile, WorkloadStateFile, FaultBoundaryFile, ProcessObservationsFile}
}

type EvidenceBundle struct {
	Manifest    Manifest             `json:"manifest"`
	Events      []CausalEvent        `json:"events"`
	Authority   AuthorityState       `json:"authority"`
	Destination DestinationState     `json:"destination"`
	Dependency  DependencyState      `json:"dependency"`
	Workload    WorkloadState        `json:"workload"`
	Fault       FaultBoundary        `json:"fault"`
	Processes   []ProcessObservation `json:"processes"`
	Native      []NativeRecord       `json:"native"`
	Input       EffectiveInput       `json:"input"`
}

func (b EvidenceBundle) Validate() error {
	if err := b.Manifest.Validate(); err != nil {
		return err
	}
	if err := ValidateCausalEvents(b.Manifest.RunID, b.Events); err != nil {
		return err
	}
	if err := b.Authority.Validate(); err != nil {
		return err
	}
	if err := b.Destination.Validate(); err != nil {
		return err
	}
	if err := b.Dependency.Validate(); err != nil {
		return err
	}
	if err := b.Workload.Validate(); err != nil {
		return err
	}
	if err := b.Fault.Validate(b.Manifest.Probe, b.Events); err != nil {
		return err
	}
	if err := b.Input.Validate(); err != nil {
		return err
	}
	if b.Manifest.EpisodeID != b.Workload.EpisodeID || b.Manifest.CohortSize != b.Workload.ExpectedWorkItems ||
		b.Input.DestinationID != b.Destination.DestinationID {
		return fmt.Errorf("%w: manifest, workload, input, and destination identities disagree", ErrInvalidEvidence)
	}
	eventIDs := make(map[string]CausalEvent, len(b.Events))
	attempts := make(map[string]CausalEvent)
	for _, event := range b.Events {
		eventIDs[event.EventID] = event
		if event.AttemptID != "" {
			attempts[event.AttemptID] = event
		}
	}
	for _, request := range b.Dependency.Requests {
		event, ok := attempts[request.AttemptID]
		if !ok || event.LogicalOperationID != request.LogicalOperationID || event.WorkItemID != request.WorkItemID ||
			event.RetryLayer != request.RetryLayer || event.RetryOrdinal != request.RetryOrdinal || event.ParentAttemptID != request.ParentAttemptID {
			return fmt.Errorf("%w: dependency request lacks matching causal attempt", ErrInvalidEvidence)
		}
	}
	for _, attempt := range b.Destination.Attempts {
		event, ok := eventIDs[attempt.EventID]
		if !ok || event.LogicalOperationID != attempt.LogicalOperationID || event.LogicalEffectID != attempt.LogicalEffectID ||
			event.PhysicalAttemptID != attempt.PhysicalAttemptID || event.ActorID != attempt.OwnerID ||
			event.Generation != attempt.Generation || event.CapabilityHash != attempt.CapabilityHash || event.Decision != attempt.Decision {
			return fmt.Errorf("%w: destination attempt lacks matching causal event", ErrInvalidEvidence)
		}
	}
	if len(b.Processes) == 0 || len(b.Native) == 0 {
		return fmt.Errorf("%w: process and native evidence are required", ErrInvalidEvidence)
	}
	for _, observation := range b.Processes {
		if err := observation.Validate(); err != nil || eventIDs[observation.EventID].EventID == "" {
			return fmt.Errorf("%w: invalid process evidence", ErrInvalidEvidence)
		}
	}
	for index, record := range b.Native {
		if err := record.Validate(); err != nil || record.Sequence != uint64(index+1) {
			return fmt.Errorf("%w: invalid native evidence", ErrInvalidEvidence)
		}
	}
	return nil
}

func (b EvidenceBundle) Clone() EvidenceBundle {
	clone := b
	clone.Events = append([]CausalEvent(nil), b.Events...)
	for index := range clone.Events {
		clone.Events[index].ParentEventIDs = append([]string(nil), b.Events[index].ParentEventIDs...)
		clone.Events[index].Details = cloneStringMap(b.Events[index].Details)
	}
	clone.Manifest.EvidenceSHA256 = cloneStringMap(b.Manifest.EvidenceSHA256)
	clone.Authority.Epochs = append([]OwnerEpoch(nil), b.Authority.Epochs...)
	clone.Authority.AcceptedActions = append([]AcceptedAction(nil), b.Authority.AcceptedActions...)
	clone.Destination.Attempts = append([]DestinationAttempt(nil), b.Destination.Attempts...)
	clone.Dependency.Transitions = append([]DependencyTransition(nil), b.Dependency.Transitions...)
	clone.Dependency.Requests = append([]DependencyRequest(nil), b.Dependency.Requests...)
	clone.Workload.Items = append([]WorkItem(nil), b.Workload.Items...)
	clone.Processes = append([]ProcessObservation(nil), b.Processes...)
	clone.Native = append([]NativeRecord(nil), b.Native...)
	clone.Input.OracleVisibility = append([]string(nil), b.Input.OracleVisibility...)
	clone.Input.HostLimits = cloneInt64Map(b.Input.HostLimits)
	clone.Input.Settings = cloneStringMap(b.Input.Settings)
	return clone
}

func RawEvidenceFiles() []string {
	return []string{
		ManifestFile,
		CausalEventsFile,
		AuthorityStateFile,
		DestinationStateFile,
		DependencyStateFile,
		WorkloadStateFile,
		FaultBoundaryFile,
		NativeJournalFile,
		ProcessObservationsFile,
		EffectiveInputFile,
	}
}

func parseUTC(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: invalid UTC time: %v", ErrInvalidEvidence, err)
	}
	_, offset := parsed.Zone()
	if offset != 0 {
		return time.Time{}, fmt.Errorf("%w: timestamp is not UTC", ErrInvalidEvidence)
	}
	return parsed, nil
}

func validSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func cloneStringMap[T ~string](source map[string]T) map[string]T {
	if source == nil {
		return nil
	}
	clone := make(map[string]T, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func cloneInt64Map(source map[string]int64) map[string]int64 {
	if source == nil {
		return nil
	}
	clone := make(map[string]int64, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}
