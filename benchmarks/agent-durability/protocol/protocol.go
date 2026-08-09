package protocol

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"time"
)

const ContractVersion = "adl.cross-system.v1"

const (
	AgentProtocol       = "agent-durability-simulator-v1"
	AuthorityProtocol   = "logical-session-generation-capability-v1"
	DestinationProtocol = "effect-id-physical-attempt-journal-v1"
	FailureProtocol     = "named-barrier-controller-v1"
	OracleProtocol      = "independent-black-box-oracle-v1"
)

const (
	FaultPointProcessCreatedBeforeVendorRegistration = "process-created-before-vendor-registration"
	FaultPointToolEffectBeforeActivityCompletion     = "tool-effect-before-activity-completion"
	FaultPointFinalOutputBeforeActivityCompletion    = "final-output-before-activity-completion"
)

const (
	ManifestFile            = "manifest.json"
	CommonEventsFile        = "common-events.jsonl"
	AuthorityStateFile      = "authority-state.json"
	DestinationStateFile    = "destination-state.json"
	FaultBoundaryFile       = "fault-boundary.json"
	NativeJournalFile       = "native-history-or-journal-export.json"
	ProcessObservationsFile = "process-observations.json"
	EffectiveInputFile      = "effective-input.json"
	VerdictFile             = "verdict.json"
)

var (
	ErrInvalidEvidence = errors.New("invalid benchmark evidence")
	ErrEvidenceExists  = errors.New("benchmark evidence already exists")
)

type CaseID string

const (
	CaseSurvivingExecutor       CaseID = "surviving-executor"
	CaseAmbiguousEffect         CaseID = "ambiguous-effect"
	CaseStaleGeneration         CaseID = "stale-generation"
	CaseCancellationUnreachable CaseID = "cancellation-unreachable"
)

func Cases() []CaseID {
	return []CaseID{
		CaseSurvivingExecutor,
		CaseAmbiguousEffect,
		CaseStaleGeneration,
		CaseCancellationUnreachable,
	}
}

func (c CaseID) Valid() bool {
	return slices.Contains(Cases(), c)
}

type Probe string

const (
	ProbeUnfaulted Probe = "unfaulted"
	ProbeUnsafe    Probe = "unsafe"
	ProbeProtected Probe = "protected"
)

func (p Probe) Valid() bool {
	return p == ProbeUnfaulted || p == ProbeUnsafe || p == ProbeProtected
}

type VerdictClass string

const (
	VerdictValidPass VerdictClass = "valid-pass"
	VerdictValidFail VerdictClass = "valid-fail"
	VerdictInvalid   VerdictClass = "invalid"
)

const (
	ReasonEvidenceMissing         = "evidence_missing"
	ReasonEvidenceMalformed       = "evidence_malformed"
	ReasonEvidenceHashMismatch    = "evidence_hash_mismatch"
	ReasonEvidenceInconsistent    = "evidence_inconsistent"
	ReasonCasePreconditionMissing = "case_precondition_missing"
	ReasonFaultNotBracketed       = "fault_not_bracketed"
	ReasonWrongProcessIdentity    = "wrong_process_identity"
	ReasonMultipleOutcomes        = "multiple_accepted_outcomes"
	ReasonCompetingOwner          = "competing_owner"
	ReasonDuplicateEffect         = "duplicate_physical_effect"
	ReasonStaleActionAccepted     = "stale_action_accepted"
	ReasonCurrentOwnerStopped     = "current_owner_stopped"
	ReasonPostCancelMutation      = "post_cancellation_mutation"
	ReasonPostCancelReplacement   = "post_cancellation_replacement"
)

const (
	EventExecutorRegistered    = "executor_registered"
	EventBarrierReached        = "barrier_reached"
	EventExecutorAttached      = "executor_attached"
	EventEffectAttempted       = "effect_attempted"
	EventEffectAccepted        = "effect_accepted"
	EventEffectRejected        = "effect_rejected"
	EventOutcomeAccepted       = "outcome_accepted"
	EventOwnerReplaced         = "owner_replaced"
	EventStaleCompletion       = "stale_completion"
	EventStaleStop             = "stale_stop"
	EventCancellationCommitted = "cancellation_committed"
	EventReplacementRejected   = "replacement_rejected"
)

type Manifest struct {
	ContractVersion string            `json:"contract_version"`
	RunID           string            `json:"run_id"`
	Case            CaseID            `json:"case"`
	Probe           Probe             `json:"probe"`
	Trial           int               `json:"trial"`
	SessionID       string            `json:"session_id"`
	InputSHA256     string            `json:"effective_input_sha256"`
	EvidenceSHA256  map[string]string `json:"evidence_sha256"`
}

func (m Manifest) Validate() error {
	if m.ContractVersion != ContractVersion || m.RunID == "" || !m.Case.Valid() || !m.Probe.Valid() ||
		m.Trial < 1 || m.SessionID == "" || m.InputSHA256 == "" || len(m.EvidenceSHA256) == 0 {
		return fmt.Errorf("%w: incomplete or unsupported manifest", ErrInvalidEvidence)
	}
	expected := RawEvidenceFiles()[1:]
	if len(m.EvidenceSHA256) != len(expected) {
		return fmt.Errorf("%w: manifest must name exactly the raw evidence files", ErrInvalidEvidence)
	}
	for _, name := range expected {
		if m.EvidenceSHA256[name] == "" {
			return fmt.Errorf("%w: manifest lacks hash for %s", ErrInvalidEvidence, name)
		}
	}
	return nil
}

type Event struct {
	Sequence          uint64 `json:"sequence"`
	Time              string `json:"time"`
	Kind              string `json:"kind"`
	SessionID         string `json:"session_id"`
	ActorID           string `json:"actor_id,omitempty"`
	Generation        uint64 `json:"generation,omitempty"`
	ProcessIdentity   string `json:"process_identity,omitempty"`
	LogicalEffectID   string `json:"logical_effect_id,omitempty"`
	PhysicalAttemptID string `json:"physical_attempt_id,omitempty"`
	Decision          string `json:"decision,omitempty"`
}

func (e Event) Validate() error {
	if e.Sequence == 0 || e.Time == "" || e.Kind == "" || e.SessionID == "" {
		return fmt.Errorf("%w: incomplete event", ErrInvalidEvidence)
	}
	if _, err := time.Parse(time.RFC3339Nano, e.Time); err != nil {
		return fmt.Errorf("%w: event time: %v", ErrInvalidEvidence, err)
	}
	if eventNeedsActor(e.Kind) && (e.ActorID == "" || e.Generation == 0 || e.ProcessIdentity == "") {
		return fmt.Errorf("%w: event %q lacks stable actor identity", ErrInvalidEvidence, e.Kind)
	}
	if eventIsEffect(e.Kind) && (e.LogicalEffectID == "" || e.PhysicalAttemptID == "") {
		return fmt.Errorf("%w: event %q lacks effect identity", ErrInvalidEvidence, e.Kind)
	}
	if !validEventDecision(e.Kind, e.Decision) {
		return fmt.Errorf("%w: unsupported event kind or decision %q/%q", ErrInvalidEvidence, e.Kind, e.Decision)
	}
	return nil
}

func validEventDecision(kind, decision string) bool {
	switch kind {
	case EventExecutorRegistered:
		return decision == "observed" || decision == "accepted"
	case EventBarrierReached:
		return decision == "blocked"
	case EventExecutorAttached:
		return decision == "observed"
	case EventEffectAccepted, EventOutcomeAccepted, EventOwnerReplaced, EventCancellationCommitted:
		return decision == "accepted"
	case EventEffectRejected:
		return decision == "duplicate" || decision == "stale_generation" || decision == "canceled"
	case EventStaleCompletion, EventStaleStop:
		return decision == "accepted" || decision == "rejected"
	case EventReplacementRejected:
		return decision == "canceled"
	default:
		return false
	}
}

func eventNeedsActor(kind string) bool {
	return kind != EventCancellationCommitted
}

func eventIsEffect(kind string) bool {
	return kind == EventEffectAttempted || kind == EventEffectAccepted || kind == EventEffectRejected
}

type AcceptedAction struct {
	Kind       string `json:"kind"`
	Generation uint64 `json:"generation"`
	Sequence   uint64 `json:"sequence"`
}

type AuthorityState struct {
	SessionID                    string           `json:"session_id"`
	ActiveGeneration             uint64           `json:"active_generation"`
	ConcurrentOwnerCount         int              `json:"concurrent_owner_count"`
	CurrentOwnerAlive            bool             `json:"current_owner_alive"`
	AcceptedOutcomes             []AcceptedAction `json:"accepted_outcomes"`
	AcceptedActions              []AcceptedAction `json:"accepted_actions"`
	CancellationCommitted        bool             `json:"cancellation_committed"`
	CancellationSequence         uint64           `json:"cancellation_sequence,omitempty"`
	ReplacementAfterCancellation bool             `json:"replacement_after_cancellation"`
}

func (s AuthorityState) Validate(sessionID string) error {
	if s.SessionID != sessionID || s.ActiveGeneration == 0 || s.ConcurrentOwnerCount < 0 {
		return fmt.Errorf("%w: invalid authority state", ErrInvalidEvidence)
	}
	if s.CancellationCommitted && s.CancellationSequence == 0 {
		return fmt.Errorf("%w: canceled authority lacks sequence", ErrInvalidEvidence)
	}
	return nil
}

type DestinationAttempt struct {
	LogicalEffectID   string `json:"logical_effect_id"`
	PhysicalAttemptID string `json:"physical_attempt_id"`
	Generation        uint64 `json:"generation"`
	Sequence          uint64 `json:"sequence"`
	Applied           bool   `json:"applied"`
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
		if attempt.LogicalEffectID == "" || attempt.PhysicalAttemptID == "" || attempt.Generation == 0 || attempt.Sequence == 0 || seen[attempt.PhysicalAttemptID] {
			return fmt.Errorf("%w: invalid or duplicate destination attempt", ErrInvalidEvidence)
		}
		seen[attempt.PhysicalAttemptID] = true
	}
	return nil
}

type FaultBoundary struct {
	Point           string `json:"point"`
	Triggered       bool   `json:"triggered"`
	AfterSequence   uint64 `json:"after_sequence,omitempty"`
	BeforeSequence  uint64 `json:"before_sequence,omitempty"`
	ActorID         string `json:"actor_id,omitempty"`
	ProcessIdentity string `json:"process_identity,omitempty"`
	TriggeredAt     string `json:"triggered_at,omitempty"`
}

type ProcessObservation struct {
	Sequence        uint64 `json:"sequence"`
	ActorID         string `json:"actor_id"`
	Generation      uint64 `json:"generation"`
	ProcessIdentity string `json:"process_identity"`
	State           string `json:"state"`
}

func (o ProcessObservation) Validate() error {
	if o.Sequence == 0 || o.ActorID == "" || o.Generation == 0 || o.ProcessIdentity == "" || o.State == "" {
		return fmt.Errorf("%w: incomplete process observation", ErrInvalidEvidence)
	}
	return nil
}

type NativeRecord struct {
	Sequence uint64 `json:"sequence"`
	Kind     string `json:"kind"`
	Detail   string `json:"detail"`
}

func (r NativeRecord) Validate() error {
	if r.Sequence == 0 || r.Kind == "" || r.Detail == "" {
		return fmt.Errorf("%w: incomplete native record", ErrInvalidEvidence)
	}
	return nil
}

type EffectiveInput struct {
	AdapterID           string            `json:"adapter_id"`
	AdapterVersion      string            `json:"adapter_version"`
	AgentProtocol       string            `json:"agent_protocol"`
	AgentBinarySHA256   string            `json:"agent_binary_sha256"`
	AuthorityProtocol   string            `json:"authority_protocol"`
	DestinationProtocol string            `json:"destination_protocol"`
	DestinationID       string            `json:"destination_id"`
	FailureProtocol     string            `json:"failure_protocol"`
	OracleProtocol      string            `json:"oracle_protocol"`
	OracleVisibility    []string          `json:"oracle_visibility"`
	Runtime             string            `json:"runtime"`
	Settings            map[string]string `json:"settings"`
}

func (i EffectiveInput) Validate() error {
	if i.AdapterID == "" || i.AdapterVersion == "" || i.DestinationID == "" || i.Runtime == "" || len(i.Settings) == 0 ||
		i.AgentProtocol != AgentProtocol || i.AuthorityProtocol != AuthorityProtocol ||
		i.DestinationProtocol != DestinationProtocol || i.FailureProtocol != FailureProtocol ||
		i.OracleProtocol != OracleProtocol {
		return fmt.Errorf("%w: incomplete effective input", ErrInvalidEvidence)
	}
	decodedHash, err := hex.DecodeString(i.AgentBinarySHA256)
	if err != nil || len(decodedHash) != sha256.Size {
		return fmt.Errorf("%w: invalid agent binary hash", ErrInvalidEvidence)
	}
	wantVisibility := []string{AuthorityStateFile, DestinationStateFile, FaultBoundaryFile, ProcessObservationsFile}
	if !slices.Equal(i.OracleVisibility, wantVisibility) {
		return fmt.Errorf("%w: invalid oracle visibility", ErrInvalidEvidence)
	}
	return nil
}

type Metrics struct {
	AcceptedOutcomeCount   int `json:"accepted_outcome_count"`
	PhysicalEffectCount    int `json:"physical_effect_count"`
	PhysicalAttemptCount   int `json:"physical_attempt_count"`
	StaleActionAcceptCount int `json:"stale_action_accept_count"`
	ConcurrentOwnerCount   int `json:"concurrent_owner_count"`
	PostCancelAcceptCount  int `json:"post_cancel_accept_count"`
}

type Verdict struct {
	ContractVersion string       `json:"contract_version"`
	RunID           string       `json:"run_id"`
	Case            CaseID       `json:"case"`
	Probe           Probe        `json:"probe"`
	Trial           int          `json:"trial"`
	Class           VerdictClass `json:"class"`
	ReasonCodes     []string     `json:"reason_codes"`
	Metrics         Metrics      `json:"metrics"`
	Oracle          string       `json:"oracle"`
}

func RawEvidenceFiles() []string {
	return []string{
		ManifestFile,
		CommonEventsFile,
		AuthorityStateFile,
		DestinationStateFile,
		FaultBoundaryFile,
		NativeJournalFile,
		ProcessObservationsFile,
		EffectiveInputFile,
	}
}

func FileSHA256(path string) (digest string, returnErr error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() {
		returnErr = errors.Join(returnErr, file.Close())
	}()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
