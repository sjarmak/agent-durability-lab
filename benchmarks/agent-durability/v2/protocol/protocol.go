// Package protocol defines the frozen, adapter-neutral evidence contract for
// authority and recovery-dynamics benchmark v2. It intentionally does not
// extend the v1 protocol package in place.
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

const (
	ContractVersion  = "adl.cross-system.v2"
	PreservedVersion = "adl.cross-system.v1"

	AuthorityProtocol  = "logical-operation-owner-epoch-capability-v2"
	DependencyProtocol = "scripted-dependency-physical-request-journal-v2"
	FailureProtocol    = "named-transition-controller-v2"
	OracleProtocol     = "independent-causal-reconstruction-oracle-v2"
)

const (
	ManifestFile            = "manifest.json"
	CausalEventsFile        = "causal-events.jsonl"
	AuthorityStateFile      = "authority-state.json"
	DestinationStateFile    = "destination-state.json"
	DependencyStateFile     = "dependency-state.json"
	WorkloadStateFile       = "workload-state.json"
	FaultBoundaryFile       = "fault-boundary.json"
	NativeJournalFile       = "native-history-or-journal-export.json"
	ProcessObservationsFile = "process-observations.json"
	EffectiveInputFile      = "effective-input.json"
	VerdictFile             = "verdict.json"
)

var (
	ErrInvalidEvidence = errors.New("invalid benchmark v2 evidence")
	ErrEvidenceExists  = errors.New("benchmark v2 evidence already exists")
)

const (
	ReasonEvidenceMissing       = "evidence_missing"
	ReasonEvidenceMalformed     = "evidence_malformed"
	ReasonEvidenceHashMismatch  = "evidence_hash_mismatch"
	ReasonEvidenceInconsistent  = "evidence_inconsistent"
	ReasonFaultNotBracketed     = "fault_not_bracketed"
	ReasonCasePrecondition      = "case_precondition_missing"
	ReasonNegativeControlWeak   = "negative_control_not_distinguishing"
	ReasonStaleActionAccepted   = "stale_action_accepted"
	ReasonCurrentOwnerStopped   = "current_owner_stopped"
	ReasonRetryBudgetExceeded   = "retry_budget_exceeded"
	ReasonRecoveryStorm         = "recovery_storm_bound_exceeded"
	ReasonCapacityExceeded      = "capacity_bound_exceeded"
	ReasonPoisonBudgetExceeded  = "poison_budget_exceeded"
	ReasonHealthyWorkStarved    = "healthy_work_starved"
	ReasonProgressUndetected    = "silent_progress_undetected"
	ReasonAcceptedWorkLost      = "accepted_work_lost"
	ReasonLegitimateWaitRevoked = "legitimate_wait_revoked"
)

type SuiteID string

const (
	SuiteAuthority        SuiteID = "authority"
	SuiteRecoveryDynamics SuiteID = "recovery-dynamics"
)

type CaseID string

const (
	CaseABAReacquisition          CaseID = "aba-reacquisition"
	CaseLayeredRetryAmplification CaseID = "layered-retry-amplification"
	CaseOutageBacklogRecovery     CaseID = "outage-backlog-recovery"
	CaseBackpressureOverload      CaseID = "backpressure-overload"
	CasePoisonWorkIsolation       CaseID = "poison-work-isolation"
	CaseSilentProgress            CaseID = "silent-progress"
)

func Cases() []CaseID {
	return []CaseID{
		CaseABAReacquisition,
		CaseLayeredRetryAmplification,
		CaseOutageBacklogRecovery,
		CaseBackpressureOverload,
		CasePoisonWorkIsolation,
		CaseSilentProgress,
	}
}

func (c CaseID) Valid() bool { return slices.Contains(Cases(), c) }

func (c CaseID) Suite() SuiteID {
	if c == CaseABAReacquisition {
		return SuiteAuthority
	}
	if c.Valid() {
		return SuiteRecoveryDynamics
	}
	return ""
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

type RetryLayer string

const (
	RetryLayerWorkflow RetryLayer = "workflow"
	RetryLayerActivity RetryLayer = "activity"
	RetryLayerClient   RetryLayer = "client"
	RetryLayerAgent    RetryLayer = "agent"
)

func (l RetryLayer) Valid() bool {
	return l == RetryLayerWorkflow || l == RetryLayerActivity || l == RetryLayerClient || l == RetryLayerAgent
}

const (
	EventOperationReady   = "operation_ready"
	EventAttemptScheduled = "attempt_scheduled"
	EventAttemptStarted   = "attempt_started"
	EventAttemptFinished  = "attempt_finished"
	EventBarrierReached   = "barrier_reached"
	EventOwnerChanged     = "owner_changed"
	EventRequestStarted   = "dependency_request_started"
	EventRequestFinished  = "dependency_request_finished"
	EventProgressAccepted = "progress_accepted"
	EventActionAccepted   = "action_accepted"
	EventActionRejected   = "action_rejected"
	EventOutcomeAccepted  = "outcome_accepted"
	EventAcknowledged     = "acknowledged"
	EventFaultCommitted   = "fault_committed"
	EventRecoveryObserved = "recovery_observed"
)

const (
	DecisionObserved = "observed"
	DecisionAccepted = "accepted"
	DecisionRejected = "rejected"
	DecisionBlocked  = "blocked"
	DecisionFailed   = "failed"
)

type CausalEvent struct {
	Sequence            uint64            `json:"sequence"`
	EventID             string            `json:"event_id"`
	ParentEventIDs      []string          `json:"parent_event_ids,omitempty"`
	Time                string            `json:"time"`
	Kind                string            `json:"kind"`
	RunID               string            `json:"run_id"`
	LogicalOperationID  string            `json:"logical_operation_id"`
	WorkItemID          string            `json:"work_item_id,omitempty"`
	AttemptID           string            `json:"attempt_id,omitempty"`
	ParentAttemptID     string            `json:"parent_attempt_id,omitempty"`
	RetryLayer          RetryLayer        `json:"retry_layer,omitempty"`
	RetryOrdinal        int               `json:"retry_ordinal,omitempty"`
	RetryCause          string            `json:"retry_cause,omitempty"`
	ActorID             string            `json:"actor_id,omitempty"`
	Generation          uint64            `json:"generation,omitempty"`
	CapabilityHash      string            `json:"capability_hash,omitempty"`
	SystemExecutionID   string            `json:"system_execution_id,omitempty"`
	WorkerID            string            `json:"worker_id,omitempty"`
	ProcessIdentity     string            `json:"process_identity,omitempty"`
	DependencyRequestID string            `json:"dependency_request_id,omitempty"`
	LogicalEffectID     string            `json:"logical_effect_id,omitempty"`
	PhysicalAttemptID   string            `json:"physical_attempt_id,omitempty"`
	Decision            string            `json:"decision"`
	Details             map[string]string `json:"details,omitempty"`
}

func (e CausalEvent) Validate() error {
	if e.Sequence == 0 || e.EventID == "" || e.Time == "" || e.Kind == "" || e.RunID == "" ||
		e.LogicalOperationID == "" || !validDecision(e.Decision) {
		return fmt.Errorf("%w: incomplete causal event", ErrInvalidEvidence)
	}
	parsed, err := time.Parse(time.RFC3339Nano, e.Time)
	if err != nil {
		return fmt.Errorf("%w: event time: %v", ErrInvalidEvidence, err)
	}
	_, offset := parsed.Zone()
	if offset != 0 {
		return fmt.Errorf("%w: event time is not UTC", ErrInvalidEvidence)
	}
	if !validEventKind(e.Kind) {
		return fmt.Errorf("%w: unknown event kind %q", ErrInvalidEvidence, e.Kind)
	}
	if e.AttemptID == "" {
		if e.ParentAttemptID != "" || e.RetryLayer != "" || e.RetryOrdinal != 0 || e.RetryCause != "" {
			return fmt.Errorf("%w: retry fields lack attempt identity", ErrInvalidEvidence)
		}
	} else {
		if !e.RetryLayer.Valid() || e.RetryOrdinal < 1 {
			return fmt.Errorf("%w: attempt lacks retry layer or ordinal", ErrInvalidEvidence)
		}
		if e.RetryOrdinal == 1 && e.ParentAttemptID != "" {
			return fmt.Errorf("%w: first attempt has a parent attempt", ErrInvalidEvidence)
		}
		if e.RetryOrdinal > 1 && (e.ParentAttemptID == "" || e.RetryCause == "") {
			return fmt.Errorf("%w: retry lacks parent attempt or cause", ErrInvalidEvidence)
		}
	}
	if e.Kind == EventAttemptStarted && (e.AttemptID == "" || e.ActorID == "" || e.Generation == 0 ||
		e.CapabilityHash == "" || e.WorkerID == "" || e.ProcessIdentity == "") {
		return fmt.Errorf("%w: attempt start lacks stable execution authority", ErrInvalidEvidence)
	}
	return nil
}

func ValidateCausalEvents(runID string, events []CausalEvent) error {
	if runID == "" || len(events) == 0 {
		return fmt.Errorf("%w: run identity and causal events are required", ErrInvalidEvidence)
	}
	seenEvents := make(map[string]bool, len(events))
	type attemptIdentity struct {
		parent  string
		layer   RetryLayer
		ordinal int
		cause   string
	}
	seenAttempts := make(map[string]attemptIdentity)
	var previous time.Time
	for index, event := range events {
		if err := event.Validate(); err != nil {
			return err
		}
		if event.RunID != runID || event.Sequence != uint64(index+1) || seenEvents[event.EventID] {
			return fmt.Errorf("%w: event run, sequence, or identity is inconsistent", ErrInvalidEvidence)
		}
		if index > 0 && event.Kind != EventOperationReady && len(event.ParentEventIDs) == 0 {
			return fmt.Errorf("%w: non-root event lacks causal parent", ErrInvalidEvidence)
		}
		for _, parentID := range event.ParentEventIDs {
			if !seenEvents[parentID] {
				return fmt.Errorf("%w: missing or forward causal parent %q", ErrInvalidEvidence, parentID)
			}
		}
		if event.ParentAttemptID != "" {
			if _, ok := seenAttempts[event.ParentAttemptID]; !ok {
				return fmt.Errorf("%w: missing or forward parent attempt %q", ErrInvalidEvidence, event.ParentAttemptID)
			}
		}
		if event.AttemptID != "" {
			identity := attemptIdentity{parent: event.ParentAttemptID, layer: event.RetryLayer, ordinal: event.RetryOrdinal, cause: event.RetryCause}
			if prior, ok := seenAttempts[event.AttemptID]; ok && prior != identity {
				return fmt.Errorf("%w: attempt identity changed", ErrInvalidEvidence)
			}
			seenAttempts[event.AttemptID] = identity
		}
		eventTime, _ := time.Parse(time.RFC3339Nano, event.Time)
		if !previous.IsZero() && eventTime.Before(previous) {
			return fmt.Errorf("%w: event time moves backwards", ErrInvalidEvidence)
		}
		previous = eventTime
		seenEvents[event.EventID] = true
	}
	return nil
}

func validDecision(decision string) bool {
	return decision == DecisionObserved || decision == DecisionAccepted || decision == DecisionRejected ||
		decision == DecisionBlocked || decision == DecisionFailed
}

func validEventKind(kind string) bool {
	return slices.Contains([]string{
		EventOperationReady, EventAttemptScheduled, EventAttemptStarted, EventAttemptFinished,
		EventBarrierReached, EventOwnerChanged, EventRequestStarted, EventRequestFinished,
		EventProgressAccepted, EventActionAccepted, EventActionRejected, EventOutcomeAccepted,
		EventAcknowledged, EventFaultCommitted, EventRecoveryObserved,
	}, kind)
}

type Admission string

const (
	AdmissionValid   Admission = "valid"
	AdmissionInvalid Admission = "invalid"
)

type Outcome string

const (
	OutcomePass          Outcome = "pass"
	OutcomeFail          Outcome = "fail"
	OutcomeUnresolved    Outcome = "unresolved"
	OutcomeNotApplicable Outcome = "not-applicable"
)

func (o Outcome) validForAdmittedRun() bool {
	return o == OutcomePass || o == OutcomeFail || o == OutcomeUnresolved
}

type Metrics struct {
	LogicalOperationCount    int     `json:"logical_operation_count"`
	WorkItemCount            int     `json:"work_item_count"`
	AcceptedOutcomeCount     int     `json:"accepted_outcome_count"`
	StaleActionAcceptCount   int     `json:"stale_action_accept_count"`
	PhysicalRequestCount     int     `json:"physical_request_count"`
	AmplificationFactor      float64 `json:"amplification_factor"`
	PeakQPS                  float64 `json:"peak_qps"`
	PeakRetryConcurrency     int     `json:"peak_retry_concurrency"`
	QueueLatencyMillis       int64   `json:"queue_latency_ms"`
	ExecutionLatencyMillis   int64   `json:"execution_latency_ms"`
	DetectionLatencyMillis   int64   `json:"failure_detection_latency_ms"`
	RetryDelayMillis         int64   `json:"retry_delay_ms"`
	RecoveryLatencyMillis    int64   `json:"recovery_latency_ms"`
	EndToEndLatencyMillis    int64   `json:"end_to_end_latency_ms"`
	BacklogIntegralMillis    int64   `json:"backlog_integral_ms"`
	BacklogDrainP50Millis    int64   `json:"backlog_drain_p50_ms"`
	BacklogDrainP90Millis    int64   `json:"backlog_drain_p90_ms"`
	BacklogDrainP99Millis    int64   `json:"backlog_drain_p99_ms"`
	HealthyTaskLatencyMillis int64   `json:"healthy_task_latency_ms"`
	ThroughputPerSecond      float64 `json:"throughput_per_second"`
	CostUnits                int64   `json:"cost_units"`
	DurableRecordCount       int     `json:"durable_record_count"`
	DurableBytes             int64   `json:"durable_bytes"`
	OperatorInterventions    int     `json:"operator_intervention_count"`
}

type Verdict struct {
	ContractVersion    string    `json:"contract_version"`
	RunID              string    `json:"run_id"`
	Case               CaseID    `json:"case"`
	Probe              Probe     `json:"probe"`
	Trial              int       `json:"trial"`
	Admission          Admission `json:"admission"`
	Correctness        Outcome   `json:"correctness"`
	Safety             Outcome   `json:"safety"`
	Liveness           Outcome   `json:"liveness"`
	Diagnosability     Outcome   `json:"diagnosability"`
	EfficiencyEligible bool      `json:"efficiency_eligible"`
	ReasonCodes        []string  `json:"reason_codes"`
	Metrics            Metrics   `json:"metrics"`
	Oracle             string    `json:"oracle"`
}

func (v Verdict) Validate() error {
	if v.ContractVersion != ContractVersion || v.RunID == "" || !v.Case.Valid() || !v.Probe.Valid() ||
		v.Trial < 1 || (v.Admission != AdmissionValid && v.Admission != AdmissionInvalid) || v.Oracle != OracleProtocol {
		return fmt.Errorf("%w: incomplete verdict identity", ErrInvalidEvidence)
	}
	if v.Admission == AdmissionInvalid {
		if v.Correctness != OutcomeNotApplicable || v.Safety != OutcomeNotApplicable ||
			v.Liveness != OutcomeNotApplicable || v.Diagnosability != OutcomeNotApplicable ||
			v.EfficiencyEligible || len(v.ReasonCodes) == 0 {
			return fmt.Errorf("%w: invalid run has outcome claims", ErrInvalidEvidence)
		}
		return nil
	}
	if !v.Correctness.validForAdmittedRun() || !v.Safety.validForAdmittedRun() ||
		!v.Liveness.validForAdmittedRun() || !v.Diagnosability.validForAdmittedRun() {
		return fmt.Errorf("%w: admitted run lacks outcome dimensions", ErrInvalidEvidence)
	}
	parity := v.Correctness == OutcomePass && v.Safety == OutcomePass && v.Liveness == OutcomePass && v.Diagnosability == OutcomePass
	if v.EfficiencyEligible != parity {
		return fmt.Errorf("%w: efficiency eligibility does not match outcome parity", ErrInvalidEvidence)
	}
	return nil
}

func FileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
