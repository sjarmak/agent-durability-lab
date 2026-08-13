package lab

import (
	"time"

	"github.com/sjarmak/temporal_projects/internal/failureinject"
)

type RuntimeProvenance struct {
	CapturedAt      time.Time `json:"captured_at"`
	OS              string    `json:"os"`
	Architecture    string    `json:"architecture"`
	GoVersion       string    `json:"go_version"`
	SDKVersion      string    `json:"sdk_version"`
	TemporalVersion string    `json:"temporal_version"`
	TemporalSHA256  string    `json:"temporal_sha256"`
	TemporalBytes   int64     `json:"temporal_bytes"`
	WorkerSHA256    string    `json:"worker_sha256"`
	WorkerBytes     int64     `json:"worker_bytes"`
}

type Options struct {
	Boundary     Boundary
	Mode         Mode
	TemporalPath string
	WorkerBinary string
	CoverageRoot string
	Provenance   RuntimeProvenance
	SourcePins   map[string]string
	OutputRoot   string
	RunID        string
	Timeout      time.Duration
}

type Result struct {
	RunDirectory  string  `json:"run_directory"`
	WorkflowID    string  `json:"workflow_id"`
	WorkflowRunID string  `json:"workflow_run_id"`
	Verdict       Verdict `json:"verdict"`
}

type KillObservation struct {
	WorkerID   string    `json:"worker_id"`
	PID        int       `json:"pid"`
	Signal     string    `json:"signal"`
	ExitStatus string    `json:"exit_status"`
	KilledAt   time.Time `json:"killed_at"`
}

type ActivityAttemptObservation struct {
	ActivityID      string `json:"activity_id"`
	Attempt         int32  `json:"attempt"`
	WorkerIdentity  string `json:"worker_identity"`
	EventID         int64  `json:"event_id"`
	PreviousFailure string `json:"previous_failure,omitempty"`
}

type HistoryObservation struct {
	Attempts                               []ActivityAttemptObservation `json:"attempts"`
	CompletedActivityIDs                   []string                     `json:"completed_activity_ids"`
	WorkflowCompleted                      bool                         `json:"workflow_completed"`
	ProducerCompletedBeforeConsumerStarted bool                         `json:"producer_completed_before_consumer_started"`
	ExternalReferencePayloads              int                          `json:"external_reference_payloads"`
	MaximumPayloadDataBytes                int                          `json:"maximum_payload_data_bytes"`
	ArtifactBytesInline                    bool                         `json:"artifact_bytes_inline"`
}

type Evidence struct {
	Boundary           Boundary               `json:"boundary"`
	Mode               Mode                   `json:"mode"`
	Barrier            failureinject.Arrival  `json:"barrier"`
	Kill               KillObservation        `json:"kill"`
	PreFaultStore      StoreSnapshot          `json:"pre_fault_store"`
	PreFaultExternal   StoreSnapshot          `json:"pre_fault_external_store"`
	BeforeReconcile    StoreSnapshot          `json:"before_reconcile"`
	FinalStore         StoreSnapshot          `json:"final_store"`
	FinalExternalStore StoreSnapshot          `json:"final_external_store"`
	Reconciliation     ReconcileReport        `json:"reconciliation"`
	History            HistoryObservation     `json:"history"`
	WorkflowResult     WorkflowResult         `json:"workflow_result,omitempty"`
	ExternalResult     ExternalWorkflowResult `json:"external_result,omitempty"`
}

type Verdict struct {
	RunValid            bool     `json:"run_valid"`
	ExpectedObservation bool     `json:"expected_observation"`
	InvariantSatisfied  bool     `json:"invariant_satisfied"`
	Failures            []string `json:"failures,omitempty"`
}

type Manifest struct {
	SchemaVersion   int               `json:"schema_version"`
	Experiment      string            `json:"experiment"`
	RunID           string            `json:"run_id"`
	Boundary        Boundary          `json:"boundary"`
	Mode            Mode              `json:"mode"`
	WorkflowID      string            `json:"workflow_id"`
	WorkflowRunID   string            `json:"workflow_run_id"`
	ArtifactID      string            `json:"artifact_id"`
	ArtifactSHA256  string            `json:"artifact_sha256"`
	ArtifactSize    int64             `json:"artifact_size"`
	StartedAt       time.Time         `json:"started_at"`
	CompletedAt     time.Time         `json:"completed_at"`
	FailureBoundary string            `json:"failure_boundary"`
	Invariant       string            `json:"invariant"`
	Falsifier       string            `json:"falsifier"`
	Runtime         RuntimeProvenance `json:"runtime"`
	SourcePins      map[string]string `json:"source_pins"`
	Directories     []string          `json:"directories"`
	Files           map[string]string `json:"files"`
}
