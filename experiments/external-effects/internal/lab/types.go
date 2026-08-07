package lab

import "time"

type Destination string

const (
	DestinationIdempotentAPI    Destination = "idempotent-api"
	DestinationNonIdempotentAPI Destination = "non-idempotent-api"
	DestinationDatabase         Destination = "database"
	DestinationGit              Destination = "git"
	DestinationMessage          Destination = "message"
	DestinationArtifact         Destination = "artifact"
)

func AllDestinations() []Destination {
	return []Destination{
		DestinationIdempotentAPI,
		DestinationNonIdempotentAPI,
		DestinationDatabase,
		DestinationGit,
		DestinationMessage,
		DestinationArtifact,
	}
}

func (d Destination) Valid() bool {
	switch d {
	case DestinationIdempotentAPI, DestinationNonIdempotentAPI, DestinationDatabase,
		DestinationGit, DestinationMessage, DestinationArtifact:
		return true
	default:
		return false
	}
}

type Mode string

const (
	ModeUnsafe    Mode = "unsafe"
	ModeProtected Mode = "protected"
)

func (m Mode) Valid() bool {
	return m == ModeUnsafe || m == ModeProtected
}

type EffectOutcome string

const (
	OutcomeApplied      EffectOutcome = "applied"
	OutcomeDeduplicated EffectOutcome = "deduplicated"
	OutcomeReconciled   EffectOutcome = "reconciled"
)

func protectedOutcome(destination Destination) EffectOutcome {
	switch destination {
	case DestinationNonIdempotentAPI, DestinationGit:
		return OutcomeReconciled
	case DestinationIdempotentAPI, DestinationDatabase, DestinationMessage, DestinationArtifact:
		return OutcomeDeduplicated
	default:
		return ""
	}
}

type EffectRequest struct {
	EffectID string `json:"effect_id"`
	Payload  string `json:"payload"`
	Mode     Mode   `json:"mode"`
	Attempt  int32  `json:"attempt"`
}

type EffectResult struct {
	Receipt string        `json:"receipt"`
	Outcome EffectOutcome `json:"outcome"`
}

type DestinationConfig struct {
	HTTPURL      string `json:"http_url,omitempty"`
	DatabasePath string `json:"database_path,omitempty"`
	GitPath      string `json:"git_path,omitempty"`
	ArtifactPath string `json:"artifact_path,omitempty"`
}

type PhysicalEffect struct {
	PhysicalID string      `json:"physical_id"`
	LogicalID  string      `json:"logical_id"`
	Receipt    string      `json:"receipt"`
	Payload    string      `json:"payload,omitempty"`
	AppliedAt  time.Time   `json:"applied_at,omitempty"`
	Attempt    int32       `json:"attempt,omitempty"`
	Kind       Destination `json:"kind,omitempty"`
}

type DestinationState struct {
	PhysicalEffects []PhysicalEffect `json:"physical_effects"`
}

type AttemptObservation struct {
	Attempt           int32         `json:"attempt"`
	WorkerID          string        `json:"worker_id"`
	PID               int           `json:"pid"`
	StartedAt         time.Time     `json:"started_at"`
	EffectRequestedAt time.Time     `json:"effect_requested_at"`
	EffectRespondedAt time.Time     `json:"effect_responded_at"`
	Outcome           EffectOutcome `json:"outcome"`
	Receipt           string        `json:"receipt"`
	Error             string        `json:"error,omitempty"`
}

type KillObservation struct {
	BarrierObservedAt time.Time `json:"barrier_observed_at"`
	KilledAt          time.Time `json:"killed_at"`
	WorkerID          string    `json:"worker_id"`
	PID               int       `json:"pid"`
	Signal            string    `json:"signal"`
	ExitStatus        string    `json:"exit_status"`
}

type HistoryObservation struct {
	RetryTimedOut    bool  `json:"retry_timed_out"`
	CompletedCount   int   `json:"completed_count"`
	CompletedAttempt int32 `json:"completed_attempt"`
}

type Evidence struct {
	Destination      Destination          `json:"destination"`
	Mode             Mode                 `json:"mode"`
	EffectID         string               `json:"effect_id"`
	Attempts         []AttemptObservation `json:"attempts"`
	Kill             KillObservation      `json:"kill"`
	DestinationState DestinationState     `json:"destination_state"`
	History          HistoryObservation   `json:"history"`
	WorkflowOutcome  string               `json:"workflow_outcome"`
}

type Verdict struct {
	RunValid            bool     `json:"run_valid"`
	ExpectedObservation bool     `json:"expected_observation"`
	InvariantSatisfied  bool     `json:"invariant_satisfied"`
	Failures            []string `json:"failures,omitempty"`
}

type WorkflowInput struct {
	Destination Destination       `json:"destination"`
	Mode        Mode              `json:"mode"`
	EffectID    string            `json:"effect_id"`
	Payload     string            `json:"payload"`
	Config      DestinationConfig `json:"config"`
	BarrierURL  string            `json:"barrier_url"`
	StorePath   string            `json:"store_path"`
}

type Options struct {
	Destination  Destination
	Mode         Mode
	TemporalPath string
	WorkerBinary string
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

type Manifest struct {
	SchemaVersion   int         `json:"schema_version"`
	Experiment      string      `json:"experiment"`
	RunID           string      `json:"run_id"`
	Destination     Destination `json:"destination"`
	Mode            Mode        `json:"mode"`
	WorkflowID      string      `json:"workflow_id"`
	WorkflowRunID   string      `json:"workflow_run_id"`
	TaskQueue       string      `json:"task_queue"`
	ActivityID      string      `json:"activity_id"`
	EffectID        string      `json:"effect_id"`
	StartedAt       time.Time   `json:"started_at"`
	CompletedAt     time.Time   `json:"completed_at"`
	TemporalCLI     string      `json:"temporal_cli"`
	TemporalServer  string      `json:"temporal_server"`
	TemporalAPI     string      `json:"temporal_api"`
	TemporalSDK     string      `json:"temporal_sdk"`
	GoVersion       string      `json:"go_version"`
	FailureBoundary string      `json:"failure_boundary"`
	Invariant       string      `json:"invariant"`
	Falsifier       string      `json:"falsifier"`
}
