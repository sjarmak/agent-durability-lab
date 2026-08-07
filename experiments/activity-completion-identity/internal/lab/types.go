package lab

import "time"

type Arm string

const (
	ArmStaleTaskToken Arm = "stale-task-token"
	ArmStaleByID      Arm = "stale-by-id"
	ArmFencedByID     Arm = "fenced-by-id"
)

func (a Arm) Valid() bool {
	switch a {
	case ArmStaleTaskToken, ArmStaleByID, ArmFencedByID:
		return true
	default:
		return false
	}
}

type CompletionMechanism string

const (
	CompletionTaskToken        CompletionMechanism = "task_token"
	CompletionByID             CompletionMechanism = "activity_id"
	CompletionApplicationFence CompletionMechanism = "application_fence"
)

type AttemptObservation struct {
	Attempt        int32     `json:"attempt"`
	ObservedAt     time.Time `json:"observed_at,omitempty"`
	TaskTokenHash  string    `json:"task_token_hash,omitempty"`
	OwnerTokenHash string    `json:"owner_token_hash,omitempty"`
}

type CompletionObservation struct {
	CallerAttempt int32               `json:"caller_attempt"`
	Mechanism     CompletionMechanism `json:"mechanism"`
	RequestedAt   time.Time           `json:"requested_at,omitempty"`
	RespondedAt   time.Time           `json:"responded_at,omitempty"`
	Accepted      bool                `json:"accepted"`
	ErrorCode     string              `json:"error_code,omitempty"`
	ErrorType     string              `json:"error_type,omitempty"`
	Error         string              `json:"error,omitempty"`
	Result        string              `json:"result,omitempty"`
}

type HistoryObservation struct {
	StartedAttempts []int32                   `json:"started_attempts"`
	RetryFailures   []HistoryRetryObservation `json:"retry_failures"`
	CompletedCount  int                       `json:"completed_count"`
}

type HistoryRetryObservation struct {
	StartedAttempt int32  `json:"started_attempt"`
	TimeoutType    string `json:"timeout_type"`
}

type Evidence struct {
	Arm             Arm                     `json:"arm"`
	Attempts        []AttemptObservation    `json:"attempts"`
	Completions     []CompletionObservation `json:"completions"`
	History         HistoryObservation      `json:"history"`
	WorkflowOutcome string                  `json:"workflow_outcome"`
}

type Verdict struct {
	RunValid            bool     `json:"run_valid"`
	ExpectedObservation bool     `json:"expected_observation"`
	InvariantSatisfied  bool     `json:"invariant_satisfied"`
	Failures            []string `json:"failures,omitempty"`
}

type Options struct {
	Arm          Arm
	TemporalPath string
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
	SchemaVersion   int       `json:"schema_version"`
	Experiment      string    `json:"experiment"`
	RunID           string    `json:"run_id"`
	Arm             Arm       `json:"arm"`
	WorkflowID      string    `json:"workflow_id"`
	WorkflowRunID   string    `json:"workflow_run_id"`
	TaskQueue       string    `json:"task_queue"`
	ActivityID      string    `json:"activity_id"`
	StartedAt       time.Time `json:"started_at"`
	CompletedAt     time.Time `json:"completed_at"`
	TemporalCLI     string    `json:"temporal_cli"`
	TemporalServer  string    `json:"temporal_server"`
	TemporalAPI     string    `json:"temporal_api"`
	TemporalSDK     string    `json:"temporal_sdk"`
	GoVersion       string    `json:"go_version"`
	FailureBoundary string    `json:"failure_boundary"`
	Invariant       string    `json:"invariant"`
	Falsifier       string    `json:"falsifier"`
}
