package lab

import (
	"time"

	"github.com/sjarmak/temporal_projects/internal/workstore"
)

type Scenario string

const (
	ScenarioTemporalControl Scenario = "temporal-control"
	ScenarioHealthySafe     Scenario = "healthy-safe"
	ScenarioWorkerDeathSafe Scenario = "worker-death-safe"
	ScenarioFrozenSafe      Scenario = "frozen-safe"
)

func (s Scenario) Valid() bool {
	return s == ScenarioTemporalControl || s == ScenarioHealthySafe ||
		s == ScenarioWorkerDeathSafe || s == ScenarioFrozenSafe
}

func (s Scenario) Safe() bool {
	return s != ScenarioTemporalControl
}

type Options struct {
	Scenario            Scenario
	WaitForCancellation bool
	TemporalPath        string
	WorkerBinary        string
	AgentBinary         string
	OutputRoot          string
	RunID               string
	Timeout             time.Duration
}

type Result struct {
	RunDirectory  string  `json:"run_directory"`
	WorkflowID    string  `json:"workflow_id"`
	WorkflowRunID string  `json:"workflow_run_id"`
	Verdict       Verdict `json:"verdict"`
}

type Verdict struct {
	RunValid            bool               `json:"run_valid"`
	ExpectedObservation bool               `json:"expected_observation"`
	InvariantSatisfied  bool               `json:"invariant_satisfied"`
	History             HistoryObservation `json:"history"`
	Failures            []string           `json:"failures,omitempty"`
}

type HistoryObservation struct {
	WorkflowCancelRequested int `json:"workflow_cancel_requested"`
	WorkflowCanceled        int `json:"workflow_canceled"`
	ActivityCancelRequested int `json:"activity_cancel_requested"`
	ActivityCanceled        int `json:"activity_canceled"`
	ActivityTimedOut        int `json:"activity_timed_out"`
	ActivityScheduled       int `json:"activity_scheduled"`
	ActivityCompleted       int `json:"activity_completed"`
}

type Manifest struct {
	SchemaVersion       int       `json:"schema_version"`
	Experiment          string    `json:"experiment"`
	RunID               string    `json:"run_id"`
	Scenario            Scenario  `json:"scenario"`
	WaitForCancellation bool      `json:"wait_for_cancellation"`
	StartedAt           time.Time `json:"started_at"`
	CompletedAt         time.Time `json:"completed_at"`
	TemporalCLI         string    `json:"temporal_cli"`
	TemporalServer      string    `json:"temporal_server"`
	TemporalSDK         string    `json:"temporal_sdk"`
	GoVersion           string    `json:"go_version"`
	FailureBoundary     string    `json:"failure_boundary"`
	Invariant           string    `json:"invariant"`
	Falsifier           string    `json:"falsifier"`
}

type BoundaryEvidence struct {
	CapturedAt time.Time          `json:"captured_at"`
	WorkerPID  int                `json:"worker_pid"`
	Store      workstore.Snapshot `json:"store"`
}
