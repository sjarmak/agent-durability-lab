package lab

import (
	"time"

	"github.com/temporalio-labs/agent-durability-lab/internal/workstore"
)

const (
	defaultRunTimeout  = 45 * time.Second
	storePollInterval  = 25 * time.Millisecond
	agentProtocolBuild = "worker-death-v1"
)

type Options struct {
	Mode         workstore.Mode
	TemporalPath string
	WorkerBinary string
	AgentBinary  string
	OutputRoot   string
	RunID        string
	Timeout      time.Duration
}

type Result struct {
	RunDirectory  string            `json:"run_directory"`
	WorkflowID    string            `json:"workflow_id"`
	WorkflowRunID string            `json:"workflow_run_id"`
	Outcome       workstore.Outcome `json:"outcome"`
	Verdict       Verdict           `json:"verdict"`
}

type Manifest struct {
	SchemaVersion      int            `json:"schema_version"`
	Experiment         string         `json:"experiment"`
	RunID              string         `json:"run_id"`
	Mode               workstore.Mode `json:"mode"`
	StartedAt          time.Time      `json:"started_at"`
	CompletedAt        time.Time      `json:"completed_at"`
	TemporalCLI        string         `json:"temporal_cli"`
	TemporalSDK        string         `json:"temporal_sdk"`
	GoVersion          string         `json:"go_version"`
	AgentProtocolBuild string         `json:"agent_protocol_build"`
	FailureBoundary    string         `json:"failure_boundary"`
	Invariant          string         `json:"invariant"`
	Falsifier          string         `json:"falsifier"`
}

type failureRecord struct {
	Time  time.Time `json:"time"`
	Error string    `json:"error"`
}
