package lab

import (
	"time"

	"github.com/temporalio-labs/agent-durability-lab/internal/workstore"
)

const (
	defaultRunTimeout  = 45 * time.Second
	storePollInterval  = 25 * time.Millisecond
	agentProtocolBuild = "worker-death-v4"
)

type LaunchGapOptions struct {
	Arm LaunchGapArm
	Options
}

type LaunchGapResult struct {
	RunDirectory  string             `json:"run_directory"`
	WorkflowID    string             `json:"workflow_id"`
	WorkflowRunID string             `json:"workflow_run_id"`
	Outcome       *workstore.Outcome `json:"outcome,omitempty"`
	Verdict       LaunchGapVerdict   `json:"verdict"`
}

type PostExecGapOptions struct {
	Arm PostExecGapArm
	Options
}

type PostExecGapResult struct {
	RunDirectory  string             `json:"run_directory"`
	WorkflowID    string             `json:"workflow_id"`
	WorkflowRunID string             `json:"workflow_run_id"`
	Outcome       *workstore.Outcome `json:"outcome,omitempty"`
	Verdict       PostExecGapVerdict `json:"verdict"`
}

type PostExecGapManifest struct {
	SchemaVersion      int            `json:"schema_version"`
	Experiment         string         `json:"experiment"`
	RunID              string         `json:"run_id"`
	Arm                PostExecGapArm `json:"arm"`
	Mode               workstore.Mode `json:"mode"`
	StartedAt          time.Time      `json:"started_at"`
	CompletedAt        time.Time      `json:"completed_at"`
	TemporalCLI        string         `json:"temporal_cli"`
	TemporalServer     string         `json:"temporal_server"`
	TemporalAPI        string         `json:"temporal_api"`
	TemporalSDK        string         `json:"temporal_sdk"`
	GoVersion          string         `json:"go_version"`
	AgentProtocolBuild string         `json:"agent_protocol_build"`
	FailureBoundary    string         `json:"failure_boundary"`
	Invariant          string         `json:"invariant"`
	Falsifier          string         `json:"falsifier"`
	CredentialBoundary string         `json:"credential_boundary"`
	CleanupBoundary    string         `json:"cleanup_boundary"`
}

type PostExecProcessObservation struct {
	PID              int       `json:"pid"`
	ProcessStart     string    `json:"process_start"`
	ActorID          string    `json:"actor_id"`
	OwnerTokenHash   string    `json:"owner_token_hash"`
	BarrierArrivedAt time.Time `json:"barrier_arrived_at"`
}

type PostExecBoundaryEvidence struct {
	CapturedAt time.Time                  `json:"captured_at"`
	WorkerPID  int                        `json:"worker_pid"`
	Child      PostExecProcessObservation `json:"child"`
	Store      workstore.Snapshot         `json:"store"`
}

type LaunchGapManifest struct {
	SchemaVersion      int            `json:"schema_version"`
	Experiment         string         `json:"experiment"`
	RunID              string         `json:"run_id"`
	Arm                LaunchGapArm   `json:"arm"`
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
