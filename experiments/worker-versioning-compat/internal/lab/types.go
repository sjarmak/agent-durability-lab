package lab

import "time"

const (
	WorkflowName   = "AgentSessionVersioningWorkflow"
	ActivityName   = "AgentSessionAttachActivity"
	continueSignal = "continue"
)

type AttachAction string

const (
	ActionStarted  AttachAction = "started"
	ActionAttached AttachAction = "attached"
)

type Attachment struct {
	WorkerBuild string    `json:"worker_build"`
	AttachedAt  time.Time `json:"attached_at"`
}

type AgentRecord struct {
	SessionID       string       `json:"session_id"`
	AgentBuild      string       `json:"agent_build"`
	StartedByWorker string       `json:"started_by_worker"`
	StartedAt       time.Time    `json:"started_at"`
	Attachments     []Attachment `json:"attachments"`
}

type AttachRequest struct {
	SessionID             string
	AgentBuild            string
	WorkerBuild           string
	CompatibleAgentBuilds []string
}

type AttachReceipt struct {
	SessionID   string       `json:"session_id"`
	WorkerBuild string       `json:"worker_build"`
	AgentBuild  string       `json:"agent_build"`
	Action      AttachAction `json:"action"`
	ObservedAt  time.Time    `json:"observed_at"`
}

type WorkflowInput struct {
	SessionID    string
	RegistryPath string
	Phases       int
}

type ActivityInput struct {
	SessionID    string
	RegistryPath string
	Phase        int
}

type ActivityReceipt struct {
	SessionID       string       `json:"session_id"`
	WorkerBuild     string       `json:"worker_build"`
	AgentBuild      string       `json:"agent_build"`
	Action          AttachAction `json:"action"`
	TemporalAttempt int32        `json:"temporal_attempt"`
	WorkflowID      string       `json:"workflow_id"`
	RunID           string       `json:"run_id"`
	Phase           int          `json:"phase"`
}

type WorkflowResult struct {
	WorkflowBuilds []string          `json:"workflow_builds"`
	Receipts       []ActivityReceipt `json:"receipts"`
}
