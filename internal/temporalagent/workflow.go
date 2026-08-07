package temporalagent

import (
	"errors"
	"fmt"
	"time"

	"github.com/temporalio-labs/agent-durability-lab/internal/workstore"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	WorkflowName = "AgentDurabilityWorkerDeathWorkflow"
	ActivityName = "RunDetachedAgent"
)

const (
	AgentHeartbeatTimeout          = time.Second
	AgentStartToCloseTimeout       = 30 * time.Second
	agentScheduleToClose           = 2 * time.Minute
	agentRetryInitial              = 100 * time.Millisecond
	agentRetryMaximum              = time.Second
	agentRetryAttempts       int32 = 3
)

type WorkflowInput struct {
	SessionID                        string         `json:"session_id"`
	Mode                             workstore.Mode `json:"mode"`
	ReplaceOwnerOnRetry              bool           `json:"replace_on_retry"`
	ReplacePendingLaunchOnRetry      bool           `json:"recover_pending_launch_on_retry"`
	BlockAttempt1AfterLaunchDecision bool           `json:"block_attempt_1_after_launch_decision"`
	BlockAttempt1BeforeHeartbeat     bool           `json:"block_attempt_1_before_heartbeat"`
}

type ActivityInput struct {
	SessionID                        string         `json:"session_id"`
	Mode                             workstore.Mode `json:"mode"`
	ReplaceOwnerOnRetry              bool           `json:"replace_on_retry"`
	ReplacePendingLaunchOnRetry      bool           `json:"recover_pending_launch_on_retry"`
	BlockAttempt1AfterLaunchDecision bool           `json:"block_attempt_1_after_launch_decision"`
	BlockAttempt1BeforeHeartbeat     bool           `json:"block_attempt_1_before_heartbeat"`
}

func ActivityID(sessionID string) string {
	return "agent-session/" + sessionID
}

func WorkerDeathWorkflow(ctx workflow.Context, input WorkflowInput) (workstore.Outcome, error) {
	if input.SessionID == "" || !input.Mode.Valid() {
		return workstore.Outcome{}, errors.New("workflow requires a session ID and valid mode")
	}
	if input.ReplaceOwnerOnRetry && input.Mode != workstore.ModeFenced {
		return workstore.Outcome{}, errors.New("replacement on retry requires fenced mode")
	}
	if input.ReplacePendingLaunchOnRetry && input.Mode != workstore.ModeFenced {
		return workstore.Outcome{}, errors.New("pending launch recovery requires fenced mode")
	}
	if input.ReplaceOwnerOnRetry && input.ReplacePendingLaunchOnRetry {
		return workstore.Outcome{}, errors.New("replacement policies are mutually exclusive")
	}
	options := workflow.ActivityOptions{
		ActivityID:             ActivityID(input.SessionID),
		ScheduleToCloseTimeout: agentScheduleToClose,
		StartToCloseTimeout:    AgentStartToCloseTimeout,
		HeartbeatTimeout:       AgentHeartbeatTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: agentRetryInitial, MaximumInterval: agentRetryMaximum,
			BackoffCoefficient: 2, MaximumAttempts: agentRetryAttempts,
		},
	}
	activityCtx := workflow.WithActivityOptions(ctx, options)
	activityInput := ActivityInput(input)
	var outcome workstore.Outcome
	if err := workflow.ExecuteActivity(activityCtx, ActivityName, activityInput).Get(activityCtx, &outcome); err != nil {
		return workstore.Outcome{}, fmt.Errorf("run detached agent: %w", err)
	}
	return outcome, nil
}
