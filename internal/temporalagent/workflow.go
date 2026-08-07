package temporalagent

import (
	"errors"
	"fmt"
	"time"

	"github.com/sjarmak/temporal_projects/internal/workstore"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	WorkflowName       = "AgentDurabilityWorkerDeathWorkflow"
	ActivityName       = "RunDetachedAgent"
	CancelActivityName = "CancelDetachedAgent"
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
	BlockAttempt1BeforeRegistration  bool           `json:"block_attempt_1_before_registration,omitempty"`
	BlockAttempt1BeforeHeartbeat     bool           `json:"block_attempt_1_before_heartbeat"`
	EnableCancellationCleanup        bool           `json:"enable_cancellation_cleanup,omitempty"`
	WaitForCancellation              bool           `json:"wait_for_cancellation,omitempty"`
	SpawnToolChild                   bool           `json:"spawn_tool_child,omitempty"`
}

type ActivityInput struct {
	SessionID                        string         `json:"session_id"`
	Mode                             workstore.Mode `json:"mode"`
	ReplaceOwnerOnRetry              bool           `json:"replace_on_retry"`
	ReplacePendingLaunchOnRetry      bool           `json:"recover_pending_launch_on_retry"`
	BlockAttempt1AfterLaunchDecision bool           `json:"block_attempt_1_after_launch_decision"`
	BlockAttempt1BeforeRegistration  bool           `json:"block_attempt_1_before_registration,omitempty"`
	BlockAttempt1BeforeHeartbeat     bool           `json:"block_attempt_1_before_heartbeat"`
	EnableCancellationCleanup        bool           `json:"enable_cancellation_cleanup,omitempty"`
	WaitForCancellation              bool           `json:"wait_for_cancellation,omitempty"`
	SpawnToolChild                   bool           `json:"spawn_tool_child,omitempty"`
}

type CancelActivityInput struct {
	SessionID string `json:"session_id"`
	RequestID string `json:"request_id"`
}

type CancellationDelivery string

const (
	CancellationDeliverySent        CancellationDelivery = "sent"
	CancellationDeliveryFailed      CancellationDelivery = "failed"
	CancellationDeliveryNotRequired CancellationDelivery = "not_required"
)

type CancelActivityResult struct {
	Action   workstore.CancelAction `json:"action"`
	Delivery CancellationDelivery   `json:"delivery"`
}

func ActivityID(sessionID string) string {
	return "agent-session/" + sessionID
}

func CancelActivityID(sessionID string) string {
	return "cancel-agent-session/" + sessionID
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
	activityCtx := workflow.WithActivityOptions(ctx, agentActivityOptions(input.SessionID, input.WaitForCancellation))
	activityInput := ActivityInput(input)
	var outcome workstore.Outcome
	err := workflow.ExecuteActivity(activityCtx, ActivityName, activityInput).Get(activityCtx, &outcome)
	if err == nil {
		return outcome, nil
	}
	if !input.EnableCancellationCleanup || !temporal.IsCanceledError(err) {
		return workstore.Outcome{}, fmt.Errorf("run detached agent: %w", err)
	}

	cleanupCtx, cleanupCancel := workflow.NewDisconnectedContext(ctx)
	defer cleanupCancel()
	cleanupCtx = workflow.WithActivityOptions(cleanupCtx, workflow.ActivityOptions{
		ActivityID:          CancelActivityID(input.SessionID),
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: agentRetryInitial, MaximumInterval: agentRetryMaximum,
			BackoffCoefficient: 2, MaximumAttempts: agentRetryAttempts,
		},
	})
	var cleanupResult CancelActivityResult
	cleanupInput := CancelActivityInput{
		SessionID: input.SessionID, RequestID: "workflow-cancel/" + input.SessionID,
	}
	if cleanupErr := workflow.ExecuteActivity(cleanupCtx, CancelActivityName, cleanupInput).Get(cleanupCtx, &cleanupResult); cleanupErr != nil {
		return workstore.Outcome{}, fmt.Errorf("cancel detached agent after Workflow cancellation: %w", cleanupErr)
	}
	return workstore.Outcome{}, fmt.Errorf("run detached agent: %w", err)
}

func agentActivityOptions(sessionID string, waitForCancellation bool) workflow.ActivityOptions {
	return workflow.ActivityOptions{
		ActivityID:             ActivityID(sessionID),
		ScheduleToCloseTimeout: agentScheduleToClose,
		StartToCloseTimeout:    AgentStartToCloseTimeout,
		HeartbeatTimeout:       AgentHeartbeatTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: agentRetryInitial, MaximumInterval: agentRetryMaximum,
			BackoffCoefficient: 2, MaximumAttempts: agentRetryAttempts,
		},
		WaitForCancellation: waitForCancellation,
	}
}
