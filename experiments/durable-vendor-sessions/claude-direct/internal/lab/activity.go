package lab

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"go.temporal.io/sdk/activity"
)

type ClaudeActivityInput struct {
	LogicalSessionID string `json:"logical_session_id"`
	LogicalTurnID    string `json:"logical_turn_id"`
	LogicalEffectID  string `json:"logical_effect_id"`
}

type ClaudeActivityResult struct {
	TemporalAttempt   int32  `json:"temporal_attempt"`
	PhysicalAttemptID string `json:"physical_attempt_id"`
	VendorSessionID   string `json:"vendor_session_id"`
	Result            string `json:"result"`
	ProcessIdentity   string `json:"process_identity"`
}

type InvocationFunc func(context.Context, Invocation, RunInvocationInput) (InvocationResult, error)

type Activities struct {
	Command         ClaudeCommand
	LauncherBinary  string
	FaultBoundary   FaultBoundary
	EffectBinary    string
	DestinationPath string
	WorkspacePath   string
	EffectPayload   string
	BarrierURL      string
	BarrierPoint    string
	RunRoot         string
	WorkerID        string
	Invoke          InvocationFunc
}

func (a Activities) RunClaude(ctx context.Context, input ClaudeActivityInput) (ClaudeActivityResult, error) {
	if err := a.validate(input); err != nil {
		return ClaudeActivityResult{}, err
	}
	info := activity.GetInfo(ctx)
	physicalAttemptID := temporalAttemptID(
		info.WorkflowExecution.ID, info.WorkflowExecution.RunID, info.ActivityID, info.Attempt,
	)
	actorID := fmt.Sprintf("%s-attempt-%d", a.WorkerID, info.Attempt)
	result, err := a.executeAttempt(ctx, input, physicalAttemptID, actorID)
	if err != nil {
		return ClaudeActivityResult{}, fmt.Errorf("run direct Claude attempt %d: %w", info.Attempt, err)
	}
	return ClaudeActivityResult{
		TemporalAttempt: info.Attempt, PhysicalAttemptID: physicalAttemptID,
		VendorSessionID: result.Claude.SessionID, Result: result.Claude.Result,
		ProcessIdentity: result.Process.Identity,
	}, nil
}

func (a Activities) executeAttempt(ctx context.Context, input ClaudeActivityInput,
	physicalAttemptID, actorID string,
) (InvocationResult, error) {
	attemptDirectory := filepath.Join(a.RunRoot, physicalAttemptID)
	prepared, err := PrepareAttempt(ctx, AttemptInput{
		Directory: attemptDirectory, EffectBinary: a.EffectBinary,
		DestinationPath: a.DestinationPath, WorkspacePath: a.WorkspacePath, Payload: a.EffectPayload,
		BarrierURL: a.BarrierURL, BarrierPoint: a.BarrierPoint,
		LogicalSessionID: input.LogicalSessionID, LogicalTurnID: input.LogicalTurnID,
		LogicalEffectID: input.LogicalEffectID, PhysicalAttemptID: physicalAttemptID, ActorID: actorID,
	})
	if err != nil {
		return InvocationResult{}, fmt.Errorf("prepare Claude attempt: %w", err)
	}
	command := a.Command
	command.AllowedTool = prepared.AllowedTool
	invocation, err := command.Invocation(prepared.Prompt)
	if err != nil {
		return InvocationResult{}, err
	}
	if a.FaultBoundary == FaultBeforeVendorRegistration {
		invocation = a.preRegistrationInvocation(invocation, input.LogicalSessionID, physicalAttemptID, actorID)
	}
	invoke := a.Invoke
	if invoke == nil {
		invoke = RunInvocation
	}
	stopHeartbeats := startActivityHeartbeats(ctx)
	defer stopHeartbeats()
	result, invokeErr := invoke(ctx, invocation, RunInvocationInput{
		Directory: attemptDirectory, AttemptID: physicalAttemptID, ActorID: actorID,
	})
	if invokeErr != nil {
		return InvocationResult{}, invokeErr
	}
	if a.FaultBoundary == FaultAfterFinalOutput {
		if err := waitAtFinalOutputBarrier(
			ctx, a.BarrierURL, finalOutputBarrier, input.LogicalSessionID, physicalAttemptID, actorID,
		); err != nil {
			return InvocationResult{}, err
		}
	}
	return result, nil
}

func (a Activities) preRegistrationInvocation(invocation Invocation, logicalSessionID,
	physicalAttemptID, actorID string,
) Invocation {
	invocation.Binary = a.LauncherBinary
	environment := append([]string(nil), invocation.Env...)
	invocation.Env = append(environment,
		"CLAUDE_DIRECT_REAL_BINARY="+a.Command.Binary,
		"CLAUDE_DIRECT_PRE_SESSION_BARRIER_URL="+a.BarrierURL,
		"CLAUDE_DIRECT_PRE_SESSION_BARRIER_POINT="+preRegistrationBarrier,
		"CLAUDE_DIRECT_PHYSICAL_ATTEMPT_ID="+physicalAttemptID,
		"CLAUDE_DIRECT_LOGICAL_SESSION_ID="+logicalSessionID,
		"CLAUDE_DIRECT_ACTOR_ID="+actorID,
	)
	return invocation
}

func (a Activities) validate(input ClaudeActivityInput) error {
	if a.Command.Binary == "" || a.LauncherBinary == "" || a.Command.WorkDir == "" || a.Command.Model == "" ||
		a.Command.MaxBudgetUSD == "" || a.Command.MaxTurns < 1 || a.EffectBinary == "" ||
		a.DestinationPath == "" || a.WorkspacePath == "" || a.EffectPayload == "" ||
		a.BarrierURL == "" || a.BarrierPoint == "" || !a.FaultBoundary.valid() ||
		a.RunRoot == "" || a.WorkerID == "" || input.LogicalSessionID == "" ||
		input.LogicalTurnID == "" || input.LogicalEffectID == "" {
		return errors.New("activity requires complete Claude command, effect, Worker, and logical identities")
	}
	return nil
}

func temporalAttemptID(workflowID, runID, activityID string, attempt int32) string {
	sum := sha256.Sum256([]byte(workflowID + "\x00" + runID + "\x00" + activityID))
	return fmt.Sprintf("%s-attempt-%d", hex.EncodeToString(sum[:8]), attempt)
}

func startActivityHeartbeats(ctx context.Context) func() {
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		activity.RecordHeartbeat(ctx, "claude-process-running")
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				activity.RecordHeartbeat(ctx, "claude-process-running")
			}
		}
	}()
	return func() {
		close(done)
		<-stopped
	}
}
