package lab

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/sjarmak/temporal_projects/internal/agentprocess"
	"github.com/sjarmak/temporal_projects/internal/failureinject"
	"github.com/sjarmak/temporal_projects/internal/workstore"
	"go.temporal.io/sdk/activity"
)

const threadReceiptFile = "thread-receipt.json"

type CodexActivityInput struct {
	LogicalSessionID string       `json:"logical_session_id"`
	LogicalTurnID    string       `json:"logical_turn_id"`
	LogicalEffectID  string       `json:"logical_effect_id"`
	RecoveryMode     RecoveryMode `json:"recovery_mode,omitempty"`
}

type CodexActivityResult struct {
	TemporalAttempt   int32  `json:"temporal_attempt"`
	PhysicalAttemptID string `json:"physical_attempt_id"`
	ThreadID          string `json:"thread_id"`
	Result            string `json:"result"`
	ProcessIdentity   string `json:"process_identity"`
}

type InvocationFunc func(context.Context, Invocation, RunInvocationInput, StreamHooks) (InvocationResult, error)

type Activities struct {
	Command           CodexCommand
	LauncherBinary    string
	FaultBoundary     FaultBoundary
	EffectBinary      string
	DestinationPath   string
	WorkspacePath     string
	EffectPayload     string
	BarrierURL        string
	BarrierCredential failureinject.Credential
	BarrierDirectory  string
	BarrierPoint      string
	RunRoot           string
	WorkerID          string
	SupervisorURL     string
	Hermetic          bool
	Invoke            InvocationFunc
}

func (a Activities) RunCodex(ctx context.Context, input CodexActivityInput) (CodexActivityResult, error) {
	if err := a.validate(input); err != nil {
		return CodexActivityResult{}, err
	}
	stopHeartbeats := startCodexActivityHeartbeats(ctx)
	defer stopHeartbeats()
	info := activity.GetInfo(ctx)
	physicalAttemptID := temporalAttemptID(
		info.WorkflowExecution.ID, info.WorkflowExecution.RunID, info.ActivityID, info.Attempt,
	)
	actorID := fmt.Sprintf("%s-attempt-%d", a.WorkerID, info.Attempt)
	if input.RecoveryMode.normalized() == RecoveryModeFenced {
		return a.runFencedCodex(ctx, input, info.Attempt)
	}
	result, err := a.executeAttempt(ctx, input, physicalAttemptID, actorID, info.Attempt)
	if err != nil {
		return CodexActivityResult{}, fmt.Errorf("run Codex delivery %d: %w", info.Attempt, err)
	}
	return CodexActivityResult{
		TemporalAttempt: info.Attempt, PhysicalAttemptID: physicalAttemptID,
		ThreadID: result.Codex.ThreadID, Result: result.Codex.Result, ProcessIdentity: result.Process.Identity,
	}, nil
}

func (a Activities) executeAttempt(ctx context.Context, input CodexActivityInput,
	physicalAttemptID, actorID string, temporalAttempt int32,
) (InvocationResult, error) {
	attemptDirectory := filepath.Join(a.RunRoot, physicalAttemptID)
	threadReceiptPath := filepath.Join(attemptDirectory, threadReceiptFile)
	canonicalPath := ""
	var selectedThread string
	if input.RecoveryMode.usesCanonicalThread() {
		canonicalPath = canonicalThreadPath(a.RunRoot, input.LogicalSessionID)
		if err := os.MkdirAll(filepath.Dir(canonicalPath), 0o750); err != nil {
			return InvocationResult{}, fmt.Errorf("create canonical Codex thread directory: %w", err)
		}
		canonical, err := ReadCanonicalThread(canonicalPath)
		if err == nil {
			if canonical.LogicalSessionID != input.LogicalSessionID || canonical.LogicalTurnID != input.LogicalTurnID {
				return InvocationResult{}, errors.New("canonical Codex thread does not match the logical turn")
			}
			selectedThread = canonical.ThreadID
		} else if !errors.Is(err, os.ErrNotExist) {
			return InvocationResult{}, err
		}
	}
	effectBarrierURL := a.BarrierURL
	if a.BarrierDirectory != "" {
		effectBarrierURL = ""
	}
	prepared, err := PrepareAttempt(ctx, AttemptInput{
		Directory: attemptDirectory, EffectBinary: a.EffectBinary,
		DestinationPath: a.DestinationPath, WorkspacePath: a.WorkspacePath,
		ThreadReceiptPath: threadReceiptPath, CanonicalThreadPath: canonicalPath,
		Payload: a.EffectPayload, BarrierURL: effectBarrierURL, BarrierDirectory: a.BarrierDirectory,
		BarrierPoint:     a.BarrierPoint,
		LogicalSessionID: input.LogicalSessionID, LogicalTurnID: input.LogicalTurnID,
		LogicalEffectID: input.LogicalEffectID, PhysicalAttemptID: physicalAttemptID, ActorID: actorID,
	})
	if err != nil {
		return InvocationResult{}, fmt.Errorf("prepare Codex attempt: %w", err)
	}
	var invocation Invocation
	if selectedThread == "" {
		invocation, err = a.Command.InitialInvocation(prepared.Prompt)
	} else {
		invocation, err = a.Command.ResumeInvocation(prepared.Prompt, selectedThread)
	}
	if err != nil {
		return InvocationResult{}, err
	}
	if a.FaultBoundary == FaultBeforeThreadObservation {
		invocation = a.preThreadInvocation(invocation, input.LogicalSessionID, physicalAttemptID, actorID, 1)
	}
	if a.Hermetic {
		hermeticThread := selectedThread
		if hermeticThread == "" {
			hermeticThread = deterministicThreadID(physicalAttemptID)
		}
		invocation.Env = append(invocation.Env, "CODEX_HERMETIC_THREAD_ID="+hermeticThread)
	}
	invoke := a.Invoke
	if invoke == nil {
		invoke = RunInvocation
	}
	hooks := StreamHooks{ExpectedCommand: prepared.Command, ThreadStarted: func(threadID string) error {
		if !input.RecoveryMode.usesCanonicalThread() {
			return nil
		}
		if a.FaultBoundary == FaultAfterThreadBeforeRegistration {
			if err := arriveAtThreadRegistrationBoundary(ctx, a.BarrierURL, a.BarrierCredential, input.LogicalSessionID,
				physicalAttemptID, actorID, threadReceiptPath, 0, ""); err != nil {
				return err
			}
		}
		return RegisterCanonicalThread(canonicalPath, CanonicalThread{
			LogicalSessionID: input.LogicalSessionID, LogicalTurnID: input.LogicalTurnID,
			ThreadID: threadID, FirstPhysicalAttemptID: physicalAttemptID, RegisteredAt: time.Now().UTC(),
		})
	}}
	result, err := invoke(ctx, invocation, RunInvocationInput{
		Directory: attemptDirectory, AttemptID: physicalAttemptID, ActorID: actorID,
		ThreadReceiptPath: threadReceiptPath, RegistrationGate: a.Hermetic,
		ProcessStartGate: a.FaultBoundary == FaultBeforeThreadObservation,
	}, hooks)
	if err != nil {
		return InvocationResult{}, err
	}
	if selectedThread != "" && result.Codex.ThreadID != selectedThread {
		return InvocationResult{}, fmt.Errorf("resumed Codex thread %q observed as %q", selectedThread, result.Codex.ThreadID)
	}
	if input.RecoveryMode.usesCanonicalThread() {
		canonical, readErr := ReadCanonicalThread(canonicalPath)
		if readErr != nil || canonical.ThreadID != result.Codex.ThreadID {
			return InvocationResult{}, errors.Join(errors.New("completed Codex thread is not canonical"), readErr)
		}
	}
	if a.FaultBoundary == FaultAfterFinalOutput {
		if err := arriveAtWorkerBoundary(ctx, a.BarrierURL, a.BarrierCredential, finalOutputBarrier,
			input.LogicalSessionID, physicalAttemptID+"-final", actorID, 1); err != nil {
			return InvocationResult{}, err
		}
	}
	return result, nil
}

func (a Activities) preThreadInvocation(invocation Invocation, logicalSessionID, physicalAttemptID,
	actorID string, generation uint64,
) Invocation {
	realBinary := invocation.Binary
	invocation.Binary = a.LauncherBinary
	invocation.Env = append(append([]string(nil), invocation.Env...),
		"CODEX_DIRECT_REAL_BINARY="+realBinary,
		"CODEX_DIRECT_PRE_THREAD_BARRIER_URL="+a.BarrierURL,
		"CODEX_DIRECT_PRE_THREAD_BARRIER_POINT="+preThreadBarrier,
		"CODEX_DIRECT_PHYSICAL_ATTEMPT_ID="+physicalAttemptID,
		"CODEX_DIRECT_LOGICAL_SESSION_ID="+logicalSessionID,
		"CODEX_DIRECT_ACTOR_ID="+actorID,
		fmt.Sprintf("CODEX_DIRECT_GENERATION=%d", generation),
	)
	invocation.BarrierCredential = a.BarrierCredential
	return invocation
}

func (a Activities) runFencedCodex(ctx context.Context, input CodexActivityInput,
	temporalAttempt int32,
) (CodexActivityResult, error) {
	client := newSupervisorClient(a.SupervisorURL, nil)
	receipt, err := client.StartOrAttach(ctx, supervisorStartRequest{
		SessionID: input.LogicalSessionID, WorkerID: a.WorkerID, AgentBuild: "codex-direct-fenced-v1",
		Attempt: temporalAttempt, LogicalTurnID: input.LogicalTurnID, LogicalEffectID: input.LogicalEffectID,
	})
	if err != nil {
		if ctx.Err() != nil {
			cancelContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			_, cancelErr := client.Cancel(cancelContext, supervisorCancelRequest{
				SessionID: input.LogicalSessionID, RequestID: cancellationRequestID(input),
			})
			cancel()
			if cancelErr != nil && !errors.Is(cancelErr, workstore.ErrSessionNotFound) {
				return CodexActivityResult{}, errors.Join(err, fmt.Errorf("durably revoke Codex turn: %w", cancelErr))
			}
		}
		return CodexActivityResult{}, err
	}
	return CodexActivityResult{
		TemporalAttempt: temporalAttempt, PhysicalAttemptID: receipt.PhysicalAttemptID,
		ThreadID: receipt.ThreadID, Result: receipt.Outcome.Value, ProcessIdentity: receipt.ProcessIdentity,
	}, nil
}

func cancellationRequestID(input CodexActivityInput) string {
	digest := sha256.Sum256([]byte(input.LogicalSessionID + "\x00" + input.LogicalTurnID))
	return "temporal-cancel-" + hex.EncodeToString(digest[:8])
}

func (a Activities) validate(input CodexActivityInput) error {
	if a.Command.Binary == "" || a.Command.WorkDir == "" || a.Command.CodexHome == "" ||
		a.Command.Model == "" || a.Command.ReasoningEffort == "" || a.Command.OutputSchema == "" ||
		a.Command.Sandbox == "" || !a.FaultBoundary.valid() || a.EffectBinary == "" ||
		a.DestinationPath == "" || a.WorkspacePath == "" || a.EffectPayload == "" ||
		a.BarrierURL == "" || (a.BarrierDirectory != "" && !safeCommandPath(a.BarrierDirectory)) || a.BarrierPoint == "" ||
		a.RunRoot == "" || a.WorkerID == "" ||
		input.LogicalSessionID == "" || input.LogicalTurnID == "" || input.LogicalEffectID == "" ||
		!input.RecoveryMode.valid() {
		return errors.New("activity requires complete Codex, effect, Worker, and logical identities")
	}
	if input.RecoveryMode.normalized() == RecoveryModeFenced && a.SupervisorURL == "" {
		return errors.New("fenced Codex Activity requires a supervisor URL")
	}
	if (a.FaultBoundary == FaultBeforeThreadObservation || a.FaultBoundary == FaultProcessFailureReplacement) &&
		a.LauncherBinary == "" {
		return errors.New("pre-thread fault requires a launcher binary")
	}
	return nil
}

func canonicalThreadPath(runRoot, logicalSessionID string) string {
	digest := sha256.Sum256([]byte(logicalSessionID))
	return filepath.Join(runRoot, "threads", hex.EncodeToString(digest[:16])+".json")
}

func temporalAttemptID(workflowID, runID, activityID string, attempt int32) string {
	digest := sha256.Sum256([]byte(workflowID + "\x00" + runID + "\x00" + activityID))
	return fmt.Sprintf("%s-attempt-%d", hex.EncodeToString(digest[:8]), attempt)
}

func deterministicThreadID(identity string) string {
	value := sha256.Sum256([]byte(identity))
	value[6] = value[6]&0x0f | 0x40
	value[8] = value[8]&0x3f | 0x80
	encoded := hex.EncodeToString(value[:16])
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:]
}

func arriveAtThreadRegistrationBoundary(ctx context.Context, barrierURL string, credential failureinject.Credential, logicalSessionID,
	physicalAttemptID, actorID, receiptPath string, generation uint64, ownerTokenHash string,
) error {
	receipt, err := ReadThreadReceipt(receiptPath)
	if err != nil {
		return err
	}
	return newBarrierClient(barrierURL, credential).Arrive(ctx, failureinject.Arrival{
		ID: physicalAttemptID + "-thread-registration", Point: threadRegistrationBarrier,
		SessionID: logicalSessionID, Generation: generation, OwnerTokenHash: ownerTokenHash,
		ActorID: actorID, PID: receipt.PID, ProcessStart: receipt.ProcessStart,
	})
}

func arriveAtWorkerBoundary(ctx context.Context, barrierURL string, credential failureinject.Credential,
	point, logicalSessionID, arrivalID, actorID string, generation uint64,
) error {
	pid := os.Getpid()
	start, err := agentprocess.ProcessStartIdentity(pid)
	if err != nil {
		return err
	}
	return newBarrierClient(barrierURL, credential).Arrive(ctx, failureinject.Arrival{
		ID: arrivalID, Point: point, SessionID: logicalSessionID,
		Generation: generation, ActorID: actorID, PID: pid, ProcessStart: start,
	})
}

func startCodexActivityHeartbeats(ctx context.Context) func() {
	done := make(chan struct{})
	stopped := make(chan struct{})
	activity.RecordHeartbeat(ctx, "codex-activity-running")
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				activity.RecordHeartbeat(ctx, "codex-activity-running")
			}
		}
	}()
	return func() {
		close(done)
		<-stopped
	}
}
