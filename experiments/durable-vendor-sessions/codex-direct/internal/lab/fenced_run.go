package lab

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/sjarmak/temporal_projects/internal/agentprocess"
	"github.com/sjarmak/temporal_projects/internal/failureinject"
	"github.com/sjarmak/temporal_projects/internal/workstore"
)

type fencedCodexRunConfig struct {
	Command            CodexCommand
	LauncherBinary     string
	FaultBoundary      FaultBoundary
	EffectBinary       string
	EffectPayload      string
	WorkspacePath      string
	AuthorityStorePath string
	BarrierURL         string
	BarrierCredential  failureinject.Credential
	BarrierDirectory   string
	BarrierPoint       string
	RunRoot            string
	LogicalSessionID   string
	LogicalTurnID      string
	LogicalEffectID    string
	Hermetic           bool
	SupervisorURL      func() string
}

func (c fencedCodexRunConfig) validate() error {
	if c.Command.Binary == "" || c.Command.WorkDir == "" || c.Command.CodexHome == "" ||
		c.Command.Model == "" || c.Command.ReasoningEffort == "" || c.Command.OutputSchema == "" ||
		c.Command.Sandbox == "" || !c.FaultBoundary.valid() || c.EffectBinary == "" ||
		c.EffectPayload == "" || c.WorkspacePath == "" || !safeCommandPath(c.AuthorityStorePath) || c.BarrierURL == "" ||
		(c.BarrierDirectory != "" && !safeCommandPath(c.BarrierDirectory)) || c.BarrierPoint == "" ||
		c.RunRoot == "" || c.LogicalSessionID == "" || c.LogicalTurnID == "" ||
		c.LogicalEffectID == "" || c.SupervisorURL == nil || c.SupervisorURL() == "" {
		return errors.New("fenced Codex runner requires command, effect, barrier, supervisor, and logical identities")
	}
	if (c.FaultBoundary == FaultBeforeThreadObservation || c.FaultBoundary == FaultProcessFailureReplacement) &&
		c.LauncherBinary == "" {
		return errors.New("fenced pre-thread fault requires launcher binary")
	}
	return nil
}

func (c fencedCodexRunConfig) validateStart(request supervisorStartRequest) error {
	if request.SessionID != c.LogicalSessionID || request.LogicalTurnID != c.LogicalTurnID ||
		request.LogicalEffectID != c.LogicalEffectID {
		return errors.New("start request does not match the supervisor-owned Codex turn")
	}
	return nil
}

func (c fencedCodexRunConfig) run(ctx context.Context, store *workstore.Store,
	lease workstore.Lease,
) (supervisedResult, error) {
	if err := c.validate(); err != nil {
		return supervisedResult{}, err
	}
	if lease.SessionID != c.LogicalSessionID || lease.Generation == 0 || lease.Generation > math.MaxInt32 {
		return supervisedResult{}, fmt.Errorf("%w: lease does not match fenced Codex turn", workstore.ErrInvalidRequest)
	}
	physicalAttemptID := fmt.Sprintf("supervisor-generation-%d", lease.Generation)
	actorID := fmt.Sprintf("codex-supervisor-g%d", lease.Generation)
	if c.FaultBoundary == FaultAfterClaimBeforeExec {
		pid := os.Getpid()
		processStart, err := agentprocess.ProcessStartIdentity(pid)
		if err != nil {
			return supervisedResult{}, err
		}
		if err := newBarrierClient(c.BarrierURL, c.BarrierCredential).Arrive(ctx, failureinject.Arrival{
			ID: physicalAttemptID + "-claim", Point: claimBeforeExecBarrier, SessionID: lease.SessionID,
			Generation: lease.Generation, OwnerTokenHash: workstore.HashToken(lease.OwnerToken),
			ActorID: actorID, PID: pid, ProcessStart: processStart,
		}); err != nil {
			return supervisedResult{}, fmt.Errorf("wait after Codex claim before exec: %w", err)
		}
	}
	attemptDirectory := filepath.Join(c.RunRoot, physicalAttemptID)
	threadReceiptPath := filepath.Join(attemptDirectory, threadReceiptFile)
	canonicalPath := canonicalThreadPath(c.RunRoot, c.LogicalSessionID)
	if err := os.MkdirAll(filepath.Dir(canonicalPath), 0o750); err != nil {
		return supervisedResult{}, err
	}
	var selectedThread string
	canonical, err := ReadCanonicalThread(canonicalPath)
	if err == nil {
		if canonical.LogicalSessionID != c.LogicalSessionID || canonical.LogicalTurnID != c.LogicalTurnID {
			return supervisedResult{}, errors.New("fenced canonical Codex thread does not match logical turn")
		}
		selectedThread = canonical.ThreadID
	} else if !errors.Is(err, os.ErrNotExist) {
		return supervisedResult{}, err
	}
	effectBarrierURL := c.BarrierURL
	if c.BarrierDirectory != "" {
		effectBarrierURL = ""
	}
	prepared, err := PrepareAttempt(ctx, AttemptInput{
		Directory: attemptDirectory, EffectBinary: c.EffectBinary, DestinationPath: "",
		WorkspacePath: c.WorkspacePath, ThreadReceiptPath: threadReceiptPath,
		CanonicalThreadPath: canonicalPath, AuthorityStorePath: c.AuthorityStorePath,
		EnforceThreadAuthority: true,
		Generation:             lease.Generation, OwnerCapability: lease.OwnerToken, Payload: c.EffectPayload,
		BarrierURL: effectBarrierURL, BarrierDirectory: c.BarrierDirectory, BarrierPoint: c.BarrierPoint,
		LogicalSessionID: c.LogicalSessionID, LogicalTurnID: c.LogicalTurnID, LogicalEffectID: c.LogicalEffectID,
		PhysicalAttemptID: physicalAttemptID, ActorID: actorID,
	})
	if err != nil {
		return supervisedResult{}, err
	}
	var invocation Invocation
	if selectedThread == "" {
		invocation, err = c.Command.InitialInvocation(prepared.Prompt)
	} else {
		invocation, err = c.Command.ResumeInvocation(prepared.Prompt, selectedThread)
	}
	if err != nil {
		return supervisedResult{}, err
	}
	if c.FaultBoundary == FaultBeforeThreadObservation || c.FaultBoundary == FaultProcessFailureReplacement {
		invocation = (Activities{LauncherBinary: c.LauncherBinary, BarrierURL: c.BarrierURL,
			BarrierCredential: c.BarrierCredential}).
			preThreadInvocation(invocation, c.LogicalSessionID, physicalAttemptID, actorID, lease.Generation)
	}
	if c.Hermetic {
		hermeticThread := selectedThread
		if hermeticThread == "" {
			hermeticThread = deterministicThreadID(c.LogicalSessionID)
		}
		invocation.Env = append(invocation.Env, "CODEX_HERMETIC_THREAD_ID="+hermeticThread)
	}
	result, err := RunSupervisedInvocation(ctx, store, lease, invocation, RunInvocationInput{
		Directory: attemptDirectory, AttemptID: physicalAttemptID, ActorID: actorID,
		ThreadReceiptPath: threadReceiptPath, RegistrationGate: c.Hermetic,
		ProcessStartGate: c.FaultBoundary == FaultBeforeThreadObservation ||
			c.FaultBoundary == FaultProcessFailureReplacement,
	}, StreamHooks{ExpectedCommand: prepared.Command, ThreadStarted: func(threadID string) error {
		if c.FaultBoundary == FaultAfterThreadBeforeRegistration || c.FaultBoundary == FaultCancellationWhileExecuting {
			if err := arriveAtThreadRegistrationBoundary(ctx, c.BarrierURL, c.BarrierCredential, c.LogicalSessionID,
				physicalAttemptID, actorID, threadReceiptPath, lease.Generation,
				workstore.HashToken(lease.OwnerToken)); err != nil {
				return err
			}
		}
		return RegisterCanonicalThread(canonicalPath, CanonicalThread{
			LogicalSessionID: c.LogicalSessionID, LogicalTurnID: c.LogicalTurnID,
			ThreadID: threadID, FirstPhysicalAttemptID: physicalAttemptID, RegisteredAt: time.Now().UTC(),
		})
	}})
	if err != nil {
		return supervisedResult{}, c.authorityError(ctx, store, lease, err)
	}
	canonical, err = ReadCanonicalThread(canonicalPath)
	if err != nil || canonical.ThreadID != result.Codex.ThreadID ||
		(selectedThread != "" && result.Codex.ThreadID != selectedThread) {
		return supervisedResult{}, errors.Join(errors.New("fenced Codex result is not on the canonical thread"), err)
	}
	if c.FaultBoundary == FaultAfterFinalOutput {
		pid := os.Getpid()
		processStart, identityErr := agentprocess.ProcessStartIdentity(pid)
		if identityErr != nil {
			return supervisedResult{}, identityErr
		}
		if err := newBarrierClient(c.BarrierURL, c.BarrierCredential).Arrive(ctx, failureinject.Arrival{
			ID: physicalAttemptID + "-final", Point: finalOutputBarrier, SessionID: lease.SessionID,
			Generation: lease.Generation, OwnerTokenHash: workstore.HashToken(lease.OwnerToken),
			ActorID: actorID, PID: pid, ProcessStart: processStart,
		}); err != nil {
			return supervisedResult{}, err
		}
	}
	return supervisedResult{
		ThreadID: result.Codex.ThreadID, PhysicalAttemptID: physicalAttemptID,
		ProcessIdentity: result.Process.Identity, Outcome: workstore.Outcome{Value: result.Codex.Result},
	}, nil
}

func (c fencedCodexRunConfig) authorityError(ctx context.Context, store *workstore.Store,
	lease workstore.Lease, runErr error,
) error {
	if ctx.Err() == nil {
		return runErr
	}
	snapshot, err := store.Snapshot(context.Background(), lease.SessionID)
	if err != nil {
		return errors.Join(runErr, err)
	}
	if snapshot.Cancellation != nil {
		return errors.Join(workstore.ErrSessionCanceled, runErr)
	}
	if snapshot.ActiveGeneration != lease.Generation ||
		snapshot.ActiveOwnerTokenHash != workstore.HashToken(lease.OwnerToken) {
		return errors.Join(workstore.ErrStaleOwner, runErr)
	}
	return runErr
}
