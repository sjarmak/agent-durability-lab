package lab

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"github.com/sjarmak/temporal_projects/internal/agentprocess"
	"github.com/sjarmak/temporal_projects/internal/failureinject"
	"github.com/sjarmak/temporal_projects/internal/workstore"
)

type fencedClaudeRunConfig struct {
	Command                 ClaudeCommand
	LauncherBinary          string
	FaultBoundary           FaultBoundary
	EffectBinary            string
	EffectPayload           string
	WorkspacePath           string
	BarrierURL              string
	BarrierPoint            string
	RunRoot                 string
	LogicalSessionID        string
	LogicalTurnID           string
	LogicalEffectID         string
	SelectedVendorSessionID string
	SupervisorURL           func() string
}

func (c fencedClaudeRunConfig) validate() error {
	if c.Command.Binary == "" || c.Command.WorkDir == "" || c.Command.Model == "" ||
		c.Command.MaxBudgetUSD == "" || c.Command.MaxTurns < 1 || c.LauncherBinary == "" ||
		!c.FaultBoundary.valid() || c.EffectBinary == "" || c.EffectPayload == "" || c.WorkspacePath == "" ||
		c.BarrierURL == "" || c.BarrierPoint == "" || c.RunRoot == "" ||
		c.LogicalSessionID == "" || c.LogicalTurnID == "" || c.LogicalEffectID == "" ||
		!validVendorSessionID(c.SelectedVendorSessionID) || c.SupervisorURL == nil {
		return errors.New("fenced Claude runner requires command, authority, effect, barrier, and logical identities")
	}
	return nil
}

func (c fencedClaudeRunConfig) validateStart(request supervisorStartRequest) error {
	if request.SessionID != c.LogicalSessionID || request.LogicalTurnID != c.LogicalTurnID ||
		request.LogicalEffectID != c.LogicalEffectID ||
		request.SelectedVendorSessionID != c.SelectedVendorSessionID {
		return errors.New("start request does not match the supervisor-owned turn specification")
	}
	return nil
}

func (c fencedClaudeRunConfig) run(ctx context.Context, store *workstore.Store,
	lease workstore.Lease,
) (supervisedResult, error) {
	if err := c.validate(); err != nil {
		return supervisedResult{}, err
	}
	if lease.SessionID != c.LogicalSessionID || lease.Generation > math.MaxInt32 {
		return supervisedResult{}, fmt.Errorf("%w: lease does not match fenced Claude turn", workstore.ErrInvalidRequest)
	}
	physicalAttemptID := fmt.Sprintf("supervisor-generation-%d-attempt-%d", lease.Generation, lease.Generation)
	actorID := fmt.Sprintf("claude-supervisor-g%d", lease.Generation)
	if c.FaultBoundary == FaultAfterClaimBeforeExec {
		pid := os.Getpid()
		processStart, err := agentprocess.ProcessStartIdentity(pid)
		if err != nil {
			return supervisedResult{}, fmt.Errorf("identify claim-boundary supervisor: %w", err)
		}
		if err := failureinject.NewClient(c.BarrierURL).Arrive(ctx, failureinject.Arrival{
			ID: physicalAttemptID + "-claim", Point: claimBeforeExecBarrier,
			SessionID: lease.SessionID, Generation: lease.Generation, ActorID: actorID,
			PID: pid, ProcessStart: processStart,
		}); err != nil {
			return supervisedResult{}, fmt.Errorf("wait after durable claim before exec: %w", err)
		}
	}
	attemptDirectory := filepath.Join(c.RunRoot, physicalAttemptID)
	prepared, err := prepareFencedAttempt(ctx, fencedAttemptInput{
		Directory: attemptDirectory, EffectBinary: c.EffectBinary, EffectPayload: c.EffectPayload,
		WorkspacePath: c.WorkspacePath,
		BarrierURL:    c.BarrierURL, BarrierPoint: c.BarrierPoint,
		SupervisorURL: c.SupervisorURL(), Lease: lease,
		LogicalTurnID: c.LogicalTurnID, LogicalEffectID: c.LogicalEffectID,
		PhysicalAttemptID: physicalAttemptID, ActorID: actorID,
	})
	if err != nil {
		return supervisedResult{}, err
	}
	command := c.Command
	command.AllowedTool = prepared.AllowedTool
	invocation, err := command.SessionInvocation(
		prepared.Prompt, c.SelectedVendorSessionID, int32(lease.Generation),
	)
	if err != nil {
		return supervisedResult{}, err
	}
	invocation = c.supervisedLauncherInvocation(invocation, physicalAttemptID, actorID)
	result, err := RunSupervisedInvocation(ctx, store, lease, invocation, RunInvocationInput{
		Directory: attemptDirectory, AttemptID: physicalAttemptID, ActorID: actorID,
	}, supervisedInvocationHooks{
		RegistrationGate: true,
		AfterFinalOutput: func(hookContext context.Context, result InvocationResult) error {
			if c.FaultBoundary != FaultAfterFinalOutput {
				return nil
			}
			arrival := failureinject.Arrival{
				ID: physicalAttemptID + "-final-output", Point: finalOutputBarrier,
				SessionID: lease.SessionID, Generation: lease.Generation, ActorID: actorID,
				PID: result.Process.PID, ProcessStart: result.Process.StartIdentity,
			}
			return failureinject.NewClient(c.BarrierURL).Arrive(hookContext, arrival)
		},
	})
	if err != nil {
		return supervisedResult{}, c.authorityError(ctx, store, lease, err)
	}
	if result.Claude.SessionID != c.SelectedVendorSessionID {
		return supervisedResult{}, fmt.Errorf("selected Claude session %q observed as %q",
			c.SelectedVendorSessionID, result.Claude.SessionID)
	}
	return supervisedResult{
		VendorSessionID: result.Claude.SessionID, PhysicalAttemptID: physicalAttemptID,
		ProcessIdentity: result.Process.Identity,
		Outcome:         workstore.Outcome{Value: result.Claude.Result},
	}, nil
}

func (c fencedClaudeRunConfig) supervisedLauncherInvocation(invocation Invocation,
	physicalAttemptID, actorID string,
) Invocation {
	realBinary := invocation.Binary
	invocation.Binary = c.LauncherBinary
	environment := append([]string(nil), invocation.Env...)
	invocation.Env = append(environment,
		"CLAUDE_DIRECT_REAL_BINARY="+realBinary,
		"CLAUDE_DIRECT_PHYSICAL_ATTEMPT_ID="+physicalAttemptID,
		"CLAUDE_DIRECT_LOGICAL_SESSION_ID="+c.LogicalSessionID,
		"CLAUDE_DIRECT_ACTOR_ID="+actorID,
	)
	if c.FaultBoundary == FaultBeforeVendorRegistration {
		invocation.Env = append(invocation.Env,
			"CLAUDE_DIRECT_PRE_SESSION_BARRIER_URL="+c.BarrierURL,
			"CLAUDE_DIRECT_PRE_SESSION_BARRIER_POINT="+preRegistrationBarrier,
		)
	}
	return invocation
}

func (c fencedClaudeRunConfig) authorityError(ctx context.Context, store *workstore.Store,
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
	if snapshot.ActiveGeneration != lease.Generation || snapshot.ActiveOwnerTokenHash != workstore.HashToken(lease.OwnerToken) {
		return errors.Join(workstore.ErrStaleOwner, runErr)
	}
	return runErr
}

type fencedAttemptInput struct {
	Directory         string
	EffectBinary      string
	EffectPayload     string
	WorkspacePath     string
	BarrierURL        string
	BarrierPoint      string
	SupervisorURL     string
	Lease             workstore.Lease
	LogicalTurnID     string
	LogicalEffectID   string
	PhysicalAttemptID string
	ActorID           string
}

func prepareFencedAttempt(ctx context.Context, input fencedAttemptInput) (PreparedAttempt, error) {
	if err := ctx.Err(); err != nil {
		return PreparedAttempt{}, err
	}
	if !safeCommandPath(input.Directory) || !safeCommandPath(input.EffectBinary) ||
		input.EffectPayload == "" || !safeCommandPath(input.WorkspacePath) ||
		input.BarrierURL == "" || input.BarrierPoint == "" ||
		input.SupervisorURL == "" || input.Lease.SessionID == "" || input.Lease.Generation == 0 ||
		input.Lease.OwnerToken == "" || input.LogicalTurnID == "" || input.LogicalEffectID == "" ||
		input.PhysicalAttemptID == "" || input.ActorID == "" {
		return PreparedAttempt{}, errors.New("fenced attempt requires safe paths and complete effect authority")
	}
	if err := os.MkdirAll(input.Directory, 0o750); err != nil {
		return PreparedAttempt{}, fmt.Errorf("create fenced attempt directory: %w", err)
	}
	requestPath := filepath.Join(input.Directory, effectRequestFile)
	request := ControlledEffectInput{
		WorkspacePath: input.WorkspacePath,
		SupervisorURL: input.SupervisorURL, OwnershipGeneration: input.Lease.Generation,
		OwnerCapability: input.Lease.OwnerToken, Payload: input.EffectPayload,
		BarrierURL: input.BarrierURL, BarrierPoint: input.BarrierPoint,
		LogicalSessionID: input.Lease.SessionID, LogicalTurnID: input.LogicalTurnID,
		LogicalEffectID: input.LogicalEffectID, PhysicalAttemptID: input.PhysicalAttemptID,
		ActorID: input.ActorID,
	}
	if err := writeJSONExclusive(requestPath, request); err != nil {
		return PreparedAttempt{}, err
	}
	command := input.EffectBinary + " --request " + requestPath
	return PreparedAttempt{
		RequestPath: requestPath, Command: command, AllowedTool: "Bash(" + command + ")",
		Prompt: "Use the Bash tool exactly once to run this exact command and no other command:\n" +
			command + "\nAfter it succeeds, reply with EFFECT_COMPLETE.",
	}, nil
}
