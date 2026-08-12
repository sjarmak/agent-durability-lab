package lab

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sjarmak/temporal_projects/internal/agentprocess"
	"github.com/sjarmak/temporal_projects/internal/failureinject"
	"github.com/sjarmak/temporal_projects/internal/workstore"
)

const committedEffectBarrier = "codex-tool-effect-committed"

type ControlledEffectInput struct {
	DestinationPath     string `json:"destination_path,omitempty"`
	WorkspacePath       string `json:"workspace_path"`
	ThreadReceiptPath   string `json:"thread_receipt_path"`
	CanonicalThreadPath string `json:"canonical_thread_path,omitempty"`
	SupervisorURL       string `json:"supervisor_url,omitempty"`
	AuthorityStorePath  string `json:"authority_store_path,omitempty"`
	OwnershipGeneration uint64 `json:"ownership_generation,omitempty"`
	OwnerCapability     string `json:"owner_capability,omitempty"`
	Payload             string `json:"payload"`
	BarrierURL          string `json:"barrier_url"`
	BarrierDirectory    string `json:"barrier_directory,omitempty"`
	BarrierPoint        string `json:"barrier_point"`
	LogicalSessionID    string `json:"logical_session_id"`
	LogicalTurnID       string `json:"logical_turn_id"`
	LogicalEffectID     string `json:"logical_effect_id"`
	PhysicalAttemptID   string `json:"physical_attempt_id"`
	ActorID             string `json:"actor_id"`
}

func RunControlledEffect(ctx context.Context, input ControlledEffectInput) error {
	if !input.valid() {
		return errors.New("controlled effect requires destination, barrier, and complete authority identities")
	}
	if input.ThreadReceiptPath != "" {
		threadReceipt, err := ReadThreadReceipt(input.ThreadReceiptPath)
		if err != nil {
			return err
		}
		if threadReceipt.PhysicalAttemptID != input.PhysicalAttemptID || threadReceipt.ActorID != input.ActorID {
			return errors.New("controlled effect does not match its durable Codex thread receipt")
		}
		if input.CanonicalThreadPath != "" {
			canonical, err := WaitForCanonicalThread(ctx, input.CanonicalThreadPath)
			if err != nil {
				return err
			}
			if canonical.LogicalSessionID != input.LogicalSessionID || canonical.LogicalTurnID != input.LogicalTurnID ||
				canonical.ThreadID != threadReceipt.ThreadID {
				return errors.New("controlled effect thread is not the canonical application thread")
			}
		}
	}
	pid := os.Getpid()
	startIdentity, err := agentprocess.ProcessStartIdentity(pid)
	if err != nil {
		return fmt.Errorf("identify controlled effect process: %w", err)
	}
	processIdentity := fmt.Sprintf("pid:%d:start:%s", pid, startIdentity)
	appliedAt := time.Now().UTC()
	if input.SupervisorURL != "" || input.AuthorityStorePath != "" {
		if err := commitFencedEffect(ctx, input); err != nil {
			return err
		}
		if err := AppendWorkspaceEffectOnce(ctx, input.WorkspacePath, input.workspaceReceipt(processIdentity, appliedAt)); err != nil {
			return err
		}
	} else {
		if err := CommitEffect(ctx, input.DestinationPath, EffectAttempt{
			LogicalSessionID: input.LogicalSessionID, LogicalTurnID: input.LogicalTurnID,
			LogicalEffectID: input.LogicalEffectID, PhysicalAttemptID: input.PhysicalAttemptID,
			ActorID: input.ActorID, ProcessIdentity: processIdentity, AppliedAt: appliedAt,
		}); err != nil {
			return err
		}
		if err := AppendWorkspaceEffect(ctx, input.WorkspacePath, input.workspaceReceipt(processIdentity, appliedAt)); err != nil {
			return err
		}
	}
	arrival := failureinject.Arrival{
		ID: input.PhysicalAttemptID, Point: input.BarrierPoint, SessionID: input.LogicalSessionID,
		Generation: input.generation(), ActorID: input.ActorID, PID: pid, ProcessStart: startIdentity,
	}
	var barrierErr error
	if input.BarrierDirectory != "" {
		barrierErr = arriveFileBarrier(ctx, input.BarrierDirectory, arrival)
	} else {
		barrierErr = failureinject.NewClient(input.BarrierURL).Arrive(ctx, arrival)
	}
	if barrierErr != nil {
		return fmt.Errorf("wait at committed Codex effect barrier: %w", barrierErr)
	}
	return nil
}

func commitFencedEffect(ctx context.Context, input ControlledEffectInput) error {
	if input.AuthorityStorePath != "" {
		store, err := workstore.Open(input.AuthorityStorePath)
		if err != nil {
			return fmt.Errorf("open fenced authority store: %w", err)
		}
		if err := store.CommitEffectOnce(ctx, workstore.Lease{
			SessionID: input.LogicalSessionID, Generation: input.OwnershipGeneration,
			OwnerToken: input.OwnerCapability,
		}, workstore.Effect{ID: input.LogicalEffectID, Value: input.Payload}); err != nil {
			return fmt.Errorf("commit fenced controlled effect: %w", err)
		}
		return nil
	}
	if err := newSupervisorClient(input.SupervisorURL, nil).CommitEffect(ctx, supervisorEffectRequest{
		SessionID: input.LogicalSessionID, Generation: input.OwnershipGeneration,
		OwnerCapability: input.OwnerCapability, EffectID: input.LogicalEffectID, Value: input.Payload,
	}); err != nil {
		return fmt.Errorf("commit fenced controlled effect: %w", err)
	}
	return nil
}

func (i ControlledEffectInput) valid() bool {
	directThread := i.ThreadReceiptPath == "" && i.CanonicalThreadPath == "" ||
		safeCommandPath(i.ThreadReceiptPath) && (i.CanonicalThreadPath == "" || safeCommandPath(i.CanonicalThreadPath))
	direct := strings.TrimSpace(i.DestinationPath) != "" && i.SupervisorURL == "" &&
		i.OwnershipGeneration == 0 && i.OwnerCapability == "" && directThread
	fencedAuthority := strings.TrimSpace(i.SupervisorURL) != "" || safeCommandPath(i.AuthorityStorePath)
	fenced := i.DestinationPath == "" && fencedAuthority &&
		!(strings.TrimSpace(i.SupervisorURL) != "" && i.AuthorityStorePath != "") &&
		i.OwnershipGeneration > 0 && i.OwnerCapability != "" && safeCommandPath(i.ThreadReceiptPath) &&
		safeCommandPath(i.CanonicalThreadPath)
	return (direct || fenced) && strings.TrimSpace(i.WorkspacePath) != "" && i.Payload != "" &&
		(strings.TrimSpace(i.BarrierURL) != "" || safeCommandPath(i.BarrierDirectory)) &&
		!(strings.TrimSpace(i.BarrierURL) != "" && i.BarrierDirectory != "") &&
		i.BarrierPoint != "" && i.LogicalSessionID != "" &&
		i.LogicalTurnID != "" && i.LogicalEffectID != "" && i.PhysicalAttemptID != "" && i.ActorID != ""
}

func (i ControlledEffectInput) generation() uint64 {
	if i.OwnershipGeneration > 0 {
		return i.OwnershipGeneration
	}
	return 1
}

func (i ControlledEffectInput) workspaceReceipt(processIdentity string, appliedAt time.Time) WorkspaceEffect {
	return WorkspaceEffect{
		LogicalEffectID: i.LogicalEffectID, PhysicalAttemptID: i.PhysicalAttemptID,
		Payload: i.Payload, ActorID: i.ActorID, ProcessIdentity: processIdentity, AppliedAt: appliedAt,
	}
}
