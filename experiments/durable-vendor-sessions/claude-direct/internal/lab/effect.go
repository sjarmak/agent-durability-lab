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
)

type ControlledEffectInput struct {
	DestinationPath   string
	WorkspacePath     string
	Payload           string
	BarrierURL        string
	BarrierPoint      string
	LogicalSessionID  string
	LogicalTurnID     string
	LogicalEffectID   string
	PhysicalAttemptID string
	ActorID           string
}

func RunControlledEffect(ctx context.Context, input ControlledEffectInput) error {
	if !input.valid() {
		return errors.New("controlled effect requires destination, barrier, and complete logical and physical identities")
	}
	pid := os.Getpid()
	startIdentity, err := agentprocess.ProcessStartIdentity(pid)
	if err != nil {
		return fmt.Errorf("identify controlled effect process: %w", err)
	}
	processIdentity := fmt.Sprintf("pid:%d:start:%s", pid, startIdentity)
	attempt := EffectAttempt{
		LogicalSessionID: input.LogicalSessionID, LogicalTurnID: input.LogicalTurnID,
		LogicalEffectID: input.LogicalEffectID, PhysicalAttemptID: input.PhysicalAttemptID,
		ActorID: input.ActorID, ProcessIdentity: processIdentity, AppliedAt: time.Now().UTC(),
	}
	if err := CommitEffect(ctx, input.DestinationPath, attempt); err != nil {
		return err
	}
	if err := AppendWorkspaceEffect(ctx, input.WorkspacePath, WorkspaceEffect{
		LogicalEffectID: input.LogicalEffectID, PhysicalAttemptID: input.PhysicalAttemptID,
		Payload: input.Payload, ActorID: input.ActorID, ProcessIdentity: processIdentity,
		AppliedAt: attempt.AppliedAt,
	}); err != nil {
		return err
	}
	arrival := failureinject.Arrival{
		ID: input.PhysicalAttemptID, Point: input.BarrierPoint, SessionID: input.LogicalSessionID,
		Generation: 1, ActorID: input.ActorID, PID: pid, ProcessStart: startIdentity,
	}
	if err := failureinject.NewClient(input.BarrierURL).Arrive(ctx, arrival); err != nil {
		return fmt.Errorf("wait at committed-effect barrier: %w", err)
	}
	return nil
}

func (i ControlledEffectInput) valid() bool {
	return strings.TrimSpace(i.DestinationPath) != "" && strings.TrimSpace(i.WorkspacePath) != "" &&
		i.Payload != "" && strings.TrimSpace(i.BarrierURL) != "" &&
		i.BarrierPoint != "" && i.LogicalSessionID != "" && i.LogicalTurnID != "" &&
		i.LogicalEffectID != "" && i.PhysicalAttemptID != "" && i.ActorID != ""
}
