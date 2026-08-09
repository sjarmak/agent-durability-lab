package lab

import (
	"context"
	"fmt"
	"os"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
	"github.com/sjarmak/temporal_projects/internal/agentprocess"
	"github.com/sjarmak/temporal_projects/internal/failureinject"
)

type FaultBoundary string

const (
	FaultNone                     FaultBoundary = "unfaulted"
	FaultBeforeVendorRegistration FaultBoundary = protocol.FaultPointProcessCreatedBeforeVendorRegistration
	FaultAfterToolEffect          FaultBoundary = protocol.FaultPointToolEffectBeforeActivityCompletion
	FaultAfterFinalOutput         FaultBoundary = protocol.FaultPointFinalOutputBeforeActivityCompletion
)

const (
	preRegistrationBarrier = "claude-process-created-before-vendor-registration"
	finalOutputBarrier     = "claude-final-output-before-activity-completion"
)

func unsafeFaultSchedule() []FaultBoundary {
	return []FaultBoundary{
		FaultBeforeVendorRegistration,
		FaultAfterToolEffect,
		FaultAfterFinalOutput,
	}
}

func (b FaultBoundary) valid() bool {
	return b == FaultNone || b == FaultBeforeVendorRegistration ||
		b == FaultAfterToolEffect || b == FaultAfterFinalOutput
}

func waitAtFinalOutputBarrier(ctx context.Context, barrierURL, point, logicalSessionID,
	physicalAttemptID, actorID string,
) error {
	pid := os.Getpid()
	processStart, err := agentprocess.ProcessStartIdentity(pid)
	if err != nil {
		return fmt.Errorf("identify final-output Worker: %w", err)
	}
	arrival := failureinject.Arrival{
		ID: physicalAttemptID + "-final-output", Point: point, SessionID: logicalSessionID,
		ActorID: actorID, PID: pid, ProcessStart: processStart,
	}
	if err := failureinject.NewClient(barrierURL).Arrive(ctx, arrival); err != nil {
		return fmt.Errorf("wait after final Claude output: %w", err)
	}
	return nil
}
