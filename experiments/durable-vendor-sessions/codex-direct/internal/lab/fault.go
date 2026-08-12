package lab

import "github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"

type FaultBoundary string

const (
	FaultNone                          FaultBoundary = "unfaulted"
	FaultAfterClaimBeforeExec          FaultBoundary = "claim-committed-before-process-exec"
	FaultBeforeThreadObservation       FaultBoundary = protocol.FaultPointProcessCreatedBeforeVendorRegistration
	FaultAfterThreadBeforeRegistration FaultBoundary = "codex-thread-observed-before-durable-registration"
	FaultAfterToolEffect               FaultBoundary = protocol.FaultPointToolEffectBeforeActivityCompletion
	FaultAfterFinalOutput              FaultBoundary = protocol.FaultPointFinalOutputBeforeActivityCompletion
	FaultConcurrentRecovery            FaultBoundary = "concurrent-recovery-at-effect-boundary"
	FaultCancellationWhileExecuting    FaultBoundary = "cancellation-while-executing"
	FaultProcessFailureReplacement     FaultBoundary = "authorized-process-failure-before-thread"
)

const (
	claimBeforeExecBarrier    = "codex-claim-committed-before-exec"
	preThreadBarrier          = "codex-process-created-before-thread-observation"
	threadRegistrationBarrier = "codex-thread-observed-before-durable-registration"
	finalOutputBarrier        = "codex-final-output-before-activity-completion"
)

func unsafeFaultSchedule() []FaultBoundary {
	return []FaultBoundary{FaultBeforeThreadObservation, FaultAfterToolEffect, FaultAfterFinalOutput}
}

func fencedFaultSchedule() []FaultBoundary {
	return []FaultBoundary{
		FaultAfterClaimBeforeExec, FaultBeforeThreadObservation, FaultAfterThreadBeforeRegistration,
		FaultAfterToolEffect, FaultAfterFinalOutput, FaultConcurrentRecovery,
		FaultCancellationWhileExecuting, FaultProcessFailureReplacement,
	}
}

func (b FaultBoundary) valid() bool {
	return b == FaultNone || b == FaultAfterClaimBeforeExec || b == FaultBeforeThreadObservation ||
		b == FaultAfterThreadBeforeRegistration || b == FaultAfterToolEffect || b == FaultAfterFinalOutput ||
		b == FaultConcurrentRecovery || b == FaultCancellationWhileExecuting || b == FaultProcessFailureReplacement
}
