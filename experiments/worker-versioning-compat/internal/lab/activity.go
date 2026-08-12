package lab

import (
	"context"
	"errors"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

type VersionedActivities struct {
	WorkerBuild           string
	AgentBuild            string
	CompatibleAgentBuilds []string
}

func (a VersionedActivities) Attach(ctx context.Context, input ActivityInput) (ActivityReceipt, error) {
	info := activity.GetInfo(ctx)
	receipt, err := (Registry{Path: input.RegistryPath}).StartOrAttach(ctx, AttachRequest{
		SessionID: input.SessionID, AgentBuild: a.AgentBuild, WorkerBuild: a.WorkerBuild,
		CompatibleAgentBuilds: a.CompatibleAgentBuilds,
	})
	if err != nil {
		if errors.Is(err, ErrIncompatibleAgentBuild) {
			return ActivityReceipt{}, temporal.NewNonRetryableApplicationError(
				err.Error(), "incompatible-agent-build", err,
			)
		}
		return ActivityReceipt{}, err
	}
	return ActivityReceipt{
		SessionID: input.SessionID, WorkerBuild: a.WorkerBuild, AgentBuild: receipt.AgentBuild,
		Action: receipt.Action, TemporalAttempt: info.Attempt,
		WorkflowID: info.WorkflowExecution.ID, RunID: info.WorkflowExecution.RunID, Phase: input.Phase,
	}, nil
}
