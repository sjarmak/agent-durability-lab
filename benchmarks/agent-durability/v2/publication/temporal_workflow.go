package publication

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	TemporalPublicationWorkflowName = "AgentDurabilityPublicationV1"
	TemporalPublicationActivityName = "AgentDurabilityPublicationActivityV1"
)

type TemporalWorkflowInput struct {
	ExecutionKey string      `json:"execution_key"`
	Plan         EpisodePlan `json:"plan"`
}

type TemporalActivityInput struct {
	ExecutionKey  string   `json:"execution_key"`
	Work          WorkSpec `json:"work"`
	ABAAction     string   `json:"aba_action,omitempty"`
	Generation    uint64   `json:"generation,omitempty"`
	StaleAccepted bool     `json:"stale_accepted,omitempty"`
}

func TemporalPublicationWorkflow(ctx workflow.Context, input TemporalWorkflowInput) error {
	if input.ExecutionKey == "" {
		return invalid("Temporal publication execution key")
	}
	if err := input.Plan.Validate(); err != nil {
		return err
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	if input.Plan.Case == "aba-reacquisition" && input.Plan.Probe != "unfaulted" {
		return runTemporalABA(ctx, input)
	}
	for _, round := range input.Plan.Rounds {
		results := workflow.NewBufferedChannel(ctx, len(round.Work))
		for _, work := range round.Work {
			work := work
			workflow.Go(ctx, func(childCtx workflow.Context) {
				if work.DelayMillis > 0 {
					if err := workflow.Sleep(childCtx, time.Duration(work.DelayMillis)*time.Millisecond); err != nil {
						results.Send(childCtx, err.Error())
						return
					}
				}
				err := workflow.ExecuteActivity(childCtx, TemporalPublicationActivityName, TemporalActivityInput{
					ExecutionKey: input.ExecutionKey, Work: work,
				}).Get(childCtx, nil)
				if err != nil {
					results.Send(childCtx, err.Error())
					return
				}
				results.Send(childCtx, "")
			})
		}
		for range round.Work {
			var result string
			results.Receive(ctx, &result)
			if result != "" {
				return fmt.Errorf("execute Temporal publication round %d: %s", round.Sequence, result)
			}
		}
	}
	return nil
}

func runTemporalABA(ctx workflow.Context, input TemporalWorkflowInput) error {
	begin := workflow.ExecuteActivity(ctx, TemporalPublicationActivityName, TemporalActivityInput{
		ExecutionKey: input.ExecutionKey, ABAAction: "begin-stale",
	})
	for _, command := range []TemporalActivityInput{
		{ExecutionKey: input.ExecutionKey, ABAAction: "wait-barrier"},
		{ExecutionKey: input.ExecutionKey, ABAAction: "advance", Generation: 8},
		{ExecutionKey: input.ExecutionKey, ABAAction: "advance", Generation: 9},
		{ExecutionKey: input.ExecutionKey, ABAAction: "complete-current"},
		{ExecutionKey: input.ExecutionKey, ABAAction: "release-stale", StaleAccepted: input.Plan.Probe == "unsafe"},
	} {
		if err := workflow.ExecuteActivity(ctx, TemporalPublicationActivityName, command).Get(ctx, nil); err != nil {
			return err
		}
	}
	return begin.Get(ctx, nil)
}

type temporalActivityHandler struct {
	lookup   func(string) (*EpisodeRuntime, bool)
	workerID string
}

func (h *temporalActivityHandler) Execute(ctx context.Context, input TemporalActivityInput) error {
	episode, ok := h.lookup(input.ExecutionKey)
	if !ok {
		return invalid("Temporal publication runtime lookup")
	}
	info := activity.GetInfo(ctx)
	identity := NativeIdentity{
		WorkerID:        h.workerID,
		ProcessIdentity: fmt.Sprintf("pid:%d:activity:%s:attempt:%d", os.Getpid(), info.ActivityID, info.Attempt),
	}
	switch input.ABAAction {
	case "":
		return episode.RunWork(ctx, input.Work, identity)
	case "begin-stale":
		return episode.BeginABA(ctx, identity)
	case "wait-barrier":
		return episode.WaitABABarrier(ctx)
	case "advance":
		return episode.AdvanceABA(input.Generation, identity)
	case "complete-current":
		return episode.CompleteABACurrent(ctx)
	case "release-stale":
		episode.ReleaseABA(input.StaleAccepted)
		return nil
	default:
		return invalid("Temporal publication ABA action")
	}
}
