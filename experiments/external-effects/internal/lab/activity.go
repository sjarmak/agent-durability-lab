package lab

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/temporalio-labs/agent-durability-lab/internal/failureinject"
	"go.temporal.io/sdk/activity"
)

type Activities struct {
	WorkerID string
}

func (a Activities) Apply(ctx context.Context, input WorkflowInput) (string, error) {
	if a.WorkerID == "" || input.StorePath == "" || input.BarrierURL == "" {
		return "", errors.New("activity requires Worker ID, observation store, and barrier URL")
	}
	info := activity.GetInfo(ctx)
	attempt := info.Attempt
	observation := AttemptObservation{
		Attempt: attempt, WorkerID: a.WorkerID, PID: os.Getpid(), StartedAt: time.Now().UTC(),
	}
	if err := recordAttemptStart(input.StorePath, observation); err != nil {
		return "", fmt.Errorf("record attempt %d start: %w", attempt, err)
	}
	request := EffectRequest{
		EffectID: input.EffectID, Payload: input.Payload, Mode: input.Mode, Attempt: attempt,
	}
	requestedAt := time.Now().UTC()
	result, effectErr := applyEffect(ctx, input.Destination, input.Config, request)
	respondedAt := time.Now().UTC()
	if err := recordAttemptFinish(input.StorePath, attempt, requestedAt, respondedAt, result, effectErr); err != nil {
		return "", fmt.Errorf("record attempt %d finish: %w", attempt, err)
	}
	if effectErr != nil {
		return "", effectErr
	}
	if attempt == 1 {
		err := failureinject.NewClient(input.BarrierURL).Arrive(ctx, failureinject.Arrival{
			ID: input.EffectID + "/attempt-1", Point: "after-effect/attempt-1",
			SessionID: input.EffectID, ActorID: a.WorkerID, PID: os.Getpid(),
		})
		if err != nil {
			return "", fmt.Errorf("wait at post-effect barrier: %w", err)
		}
		return "", errors.New("post-effect barrier unexpectedly released")
	}
	return result.Receipt, nil
}
