package lab

import (
	"context"
	"errors"
	"testing"

	"github.com/sjarmak/temporal_projects/internal/failureinject"
)

func TestWaitForEffectArrivalsFailsWhenWorkflowClosesWithoutEffect(t *testing.T) {
	workflowClosed := make(chan struct{})
	close(workflowClosed)
	_, err := waitForEffectArrivals(context.Background(), 1,
		func(ctx context.Context) ([]failureinject.Arrival, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
		func(context.Context) error {
			<-workflowClosed
			return nil
		},
		func() ([]failureinject.Arrival, error) { return nil, nil },
	)
	if !errors.Is(err, errWorkflowClosedBeforeEffect) {
		t.Fatalf("wait without effect = %v, want workflow-before-effect error", err)
	}
}

func TestWaitForEffectArrivalsAcceptsExactArrivalBeforeWorkflowClose(t *testing.T) {
	want := []failureinject.Arrival{{ID: "attempt-1"}}
	got, err := waitForEffectArrivals(context.Background(), 1,
		func(context.Context) ([]failureinject.Arrival, error) { return want, nil },
		func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
		func() ([]failureinject.Arrival, error) { return nil, nil },
	)
	if err != nil || len(got) != 1 || got[0].ID != want[0].ID {
		t.Fatalf("wait with exact arrival = %+v, %v", got, err)
	}
}
