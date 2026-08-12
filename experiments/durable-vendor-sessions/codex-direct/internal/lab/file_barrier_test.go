package lab

import (
	"context"
	"errors"
	"testing"

	"github.com/sjarmak/temporal_projects/internal/failureinject"
)

func TestFileBarrierBlocksUntilExactRelease(t *testing.T) {
	barrier, err := newFileBarrier(t.TempDir() + "/barrier")
	if err != nil {
		t.Fatalf("create file barrier: %v", err)
	}
	arrival := failureinject.Arrival{
		ID: "attempt-1", Point: committedEffectBarrier, SessionID: "session-1",
		Generation: 1, ActorID: "worker-1", PID: 123, ProcessStart: "start-1",
	}
	arrived := make(chan error, 1)
	go func() { arrived <- arriveFileBarrier(context.Background(), barrier.directory, arrival) }()
	got, err := barrier.WaitForArrivals(context.Background(), 1)
	if err != nil {
		t.Fatalf("wait for arrival: %v", err)
	}
	if len(got) != 1 || !sameFileBarrierArrival(got[0], arrival) {
		t.Fatalf("arrivals = %+v, want %+v", got, arrival)
	}
	select {
	case err := <-arrived:
		t.Fatalf("arrival returned before release: %v", err)
	default:
	}
	if err := barrier.Release(); err != nil {
		t.Fatalf("release file barrier: %v", err)
	}
	if err := <-arrived; err != nil {
		t.Fatalf("arrival after release: %v", err)
	}
	if err := barrier.Release(); err != nil {
		t.Fatalf("idempotent release: %v", err)
	}
}

func TestFileBarrierWaitHonorsCancellation(t *testing.T) {
	barrier, err := newFileBarrier(t.TempDir() + "/barrier")
	if err != nil {
		t.Fatalf("create file barrier: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := barrier.WaitForArrivals(ctx, 1); err == nil {
		t.Fatal("canceled file barrier wait returned nil")
	}
}

func TestFileBarrierWatchHonorsCancellationAfterRegistration(t *testing.T) {
	directory := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	registered := make(chan struct{})
	calls := 0
	finished := make(chan error, 1)
	go func() {
		finished <- waitForFileBarrierChange(ctx, directory, func() (bool, error) {
			calls++
			if calls == 2 {
				close(registered)
			}
			return false, nil
		})
	}()
	<-registered
	cancel()
	if err := <-finished; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled registered watch = %v, want context.Canceled", err)
	}
}

func TestFileBarrierRejectsConflictingArrivalIdentity(t *testing.T) {
	barrier, err := newFileBarrier(t.TempDir() + "/barrier")
	if err != nil {
		t.Fatalf("create file barrier: %v", err)
	}
	if err := barrier.Release(); err != nil {
		t.Fatalf("release file barrier: %v", err)
	}
	first := failureinject.Arrival{
		ID: "attempt-1", Point: committedEffectBarrier, SessionID: "session-1", ActorID: "worker-1",
	}
	if err := arriveFileBarrier(context.Background(), barrier.directory, first); err != nil {
		t.Fatalf("first arrival: %v", err)
	}
	conflict := first
	conflict.ActorID = "worker-2"
	if err := arriveFileBarrier(context.Background(), barrier.directory, conflict); err == nil {
		t.Fatal("conflicting file barrier arrival was accepted")
	}
}
