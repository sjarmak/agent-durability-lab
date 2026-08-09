package lab

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/sjarmak/temporal_projects/internal/failureinject"
)

func TestFinalOutputBarrierBlocksActivityCompletion(t *testing.T) {
	coordinator := failureinject.NewCoordinator()
	server := httptest.NewServer(coordinator.Handler())
	defer server.Close()
	finished := make(chan error, 1)
	go func() {
		finished <- waitAtFinalOutputBarrier(context.Background(), server.URL, finalOutputBarrier,
			"logical-session-1", "physical-attempt-1", "worker-one-attempt-1")
	}()
	arrivals, err := coordinator.WaitForArrivals(context.Background(), finalOutputBarrier, 1)
	if err != nil {
		t.Fatalf("wait for final-output barrier: %v", err)
	}
	if arrivals[0].ID != "physical-attempt-1-final-output" || arrivals[0].PID < 1 {
		t.Fatalf("final-output arrival = %+v", arrivals[0])
	}
	select {
	case err := <-finished:
		t.Fatalf("final-output barrier returned before release: %v", err)
	default:
	}
	if err := coordinator.Release(finalOutputBarrier); err != nil {
		t.Fatalf("release final output: %v", err)
	}
	if err := <-finished; err != nil {
		t.Fatalf("wait at final output: %v", err)
	}
}
