package main

import (
	"context"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/internal/failureinject"
)

func TestLauncherWaitsAtExactProcessBeforeThreadBoundary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	coordinator := failureinject.NewCoordinator()
	server := httptest.NewServer(coordinator.Handler())
	t.Cleanup(server.Close)
	config := launcherConfig{
		RealBinary: "/opt/codex", BarrierURL: server.URL, BarrierPoint: "pre-thread",
		PhysicalAttemptID: "attempt-1", LogicalSessionID: "session-1", Generation: 1, ActorID: "worker-1",
		Args: []string{"exec", "--json"},
	}
	gateReader, gateWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer gateWriter.Close()
	config.ParentProcessGateFD = int(gateReader.Fd())
	executed := make(chan launcherConfig, 1)
	finished := make(chan error, 1)
	go func() {
		finished <- runLauncher(ctx, config, func(got launcherConfig) error {
			executed <- got
			return nil
		})
	}()
	if got := coordinator.ArrivalCount(config.BarrierPoint); got != 0 {
		t.Fatalf("arrival count before parent process receipt = %d", got)
	}
	if _, err := gateWriter.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	if err := gateWriter.Close(); err != nil {
		t.Fatal(err)
	}
	arrivals, err := coordinator.WaitForArrivals(ctx, config.BarrierPoint, 1)
	if err != nil {
		t.Fatalf("wait for launcher: %v", err)
	}
	if len(arrivals) != 1 || arrivals[0].ID != config.PhysicalAttemptID || arrivals[0].PID <= 0 || arrivals[0].ProcessStart == "" {
		t.Fatalf("arrival = %+v", arrivals)
	}
	select {
	case <-executed:
		t.Fatal("Codex exec happened before boundary release")
	default:
	}
	if err := coordinator.Release(config.BarrierPoint); err != nil {
		t.Fatalf("release launcher: %v", err)
	}
	if err := <-finished; err != nil {
		t.Fatalf("launcher: %v", err)
	}
	if got := <-executed; got.RealBinary != config.RealBinary {
		t.Fatalf("executed = %+v", got)
	}
}

func TestLauncherRejectsIncompleteConfiguration(t *testing.T) {
	if err := runLauncher(context.Background(), launcherConfig{}, func(launcherConfig) error { return nil }); err == nil {
		t.Fatal("incomplete launcher unexpectedly succeeded")
	}
}
