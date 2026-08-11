package main

import (
	"context"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"

	"github.com/sjarmak/temporal_projects/internal/failureinject"
)

func TestRunLauncherBlocksBeforeVendorExec(t *testing.T) {
	coordinator := failureinject.NewCoordinator()
	server := httptest.NewServer(coordinator.Handler())
	defer server.Close()
	config := launcherConfig{
		RealBinary: "/opt/claude", BarrierURL: server.URL, BarrierPoint: "pre-registration",
		ArrivalID: "physical-attempt-1", SessionID: "logical-session-1", ActorID: "worker-one-attempt-1",
		Args: []string{"-p", "--output-format", "stream-json"},
	}
	executed := make(chan launcherConfig, 1)
	finished := make(chan error, 1)
	go func() {
		finished <- runLauncher(context.Background(), config, func(got launcherConfig) error {
			executed <- got
			return nil
		})
	}()
	arrivals, err := coordinator.WaitForArrivals(context.Background(), config.BarrierPoint, 1)
	if err != nil {
		t.Fatalf("wait for launcher barrier: %v", err)
	}
	if arrivals[0].PID < 1 || arrivals[0].ProcessStart == "" {
		t.Fatalf("launcher process identity = %+v", arrivals[0])
	}
	select {
	case got := <-executed:
		t.Fatalf("vendor exec occurred before release: %+v", got)
	default:
	}
	if err := coordinator.Release(config.BarrierPoint); err != nil {
		t.Fatalf("release launcher: %v", err)
	}
	if err := <-finished; err != nil {
		t.Fatalf("run launcher: %v", err)
	}
	got := <-executed
	if got.RealBinary != config.RealBinary || !reflect.DeepEqual(got.Args, config.Args) {
		t.Fatalf("exec config = %+v, want %+v", got, config)
	}
}

func TestRunLauncherWaitsForDurableRegistrationGateBeforeExec(t *testing.T) {
	gateReader, gateWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("create registration gate: %v", err)
	}
	t.Cleanup(func() {
		_ = gateReader.Close()
		_ = gateWriter.Close()
	})
	reachedGate := make(chan struct{})
	config := launcherConfig{
		RealBinary: "/opt/claude", RegistrationGateFD: int(gateReader.Fd()),
		RegistrationGateReached: func() { close(reachedGate) },
		ArrivalID:               "supervisor-generation-1-attempt-1", SessionID: "logical-session-1",
		ActorID: "claude-supervisor-g1", Args: []string{"-p"},
	}
	executed := make(chan struct{}, 1)
	finished := make(chan error, 1)
	go func() {
		finished <- runLauncher(context.Background(), config, func(launcherConfig) error {
			executed <- struct{}{}
			return nil
		})
	}()
	<-reachedGate
	select {
	case <-executed:
		t.Fatal("vendor exec occurred before durable registration release")
	default:
	}
	if _, err := gateWriter.Write([]byte{1}); err != nil {
		t.Fatalf("release registration gate: %v", err)
	}
	if err := gateWriter.Close(); err != nil {
		t.Fatalf("close registration gate: %v", err)
	}
	if err := <-finished; err != nil {
		t.Fatalf("run gated launcher: %v", err)
	}
	<-executed
}

func TestLauncherConfigRejectsIncompleteIdentity(t *testing.T) {
	if err := (launcherConfig{}).validate(); err == nil {
		t.Fatal("empty launcher config validated")
	}
}
