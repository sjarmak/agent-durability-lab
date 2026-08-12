package semantics

import (
	"context"
	"errors"
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
	"go.temporal.io/sdk/testsuite"
)

func TestTemporalExecutorRejectsUnavailableState(t *testing.T) {
	var unavailable *TemporalExecutor
	if err := unavailable.Ready(context.Background()); !errors.Is(err, protocol.ErrInvalidEvidence) {
		t.Fatalf("nil executor readiness returned %v", err)
	}
	if _, err := unavailable.ServerVersion(context.Background()); !errors.Is(err, protocol.ErrInvalidEvidence) {
		t.Fatalf("nil executor version returned %v", err)
	}

	closed := &TemporalExecutor{server: &testsuite.DevServer{}, closed: true}
	if err := closed.Ready(context.Background()); !errors.Is(err, protocol.ErrInvalidEvidence) {
		t.Fatalf("closed executor readiness returned %v", err)
	}
}

func TestTemporalExecutorRunRejectsUnavailableStateAndMissingIdentity(t *testing.T) {
	ctx := context.Background()
	var unavailable *TemporalExecutor
	if _, err := unavailable.Run(ctx, RunRequest{}); !errors.Is(err, protocol.ErrInvalidEvidence) {
		t.Fatalf("nil executor run returned %v", err)
	}

	closed := &TemporalExecutor{server: &testsuite.DevServer{}, closed: true}
	if _, err := closed.Run(ctx, RunRequest{}); !errors.Is(err, protocol.ErrInvalidEvidence) {
		t.Fatalf("closed executor run returned %v", err)
	}

	ready := &TemporalExecutor{server: &testsuite.DevServer{}}
	if _, err := ready.Run(ctx, RunRequest{}); !errors.Is(err, protocol.ErrInvalidEvidence) {
		t.Fatalf("missing pair identity returned %v", err)
	}
}
