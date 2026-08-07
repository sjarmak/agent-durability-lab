package main

import (
	"context"
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

func TestRunPreservesThreeTrialsForEveryCaseAndProbe(t *testing.T) {
	t.Parallel()

	summary, err := run(context.Background(), t.TempDir())
	if err != nil {
		t.Fatalf("run calibration suite: %v", err)
	}
	wantPerClass := len(protocol.Cases()) * 3
	if summary.ValidPass != wantPerClass*2 || summary.ValidFail != wantPerClass || summary.Invalid != 0 {
		t.Fatalf("summary = %+v, want passes=%d failures=%d invalid=0", summary, wantPerClass*2, wantPerClass)
	}
}
