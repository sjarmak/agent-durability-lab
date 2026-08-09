package temporaladapter

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/systemplan"
)

func TestLiveSessionExportsReplayableHistoryWithRetriedFault(t *testing.T) {
	temporalPath := os.Getenv("TEMPORAL_CLI_PATH")
	if temporalPath == "" {
		t.Skip("TEMPORAL_CLI_PATH is required for live Temporal adapter test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	session, err := Open(ctx, Config{
		TemporalPath: temporalPath, WorkRoot: t.TempDir(), AdapterVersion: "source-sha256:" + strings.Repeat("a", 64),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := session.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()
	plan, _ := systemplan.Build(protocol.CaseSilentProgress, protocol.ProbeProtected, 1)
	execution, err := session.Execute(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	if !execution.ReplayVerified || len(execution.Native) == 0 || execution.SystemID != "temporal" || execution.Settings["history_replay"] != "passed" {
		t.Fatalf("execution = %+v", execution)
	}
}
