package publication

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
)

func TestLivePostgreSQLPublicationExecutor(t *testing.T) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN is required")
	}
	root := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	executor, err := OpenPostgreSQLTimedExecutor(ctx, PostgreSQLExecutorConfig{
		DSN: dsn, EvidenceRoot: root,
		AdapterVersion: "source-sha256:" + digest("integration-adapter"), AgentBinarySHA256: digest("integration-agent"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := executor.Close(); err != nil {
			t.Errorf("close PostgreSQL executor: %v", err)
		}
	})
	suffix := randomHex()
	var requests []EpisodeRequest
	for _, benchmarkCase := range protocol.Cases() {
		for _, probe := range []protocol.Probe{protocol.ProbeUnfaulted, protocol.ProbeUnsafe, protocol.ProbeProtected} {
			requests = append(requests, EpisodeRequest{
				Phase: PhasePilot, PairID: "live-postgresql-" + string(benchmarkCase) + "-" + string(probe) + "-" + suffix,
				PairIndex: len(requests) + 1, Case: benchmarkCase, Probe: probe, Slot: 1,
			})
		}
	}
	for _, request := range requests {
		if err := executor.Ready(ctx); err != nil {
			t.Fatal(err)
		}
		timing := NewTimingRecorder(wallClock{}, executor.SystemID(), request)
		result, err := executor.ExecuteTimed(ctx, request, timing)
		if err != nil {
			t.Fatalf("%s/%s: %v", request.Case, request.Probe, err)
		}
		if err := result.Verdict.Validate(); err != nil {
			t.Fatal(err)
		}
		if request.Probe != protocol.ProbeUnsafe && !result.Verdict.EfficiencyEligible {
			t.Fatalf("protected/unfaulted verdict = %+v", result.Verdict)
		}
		if request.Probe == protocol.ProbeUnsafe && result.Verdict.Correctness == protocol.OutcomePass &&
			result.Verdict.Safety == protocol.OutcomePass && result.Verdict.Liveness == protocol.OutcomePass {
			t.Fatalf("unsafe control did not distinguish: %+v", result.Verdict)
		}
		if err := ValidateTimingEvents(request.Probe, timing.Events()); err != nil {
			t.Fatal(err)
		}
	}
}
