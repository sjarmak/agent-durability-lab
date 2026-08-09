package publication

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
	"go.temporal.io/sdk/activity"
	temporallog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestTemporalPublicationWorkflowExecutesEveryFrozenRound(t *testing.T) {
	for _, benchmarkCase := range protocol.Cases() {
		for _, probe := range []protocol.Probe{protocol.ProbeUnfaulted, protocol.ProbeUnsafe, protocol.ProbeProtected} {
			benchmarkCase, probe := benchmarkCase, probe
			t.Run(string(benchmarkCase)+"/"+string(probe), func(t *testing.T) {
				request := EpisodeRequest{Case: benchmarkCase, Probe: probe, Slot: 1}
				plan, err := BuildEpisodePlan(request)
				if err != nil {
					t.Fatal(err)
				}
				var suite testsuite.WorkflowTestSuite
				suite.SetLogger(temporallog.NewStructuredLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
				environment := suite.NewTestWorkflowEnvironment()
				var mu sync.Mutex
				calls := 0
				environment.RegisterWorkflowWithOptions(TemporalPublicationWorkflow, workflow.RegisterOptions{Name: TemporalPublicationWorkflowName})
				environment.RegisterActivityWithOptions(func(context.Context, TemporalActivityInput) error {
					mu.Lock()
					calls++
					mu.Unlock()
					return nil
				}, activity.RegisterOptions{Name: TemporalPublicationActivityName})
				environment.ExecuteWorkflow(TemporalPublicationWorkflowName, TemporalWorkflowInput{ExecutionKey: "test", Plan: plan})
				if err := environment.GetWorkflowError(); err != nil {
					t.Fatal(err)
				}
				want := 0
				if benchmarkCase == protocol.CaseABAReacquisition && probe != protocol.ProbeUnfaulted {
					want = 6
				} else {
					for _, round := range plan.Rounds {
						want += len(round.Work)
					}
				}
				if calls != want {
					t.Fatalf("activity calls = %d, want %d", calls, want)
				}
			})
		}
	}
}
