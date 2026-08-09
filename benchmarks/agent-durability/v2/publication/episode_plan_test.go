package publication

import (
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
)

func TestOutageUnsafeRecoverySustainsStormControl(t *testing.T) {
	plan, err := BuildEpisodePlan(EpisodeRequest{
		Phase: PhasePilot, PairID: "pilot-v2-pair/outage-backlog-recovery/unsafe/slot-01", PairIndex: 1,
		Case: protocol.CaseOutageBacklogRecovery, Probe: protocol.ProbeUnsafe, Slot: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	recovery := plan.Rounds[len(plan.Rounds)-1].Work
	if len(recovery) <= 4 {
		t.Fatalf("recovery cohort = %d, must exceed concurrency bound", len(recovery))
	}
	for _, work := range recovery {
		if work.DelayMillis != 0 || work.ServiceMillis < 100 {
			t.Fatalf("unsafe recovery work does not sustain overlap: %+v", work)
		}
	}
}
