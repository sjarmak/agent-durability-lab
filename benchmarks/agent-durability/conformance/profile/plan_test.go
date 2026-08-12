package profile

import (
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

func TestPlanHasExactlyOneUnfaultedAndThreeUnsafeAndProtectedTrialsPerCase(t *testing.T) {
	t.Parallel()

	plan := Plan()
	if len(plan) != 28 {
		t.Fatalf("len(Plan()) = %d, want 28", len(plan))
	}
	for _, benchmarkCase := range protocol.Cases() {
		counts := map[protocol.Probe]int{}
		trials := map[protocol.Probe]map[int]bool{}
		for _, spec := range plan {
			if spec.Case != benchmarkCase {
				continue
			}
			counts[spec.Probe]++
			if trials[spec.Probe] == nil {
				trials[spec.Probe] = map[int]bool{}
			}
			trials[spec.Probe][spec.Trial] = true
		}
		if counts[protocol.ProbeUnfaulted] != 1 || !trials[protocol.ProbeUnfaulted][1] {
			t.Errorf("%s unfaulted schedule = %v/%v, want trial 1 exactly once", benchmarkCase, counts, trials)
		}
		for _, probe := range []protocol.Probe{protocol.ProbeUnsafe, protocol.ProbeProtected} {
			if counts[probe] != DevelopmentTrials || len(trials[probe]) != DevelopmentTrials {
				t.Errorf("%s/%s schedule = %v/%v, want trials 1..%d", benchmarkCase, probe, counts, trials, DevelopmentTrials)
			}
		}
	}
}
