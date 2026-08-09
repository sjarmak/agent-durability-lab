package systemplan

import (
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
)

func TestBuildCoversEveryCaseAndProbeWithOneExactFault(t *testing.T) {
	for _, benchmarkCase := range protocol.Cases() {
		for _, probe := range []protocol.Probe{protocol.ProbeUnfaulted, protocol.ProbeUnsafe, protocol.ProbeProtected} {
			plan, err := Build(benchmarkCase, probe, 1)
			if err != nil {
				t.Fatalf("%s/%s: %v", benchmarkCase, probe, err)
			}
			faults := 0
			for _, step := range plan.Steps {
				if step.FailureOnce {
					faults++
				}
			}
			want := 1
			if probe == protocol.ProbeUnfaulted {
				want = 0
			}
			if faults != want {
				t.Errorf("%s/%s faults=%d, want %d", benchmarkCase, probe, faults, want)
			}
		}
	}
}

func TestPlanValidationRejectsChangedIdentityAndFaultCount(t *testing.T) {
	plan, err := Build(protocol.CaseSilentProgress, protocol.ProbeProtected, 1)
	if err != nil {
		t.Fatal(err)
	}
	plan.Steps[1].ID = plan.Steps[0].ID
	if err := plan.Validate(); err == nil {
		t.Fatal("duplicate step identity accepted")
	}
	plan, _ = Build(protocol.CaseSilentProgress, protocol.ProbeProtected, 1)
	plan.Steps[2].FailureOnce = false
	if err := plan.Validate(); err == nil {
		t.Fatal("missing exact fault accepted")
	}
}
