package systemsuite

import (
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
	v2protocol "github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
)

func TestEveryV1CaseMapsToAValidDurableSystemPlan(t *testing.T) {
	for _, benchmarkCase := range protocol.Cases() {
		plan, err := systemPlan(benchmarkCase, protocol.ProbeProtected, 1)
		if err != nil {
			t.Fatalf("%s: %v", benchmarkCase, err)
		}
		if plan.Probe != v2protocol.ProbeProtected || len(plan.Steps) < 3 {
			t.Errorf("%s plan = %+v", benchmarkCase, plan)
		}
	}
}
