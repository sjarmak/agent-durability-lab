package profile

import (
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/conformance/evidence"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

const DevelopmentTrials = evidence.DevelopmentTrials

type RunSpec struct {
	Case  protocol.CaseID
	Probe protocol.Probe
	Trial int
}

func Plan() []RunSpec {
	plan := make([]RunSpec, 0, len(protocol.Cases())*(1+2*DevelopmentTrials))
	for _, benchmarkCase := range protocol.Cases() {
		plan = append(plan, RunSpec{Case: benchmarkCase, Probe: protocol.ProbeUnfaulted, Trial: 1})
		for _, probe := range []protocol.Probe{protocol.ProbeUnsafe, protocol.ProbeProtected} {
			for trial := 1; trial <= DevelopmentTrials; trial++ {
				plan = append(plan, RunSpec{Case: benchmarkCase, Probe: probe, Trial: trial})
			}
		}
	}
	return plan
}
