package testfixture

import (
	"path/filepath"
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/oracle"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
)

func TestBundleCoversEveryFrozenStratumAndDistinguishesUnsafeControls(t *testing.T) {
	registration, err := protocol.LoadPreregistration(filepath.Join("..", "..", "..", "topology-preregistration-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	strata, err := protocol.BuildStrata(registration)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(strata), registration.Population.ExpectedStrata; got != want {
		t.Fatalf("strata = %d, want %d", got, want)
	}
	for index, stratum := range strata {
		block := protocol.PairBlock{
			Index: index + 1, ScheduleBlockID: "schedule-block/fixture/" + stratum.ID,
			PairID: "fixture/" + stratum.ID, Stratum: stratum, Slot: 1,
			TopologyOrder: protocol.Topologies(),
		}
		for _, topology := range protocol.Topologies() {
			t.Run(stratum.ID+"/"+string(topology), func(t *testing.T) {
				bundle := Bundle(block, topology)
				if err := bundle.Validate(); err != nil {
					t.Fatalf("fixture validation: %v", err)
				}
				verdict := oracle.Evaluate(bundle)
				if verdict.Admission != protocol.AdmissionValid || verdict.Liveness != protocol.OutcomePass ||
					verdict.Diagnosability != protocol.OutcomePass {
					t.Fatalf("fixture verdict = %+v", verdict)
				}
				if stratum.Probe == protocol.ProbeUnsafe {
					if verdict.Safety != protocol.OutcomeFail || verdict.EfficiencyEligible {
						t.Fatalf("unsafe fixture did not distinguish: %+v", verdict)
					}
				} else if verdict.Correctness != protocol.OutcomePass || verdict.Safety != protocol.OutcomePass || !verdict.EfficiencyEligible {
					t.Fatalf("protected/unfaulted fixture failed: %+v", verdict)
				}
			})
		}
	}
}
