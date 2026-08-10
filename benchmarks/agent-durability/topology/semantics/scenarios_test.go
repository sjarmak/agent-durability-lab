package semantics

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
)

func TestFrozenSemanticsScenariosMatchEveryRegisteredBoundaryAndProbe(t *testing.T) {
	registration, err := protocol.LoadPreregistration(filepath.Join(
		semanticsRepositoryRoot(t), "benchmarks/agent-durability/topology-preregistration-v1.json",
	))
	if err != nil {
		t.Fatal(err)
	}
	want := make(map[string]bool)
	for _, benchmarkCase := range []protocol.CaseID{
		protocol.CaseJoinBarrier,
		protocol.CaseIncrementalPartialReduction,
		protocol.CaseQueuedExecutingSupersession,
		protocol.CaseDestructiveTransition,
	} {
		want[scenarioKey(Scenario{benchmarkCase, protocol.UnfaultedBoundary, protocol.ProbeUnfaulted})] = true
		primary := registration.PrimaryBoundaryByCase[benchmarkCase]
		for _, probe := range []protocol.Probe{protocol.ProbeUnsafe, protocol.ProbeProtected} {
			want[scenarioKey(Scenario{benchmarkCase, primary, probe})] = true
			for _, boundary := range registration.SecondaryBoundaries[benchmarkCase] {
				want[scenarioKey(Scenario{benchmarkCase, boundary, probe})] = true
			}
		}
	}
	got := make(map[string]bool)
	for _, scenario := range FrozenSemanticsScenarios() {
		key := scenarioKey(scenario)
		if got[key] {
			t.Fatalf("duplicate scenario %s", key)
		}
		got[key] = true
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("scenario set differs:\n got=%v\nwant=%v", got, want)
	}
}

func TestFrozenRecoveryScenariosMatchEveryRegisteredBoundaryAndProbe(t *testing.T) {
	registration, err := protocol.LoadPreregistration(filepath.Join(
		semanticsRepositoryRoot(t), "benchmarks/agent-durability/topology-preregistration-v1.json",
	))
	if err != nil {
		t.Fatal(err)
	}
	want := make(map[string]bool)
	for _, benchmarkCase := range []protocol.CaseID{
		protocol.CaseCrashRecoveryBoundaries,
		protocol.CaseLayeredRetryAmplification,
		protocol.CaseOutageBacklogHerdRecovery,
		protocol.CaseBackpressureOverload,
		protocol.CasePoisonWorkIsolation,
		protocol.CaseSilentProgress,
	} {
		want[scenarioKey(Scenario{benchmarkCase, protocol.UnfaultedBoundary, protocol.ProbeUnfaulted})] = true
		primary := registration.PrimaryBoundaryByCase[benchmarkCase]
		for _, probe := range []protocol.Probe{protocol.ProbeUnsafe, protocol.ProbeProtected} {
			want[scenarioKey(Scenario{benchmarkCase, primary, probe})] = true
			for _, boundary := range registration.SecondaryBoundaries[benchmarkCase] {
				want[scenarioKey(Scenario{benchmarkCase, boundary, probe})] = true
			}
		}
	}
	got := make(map[string]bool)
	for _, scenario := range FrozenRecoveryScenarios() {
		key := scenarioKey(scenario)
		if got[key] {
			t.Fatalf("duplicate recovery scenario %s", key)
		}
		got[key] = true
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("recovery scenario set differs:\n got=%v\nwant=%v", got, want)
	}
}

func TestRunConformanceRejectsInvalidConfiguration(t *testing.T) {
	if _, err := RunConformance(context.Background(), nil, "", 7, 0); err == nil {
		t.Fatal("invalid conformance configuration was accepted")
	}
	if _, err := RunRecoveryConformance(context.Background(), nil, "", 8, 1); err == nil {
		t.Fatal("noncanonical recovery conformance fanout was accepted")
	}
}

func scenarioKey(scenario Scenario) string {
	return fmt.Sprintf("%s/%s/%s", scenario.Case, scenario.Boundary, scenario.Probe)
}
