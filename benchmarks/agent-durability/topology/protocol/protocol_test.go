package protocol

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestFrozenPreregistrationLoadsAndPinsContract(t *testing.T) {
	registration := loadFrozenRegistration(t)
	if registration.ProtocolVersion != PublicationProtocolVersion || registration.ContractVersion != ContractVersion {
		t.Fatalf("versions = %q / %q", registration.ProtocolVersion, registration.ContractVersion)
	}
	if got := len(registration.Cases); got != 10 {
		t.Fatalf("cases = %d, want 10", got)
	}
	if registration.Population.ExpectedStrata != 88 || registration.Population.MinimumValidPairsPerStratum != 30 || registration.Population.MaximumAttemptedPairsPerStratum != 40 {
		t.Fatalf("population = %+v", registration.Population)
	}
	if err := VerifyContractHash(registration, filepath.Join("..", "..")); err != nil {
		t.Fatal(err)
	}
}

func TestFrozenPreregistrationRejectsSemanticallyValidByteDrift(t *testing.T) {
	path := filepath.Join("..", "..", "topology-preregistration-v1.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	mutated := bytes.Replace(data, []byte(`"bootstrap_resamples": 20000`), []byte(`"bootstrap_resamples": 20001`), 1)
	if bytes.Equal(mutated, data) {
		t.Fatal("test mutation did not change preregistration")
	}
	if _, err := DecodePreregistration(mutated); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("DecodePreregistration() error = %v, want invalid evidence", err)
	}
}

func TestBuildPublicationScheduleIsDeterministicBalancedAndComplete(t *testing.T) {
	registration := loadFrozenRegistration(t)
	first, err := BuildSchedule(registration, PhasePublication)
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildSchedule(registration, PhasePublication)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("same frozen seed produced different schedules")
	}
	if first.Seed != registration.Population.PublicationSeed || first.Algorithm != registration.Population.ScheduleAlgorithm {
		t.Fatalf("schedule identity = %+v", first)
	}
	wantBlocks := registration.Population.ExpectedStrata * registration.Population.MaximumAttemptedPairsPerStratum
	if len(first.Blocks) != wantBlocks {
		t.Fatalf("blocks = %d, want %d", len(first.Blocks), wantBlocks)
	}

	type counts struct {
		total, directPrimary, childPrimary, directReserve, childReserve int
	}
	byStratum := make(map[string]*counts)
	seenPairs := make(map[string]bool, len(first.Blocks))
	seenBlocks := make(map[string]bool, len(first.Blocks))
	for index, block := range first.Blocks {
		if block.Index != index+1 || block.PairID == "" || block.ScheduleBlockID == "" || seenPairs[block.PairID] || seenBlocks[block.ScheduleBlockID] {
			t.Fatalf("block %d has unstable identity: %+v", index, block)
		}
		seenPairs[block.PairID], seenBlocks[block.ScheduleBlockID] = true, true
		if err := block.Validate(); err != nil {
			t.Fatalf("block %q: %v", block.PairID, err)
		}
		value := byStratum[block.Stratum.ID]
		if value == nil {
			value = &counts{}
			byStratum[block.Stratum.ID] = value
		}
		value.total++
		if block.Reserve {
			if block.TopologyOrder[0] == TopologyDirectActivity {
				value.directReserve++
			} else {
				value.childReserve++
			}
		} else if block.TopologyOrder[0] == TopologyDirectActivity {
			value.directPrimary++
		} else {
			value.childPrimary++
		}
	}
	if len(byStratum) != registration.Population.ExpectedStrata {
		t.Fatalf("strata = %d, want %d", len(byStratum), registration.Population.ExpectedStrata)
	}
	for id, value := range byStratum {
		if value.total != 40 || value.directPrimary != 15 || value.childPrimary != 15 || value.directReserve != 5 || value.childReserve != 5 {
			t.Errorf("stratum %q counts = %+v", id, value)
		}
	}
}

func TestBuildPilotScheduleUsesFrozenPilotSeedAndAllStrata(t *testing.T) {
	registration := loadFrozenRegistration(t)
	schedule, err := BuildSchedule(registration, PhasePilot)
	if err != nil {
		t.Fatal(err)
	}
	if schedule.Seed != registration.Population.PilotSeed {
		t.Fatalf("seed = %d, want %d", schedule.Seed, registration.Population.PilotSeed)
	}
	want := registration.Population.ExpectedStrata * registration.Population.PilotPairsPerStratum
	if len(schedule.Blocks) != want {
		t.Fatalf("blocks = %d, want %d", len(schedule.Blocks), want)
	}
	firstByStratum := make(map[string]map[Topology]int)
	firstTotals := make(map[Topology]int)
	for _, block := range schedule.Blocks {
		if block.Reserve {
			t.Fatalf("pilot block marked reserve: %+v", block)
		}
		if firstByStratum[block.Stratum.ID] == nil {
			firstByStratum[block.Stratum.ID] = make(map[Topology]int)
		}
		firstByStratum[block.Stratum.ID][block.TopologyOrder[0]]++
		firstTotals[block.TopologyOrder[0]]++
	}
	for id, firstCounts := range firstByStratum {
		if firstCounts[TopologyDirectActivity] == 0 || firstCounts[TopologyChildWorkflow] == 0 {
			t.Errorf("stratum %q pilot order is not balanced within one: %v", id, firstCounts)
		}
	}
	if firstTotals[TopologyDirectActivity] != firstTotals[TopologyChildWorkflow] {
		t.Fatalf("pilot first-arm totals are globally imbalanced: %v", firstTotals)
	}
}

func TestScheduleValidationRejectsRecordedDrift(t *testing.T) {
	registration := loadFrozenRegistration(t)
	schedule, err := BuildSchedule(registration, PhasePilot)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateSchedule(registration, PhasePilot, schedule); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*Schedule)
	}{
		{name: "seed", mutate: func(value *Schedule) { value.Seed++ }},
		{name: "arm order", mutate: func(value *Schedule) {
			value.Blocks[0].TopologyOrder[0], value.Blocks[0].TopologyOrder[1] = value.Blocks[0].TopologyOrder[1], value.Blocks[0].TopologyOrder[0]
		}},
		{name: "stratum order", mutate: func(value *Schedule) { value.Blocks[0], value.Blocks[1] = value.Blocks[1], value.Blocks[0] }},
		{name: "pair identity", mutate: func(value *Schedule) { value.Blocks[0].PairID += "-changed" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := schedule.Clone()
			test.mutate(&candidate)
			if err := ValidateSchedule(registration, PhasePilot, candidate); !errors.Is(err, ErrInvalidEvidence) {
				t.Fatalf("ValidateSchedule() error = %v", err)
			}
		})
	}
}

func TestPreregistrationValidationRejectsSemanticDrift(t *testing.T) {
	registration := loadFrozenRegistration(t)
	tests := []struct {
		name   string
		mutate func(*Preregistration)
	}{
		{name: "arm", mutate: func(value *Preregistration) { value.Arms[0] = "different" }},
		{name: "fanout", mutate: func(value *Preregistration) { value.ScalePolicy.FanoutSizes[0] = 4 }},
		{name: "outcome exclusion", mutate: func(value *Preregistration) { value.Admission.OutcomeBasedExclusion = "allowed" }},
		{name: "replay", mutate: func(value *Preregistration) { value.Diagnosability.ReplayRequired = false }},
		{name: "schedule", mutate: func(value *Preregistration) { value.Population.ScheduleAlgorithm = "shuffle-later" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := registration.Clone()
			test.mutate(&candidate)
			if err := candidate.Validate(); !errors.Is(err, ErrInvalidEvidence) {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
}

func TestMetricsForCaseReturnsIndependentFrozenMetricShapes(t *testing.T) {
	for _, benchmarkCase := range Cases() {
		first := MetricsForCase(benchmarkCase)
		second := MetricsForCase(benchmarkCase)
		if len(first) == 0 || len(first) != len(second) {
			t.Fatalf("metric shape for %s = %v", benchmarkCase, first)
		}
		first[0].Name = "mutated"
		if second[0].Name == "mutated" {
			t.Fatalf("metric shape for %s aliases prior result", benchmarkCase)
		}
	}
}

func loadFrozenRegistration(t *testing.T) Preregistration {
	t.Helper()
	registration, err := LoadPreregistration(filepath.Join("..", "..", "topology-preregistration-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	return registration
}
