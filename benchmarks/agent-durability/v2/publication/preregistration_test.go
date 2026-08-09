package publication

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
)

func TestLoadPreregistrationPinsPublicationBeforeResults(t *testing.T) {
	registration, err := LoadPreregistration(filepath.Join("..", "..", "publication-preregistration-v2.json"))
	if err != nil {
		t.Fatal(err)
	}
	if registration.ProtocolVersion != ProtocolVersion || registration.ContractVersion != protocol.ContractVersion {
		t.Fatalf("version boundary = %q / %q", registration.ProtocolVersion, registration.ContractVersion)
	}
	if registration.Population.MinimumValidPairsPerStratum != 30 || registration.Population.MaximumAttemptedPairsPerStratum != 40 {
		t.Fatalf("population = %+v", registration.Population)
	}
	if registration.Population.PublicationSeed == 0 || registration.Population.PilotSeed == 0 || registration.Population.PublicationSeed == registration.Population.PilotSeed {
		t.Fatalf("seeds = pilot %d publication %d", registration.Population.PilotSeed, registration.Population.PublicationSeed)
	}
	if registration.Population.PilotNamespace != "pilot-v2-harness-r2-pair" {
		t.Fatalf("pilot namespace = %q", registration.Population.PilotNamespace)
	}
	if registration.Analysis.BootstrapResamples < 10_000 || registration.Analysis.ConfidenceLevel != 0.95 {
		t.Fatalf("analysis = %+v", registration.Analysis)
	}
	if registration.ChangePolicy != "no-post-publication-result-changes" {
		t.Fatalf("change policy = %q", registration.ChangePolicy)
	}
	if registration.Supersedes != PreservedProtocolVersion || registration.Liveness.HealthyTaskLatencyBoundMilliseconds != 1000 {
		t.Fatalf("supersession/liveness = %q / %+v", registration.Supersedes, registration.Liveness)
	}
	for _, benchmarkCase := range protocol.Cases() {
		if len(registration.PrimaryEstimands[benchmarkCase]) == 0 {
			t.Errorf("case %q lacks primary estimands", benchmarkCase)
		}
	}
}

func TestSupersededPublicationV1RemainsReadable(t *testing.T) {
	registration, err := LoadPreregistration(filepath.Join("..", "..", "publication-preregistration-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	if registration.ProtocolVersion != PreservedProtocolVersion || registration.Liveness != (LivenessPolicy{}) || registration.Supersedes != "" {
		t.Fatalf("preserved v1 = %+v", registration)
	}
}

func TestBuildScheduleIsDeterministicBalancedAndComplete(t *testing.T) {
	registration, err := LoadPreregistration(filepath.Join("..", "..", "publication-preregistration-v2.json"))
	if err != nil {
		t.Fatal(err)
	}
	first, err := BuildSchedule(registration)
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildSchedule(registration)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("same frozen seed produced different schedules")
	}
	wantBlocks := len(protocol.Cases()) * 3 * registration.Population.MaximumAttemptedPairsPerStratum
	if len(first.Blocks) != wantBlocks {
		t.Fatalf("blocks = %d, want %d", len(first.Blocks), wantBlocks)
	}
	type stratum struct {
		benchmarkCase protocol.CaseID
		probe         protocol.Probe
	}
	totals := make(map[stratum]int)
	primaryTemporalFirst := make(map[stratum]int)
	primaryPostgresFirst := make(map[stratum]int)
	seenIDs := make(map[string]bool, len(first.Blocks))
	for index, block := range first.Blocks {
		if block.Index != index+1 || seenIDs[block.PairID] {
			t.Fatalf("block %d identity = %+v", index, block)
		}
		seenIDs[block.PairID] = true
		if len(block.SystemOrder) != 2 || block.SystemOrder[0] == block.SystemOrder[1] {
			t.Fatalf("block %s order = %v", block.PairID, block.SystemOrder)
		}
		key := stratum{benchmarkCase: block.Case, probe: block.Probe}
		totals[key]++
		if block.Slot <= registration.Population.MinimumValidPairsPerStratum {
			switch block.SystemOrder[0] {
			case SystemTemporal:
				primaryTemporalFirst[key]++
			case SystemPostgreSQL:
				primaryPostgresFirst[key]++
			default:
				t.Fatalf("unknown system in order: %v", block.SystemOrder)
			}
		}
	}
	for _, benchmarkCase := range protocol.Cases() {
		for _, probe := range []protocol.Probe{protocol.ProbeUnfaulted, protocol.ProbeUnsafe, protocol.ProbeProtected} {
			key := stratum{benchmarkCase: benchmarkCase, probe: probe}
			if totals[key] != registration.Population.MaximumAttemptedPairsPerStratum {
				t.Errorf("stratum %+v total = %d", key, totals[key])
			}
			if primaryTemporalFirst[key] != 15 || primaryPostgresFirst[key] != 15 {
				t.Errorf("stratum %+v primary balance = %d/%d", key, primaryTemporalFirst[key], primaryPostgresFirst[key])
			}
		}
	}
}

func TestBuildPilotScheduleUsesDistinctSeedAndThreePairedEpisodesPerStratum(t *testing.T) {
	registration, err := LoadPreregistration(filepath.Join("..", "..", "publication-preregistration-v2.json"))
	if err != nil {
		t.Fatal(err)
	}
	schedule, err := BuildPilotSchedule(registration)
	if err != nil {
		t.Fatal(err)
	}
	for _, block := range schedule.Blocks {
		if got, want := block.PairID[:len(registration.Population.PilotNamespace)], registration.Population.PilotNamespace; got != want {
			t.Fatalf("pilot pair namespace = %q, want prefix %q", block.PairID, want)
		}
	}
	want := len(registration.Cases) * len(registration.Probes) * registration.Population.PilotPairsPerStratum
	if schedule.Seed != registration.Population.PilotSeed || len(schedule.Blocks) != want {
		t.Fatalf("pilot schedule = %+v", schedule)
	}
	counts := make(map[string]int)
	firstCounts := make(map[string]map[string]int)
	for _, block := range schedule.Blocks {
		key := string(block.Case) + "/" + string(block.Probe)
		counts[key]++
		if block.Reserve || len(block.SystemOrder) != 2 || block.SystemOrder[1] != otherSystem(block.SystemOrder[0]) {
			t.Fatalf("invalid pilot block: %+v", block)
		}
		if firstCounts[key] == nil {
			firstCounts[key] = make(map[string]int)
		}
		firstCounts[key][block.SystemOrder[0]]++
	}
	for key, count := range counts {
		if count != 3 || firstCounts[key][SystemTemporal] == 0 || firstCounts[key][SystemPostgreSQL] == 0 {
			t.Fatalf("%s: count=%d first=%v", key, count, firstCounts[key])
		}
	}
}

func TestFrozenHashPinsMatchContractAdaptersAndPopulation(t *testing.T) {
	registration, err := LoadPreregistration(filepath.Join("..", "..", "publication-preregistration-v2.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyHashPins(registration, filepath.Join("..", "..")); err != nil {
		t.Fatal(err)
	}
}

func TestPreregistrationValidationFailsClosed(t *testing.T) {
	registration, err := LoadPreregistration(filepath.Join("..", "..", "publication-preregistration-v2.json"))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*Preregistration)
	}{
		{name: "too few pairs", mutate: func(value *Preregistration) { value.Population.MinimumValidPairsPerStratum = 3 }},
		{name: "same pilot seed", mutate: func(value *Preregistration) { value.Population.PilotSeed = value.Population.PublicationSeed }},
		{name: "missing invalid retention", mutate: func(value *Preregistration) { value.Admission.InvalidPolicy = "" }},
		{name: "weak bootstrap", mutate: func(value *Preregistration) { value.Analysis.BootstrapResamples = 100 }},
		{name: "winner posture", mutate: func(value *Preregistration) { value.Analysis.ScalarWinner = true }},
		{name: "outcome-tuned liveness bound", mutate: func(value *Preregistration) { value.Liveness.HealthyTaskLatencyBoundMilliseconds = 250 }},
		{name: "mutable policy", mutate: func(value *Preregistration) { value.ChangePolicy = "edit-after-results" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := registration.Clone()
			test.mutate(&candidate)
			if err := candidate.Validate(); !errors.Is(err, protocol.ErrInvalidEvidence) {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
}
