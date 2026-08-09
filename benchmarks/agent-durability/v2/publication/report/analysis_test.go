package report

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/publication"
)

func TestResolveObservationPathsRelocatesLegacyPathsInsideSealedRoot(t *testing.T) {
	root := t.TempDir()
	block := publication.PairBlock{PairID: "relocated-pair"}
	pairName := publication.PairDirectoryName(block.PairID)
	record := publication.PopulationRecord{
		Block:         block,
		PairDirectory: filepath.Join(string(filepath.Separator), "old", "checkout", "population", "pairs", pairName),
	}
	pairDir, err := resolvePairDirectory(root, record)
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(root, "pairs", pairName); pairDir != want {
		t.Fatalf("pair directory = %q, want %q", pairDir, want)
	}

	runID := "observed-temporal-relocated"
	system := publication.SystemRun{
		SystemID:    publication.SystemTemporal,
		EvidenceDir: filepath.Join(string(filepath.Separator), "old", "checkout", "population", "systems", publication.SystemTemporal, runID),
		Verdict:     protocol.Verdict{RunID: runID},
	}
	systemDir, err := resolveSystemEvidenceDirectory(root, system)
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(root, "systems", publication.SystemTemporal, runID); systemDir != want {
		t.Fatalf("system directory = %q, want %q", systemDir, want)
	}
}

func TestResolveObservationPathsRejectsUnexpectedOutOfRootTargets(t *testing.T) {
	root := t.TempDir()
	record := publication.PopulationRecord{
		Block:         publication.PairBlock{PairID: "sealed-pair"},
		PairDirectory: filepath.Join(string(filepath.Separator), "tmp", "attacker-controlled"),
	}
	if _, err := resolvePairDirectory(root, record); !errors.Is(err, protocol.ErrInvalidEvidence) {
		t.Fatalf("pair path error = %v, want invalid evidence", err)
	}

	system := publication.SystemRun{
		SystemID:    publication.SystemTemporal,
		EvidenceDir: filepath.Join(string(filepath.Separator), "tmp", "attacker-controlled"),
		Verdict:     protocol.Verdict{RunID: "observed-temporal-sealed"},
	}
	if _, err := resolveSystemEvidenceDirectory(root, system); !errors.Is(err, protocol.ErrInvalidEvidence) {
		t.Fatalf("system path error = %v, want invalid evidence", err)
	}
}

func TestValidateFrozenPopulationScheduleRejectsOrderOrBlockDrift(t *testing.T) {
	registration, err := publication.LoadPreregistration(filepath.Join("..", "..", "..", "publication-preregistration-v2.json"))
	if err != nil {
		t.Fatal(err)
	}
	schedule, err := publication.BuildPilotSchedule(registration)
	if err != nil {
		t.Fatal(err)
	}
	population := publication.PopulationExecution{
		ProtocolVersion: publication.ProtocolVersion,
		Phase:           publication.PhasePilot,
		Seed:            schedule.Seed,
		Records:         make([]publication.PopulationRecord, len(schedule.Blocks)),
	}
	for index, block := range schedule.Blocks {
		population.Records[index] = publication.PopulationRecord{
			ExecutionOrdinal: index + 1,
			Block:            block,
			Disposition:      publication.DispositionExecuted,
			Admission:        protocol.AdmissionValid,
		}
	}
	if err := validateFrozenPopulationSchedule(registration, schedule, population); err != nil {
		t.Fatal(err)
	}

	driftedSchedule := schedule
	driftedSchedule.Blocks = append([]publication.PairBlock(nil), schedule.Blocks...)
	driftedSchedule.Blocks[0].SystemOrder = append([]string(nil), schedule.Blocks[0].SystemOrder...)
	driftedSchedule.Blocks[0].SystemOrder[0], driftedSchedule.Blocks[0].SystemOrder[1] =
		driftedSchedule.Blocks[0].SystemOrder[1], driftedSchedule.Blocks[0].SystemOrder[0]
	if reflect.DeepEqual(driftedSchedule, schedule) {
		t.Fatal("schedule mutation did not change the fixture")
	}
	if err := validateFrozenPopulationSchedule(registration, driftedSchedule, population); !errors.Is(err, protocol.ErrInvalidEvidence) {
		t.Fatalf("schedule drift error = %v, want invalid evidence", err)
	}

	driftedPopulation := population
	driftedPopulation.Records = append([]publication.PopulationRecord(nil), population.Records...)
	driftedPopulation.Records[0].Block = driftedSchedule.Blocks[0]
	if err := validateFrozenPopulationSchedule(registration, schedule, driftedPopulation); !errors.Is(err, protocol.ErrInvalidEvidence) {
		t.Fatalf("population drift error = %v, want invalid evidence", err)
	}
}

func TestAnalyzePilotV4ProducesPairedStrataWithoutEfficiencyAdmission(t *testing.T) {
	root := filepath.Join("..", "..", "..", "evidence", "publication-v2-pilot-20260809-v4")
	if _, err := os.Stat(root); os.IsNotExist(err) {
		t.Skip("preserved pilot v4 evidence is not present")
	}
	registration, err := publication.LoadPreregistration(filepath.Join("..", "..", "..", "publication-preregistration-v2.json"))
	if err != nil {
		t.Fatal(err)
	}
	report, err := AnalyzePopulation(root, registration)
	if err != nil {
		t.Fatal(err)
	}
	if report.Phase != publication.PhasePilot || report.Pairs != 54 || len(report.Strata) != 18 {
		t.Fatalf("report identity = phase %q pairs %d strata %d", report.Phase, report.Pairs, len(report.Strata))
	}
	if len(report.AnalyzerBinarySHA256) != 64 {
		t.Fatalf("analyzer binary hash = %q", report.AnalyzerBinarySHA256)
	}
	for _, stratum := range report.Strata {
		if stratum.Pairs != 3 || stratum.EfficiencyEligible {
			t.Fatalf("pilot stratum = %+v", stratum)
		}
		if len(stratum.PrimaryMetrics) == 0 || len(stratum.BinaryOutcomes) != 12 {
			t.Fatalf("pilot analysis incomplete = %+v", stratum)
		}
		if stratum.Case == "layered-retry-amplification" && stratum.Probe == "protected" {
			for _, metric := range stratum.SupportingMetrics {
				if metric.Name == "execution_latency_ms" && metric.Temporal.Median >= 100 {
					t.Fatalf("execution metric includes retry waits: %+v", metric)
				}
			}
		}
	}
}

func TestVerifyPopulationInventoryRejectsUninventoriedFile(t *testing.T) {
	root := t.TempDir()
	recorded := filepath.Join(root, "recorded.json")
	if err := os.WriteFile(recorded, []byte("{}\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	digest, err := protocol.FileSHA256(recorded)
	if err != nil {
		t.Fatal(err)
	}
	inventory := publication.PopulationInventory{
		ProtocolVersion: publication.ProtocolVersion, Phase: publication.PhasePilot,
		SHA256: map[string]string{"recorded.json": digest},
	}
	data, err := json.Marshal(inventory)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, publication.PublicationPopulationInventoryFile), data, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "unexpected.json"), []byte("{}\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	if _, _, err := verifyPopulationInventory(root); !errors.Is(err, protocol.ErrInvalidEvidence) {
		t.Fatalf("inventory error = %v, want invalid evidence", err)
	}
}
