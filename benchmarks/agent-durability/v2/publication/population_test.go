package publication

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
)

func TestPopulationRecoveryRetainsCompleteAndPartialPairDirectories(t *testing.T) {
	root := t.TempDir()
	runnerRoot := filepath.Join(root, "pairs")
	if err := os.MkdirAll(runnerRoot, 0o750); err != nil {
		t.Fatal(err)
	}
	block := PairBlock{
		Index: 1, PairID: "resume-complete", Case: protocol.CaseABAReacquisition,
		Probe: protocol.ProbeProtected, Slot: 1, SystemOrder: []string{SystemTemporal, SystemPostgreSQL},
	}
	completeDir := filepath.Join(runnerRoot, PairDirectoryName(block.PairID))
	if err := os.Mkdir(completeDir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := writeJSONExclusive(filepath.Join(completeDir, PublicationExecutionFile), PairExecution{
		ProtocolVersion: ProtocolVersion, Phase: PhasePilot, PairID: block.PairID, PairIndex: block.Index,
		Case: block.Case, Probe: block.Probe, Slot: block.Slot, Admission: protocol.AdmissionValid,
	}); err != nil {
		t.Fatal(err)
	}
	config := PopulationRunConfig{Runner: RunnerConfig{Root: runnerRoot}, Clock: newFakeClock()}
	record, err := executePopulationBlock(context.Background(), config, block, 1)
	if err != nil {
		t.Fatal(err)
	}
	if record.Disposition != DispositionRecoveredExisting || record.Admission != protocol.AdmissionValid {
		t.Fatalf("complete recovery = %+v", record)
	}
	partial := block
	partial.PairID = "resume-partial"
	if err := os.Mkdir(filepath.Join(runnerRoot, PairDirectoryName(partial.PairID)), 0o750); err != nil {
		t.Fatal(err)
	}
	record, err = executePopulationBlock(context.Background(), config, partial, 2)
	if err != nil {
		t.Fatal(err)
	}
	if record.Disposition != DispositionPartialInvalid || record.Admission != protocol.AdmissionInvalid || record.ReasonCodes[0] != "partial_pair_evidence" {
		t.Fatalf("partial recovery = %+v", record)
	}
}

func TestFrozenScheduleCanResumeOnlyWhenByteMeaningIsUnchanged(t *testing.T) {
	root := t.TempDir()
	schedule := Schedule{ProtocolVersion: ProtocolVersion, Seed: 1, Algorithm: scheduleAlgorithm, Blocks: []PairBlock{{
		Index: 1, PairID: "schedule-pair", Case: protocol.CaseSilentProgress, Probe: protocol.ProbeProtected,
		Slot: 1, SystemOrder: []string{SystemTemporal, SystemPostgreSQL},
	}}}
	if err := ensureFrozenSchedule(root, schedule); err != nil {
		t.Fatal(err)
	}
	if err := ensureFrozenSchedule(root, schedule); err != nil {
		t.Fatal(err)
	}
	changed := schedule
	changed.Seed = 2
	if err := ensureFrozenSchedule(root, changed); !errors.Is(err, protocol.ErrInvalidEvidence) {
		t.Fatalf("changed schedule error = %v", err)
	}
}

func TestRunPopulationConsumesReserveOnlyAfterInvalidAndResumesAppendOnly(t *testing.T) {
	registration, err := LoadPreregistration(filepath.Join("..", "..", "publication-preregistration-v2.json"))
	if err != nil {
		t.Fatal(err)
	}
	blocks := make([]PairBlock, registration.Population.MaximumAttemptedPairsPerStratum)
	for index := range blocks {
		slot := index + 1
		first := SystemTemporal
		if slot%2 == 0 {
			first = SystemPostgreSQL
		}
		blocks[index] = PairBlock{
			Index: slot, PairID: "population-test-slot-" + twoDigits(slot),
			Case: protocol.CaseLayeredRetryAmplification, Probe: protocol.ProbeProtected, Slot: slot,
			Reserve:     slot > registration.Population.MinimumValidPairsPerStratum,
			SystemOrder: []string{first, otherSystem(first)},
		}
	}
	schedule := Schedule{ProtocolVersion: ProtocolVersion, Seed: registration.Population.PublicationSeed, Algorithm: scheduleAlgorithm, Blocks: blocks}
	root := t.TempDir()
	clock := newFakeClock()
	baseTemporal := &fakeTimedExecutor{systemID: SystemTemporal, clock: clock}
	basePostgreSQL := &fakeTimedExecutor{systemID: SystemPostgreSQL, clock: clock}
	temporal := &conditionalTimedExecutor{TimedExecutor: baseTemporal, failSlot: 1}
	postgresql := &conditionalTimedExecutor{TimedExecutor: basePostgreSQL}
	runner := RunnerConfig{
		Root: filepath.Join(root, "pairs"), Phase: PhasePublication, Clock: clock,
		Executors: map[string]TimedExecutor{SystemTemporal: temporal, SystemPostgreSQL: postgresql},
	}
	config := PopulationRunConfig{
		Root: root, Runner: runner, Phase: PhasePublication, Schedule: schedule, Registration: registration, Clock: clock,
	}
	execution, err := RunPopulation(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if len(execution.Records) != 40 || len(execution.Strata) != 18 {
		t.Fatalf("population = %+v", execution)
	}
	var target StratumSummary
	for _, summary := range execution.Strata {
		if summary.Case == protocol.CaseLayeredRetryAmplification && summary.Probe == protocol.ProbeProtected {
			target = summary
		}
	}
	if target.Valid != 30 || target.Invalid != 1 || target.Attempted != 31 || target.Excluded != 9 {
		t.Fatalf("target stratum = %+v", target)
	}
	if temporal.calls+postgresql.calls != 62 {
		t.Fatalf("system executions = %d, want 62", temporal.calls+postgresql.calls)
	}
	before := temporal.calls + postgresql.calls
	recovered, err := RunPopulation(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered.Records) != len(execution.Records) || temporal.calls+postgresql.calls != before {
		t.Fatalf("resume reran work: records=%d calls=%d", len(recovered.Records), temporal.calls+postgresql.calls)
	}
	for _, name := range []string{PublicationScheduleFile, PublicationProgressFile, PublicationPopulationFile, PublicationPopulationInventoryFile} {
		if _, err := os.Stat(filepath.Join(root, name)); err != nil {
			t.Fatal(err)
		}
	}
}

type conditionalTimedExecutor struct {
	TimedExecutor
	failSlot int
	calls    int
}

func (e *conditionalTimedExecutor) ExecuteTimed(ctx context.Context, request EpisodeRequest, recorder *TimingRecorder) (TimedResult, error) {
	e.calls++
	result, err := e.TimedExecutor.ExecuteTimed(ctx, request, recorder)
	if request.Slot == e.failSlot {
		return result, errors.New("preregistered infrastructure failure")
	}
	return result, err
}

func twoDigits(value int) string {
	return fmt.Sprintf("%02d", value)
}
