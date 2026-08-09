package publication

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
)

const (
	PublicationScheduleFile            = "publication-schedule.json"
	PublicationProgressFile            = "publication-progress.jsonl"
	PublicationPopulationFile          = "publication-population.json"
	PublicationPopulationInventoryFile = "publication-population-inventory.json"
)

type PairDisposition string

const (
	DispositionExecuted           PairDisposition = "executed"
	DispositionRecoveredExisting  PairDisposition = "recovered-existing"
	DispositionPartialInvalid     PairDisposition = "partial-evidence-invalid"
	DispositionReserveNotRequired PairDisposition = "reserve-not-required"
)

type PopulationRecord struct {
	ExecutionOrdinal int                `json:"execution_ordinal,omitempty"`
	Block            PairBlock          `json:"block"`
	Disposition      PairDisposition    `json:"disposition"`
	Admission        protocol.Admission `json:"admission,omitempty"`
	PairDirectory    string             `json:"pair_directory,omitempty"`
	ReasonCodes      []string           `json:"reason_codes,omitempty"`
	RecordedAtUTC    string             `json:"recorded_at_utc"`
}

type StratumSummary struct {
	Case      protocol.CaseID `json:"case"`
	Probe     protocol.Probe  `json:"probe"`
	Valid     int             `json:"valid_pairs"`
	Invalid   int             `json:"invalid_pairs"`
	Attempted int             `json:"attempted_pairs"`
	Excluded  int             `json:"excluded_reserve_pairs"`
}

type PopulationExecution struct {
	ProtocolVersion string             `json:"protocol_version"`
	Phase           Phase              `json:"phase"`
	Seed            uint64             `json:"seed"`
	StartedAtUTC    string             `json:"started_at_utc"`
	FinishedAtUTC   string             `json:"finished_at_utc"`
	Records         []PopulationRecord `json:"records"`
	Strata          []StratumSummary   `json:"strata"`
}

type PopulationInventory struct {
	ProtocolVersion string            `json:"protocol_version"`
	Phase           Phase             `json:"phase"`
	SHA256          map[string]string `json:"sha256"`
}

type PopulationRunConfig struct {
	Root         string
	Runner       RunnerConfig
	Phase        Phase
	Schedule     Schedule
	Registration Preregistration
	Clock        Clock
}

func RunPopulation(ctx context.Context, config PopulationRunConfig) (PopulationExecution, error) {
	if err := validatePopulationRunConfig(config); err != nil {
		return PopulationExecution{}, err
	}
	if config.Clock == nil {
		config.Clock = wallClock{}
	}
	if err := os.MkdirAll(config.Root, 0o750); err != nil {
		return PopulationExecution{}, err
	}
	if existing, ok, err := loadCompletedPopulation(config.Root, config); err != nil {
		return PopulationExecution{}, err
	} else if ok {
		return existing, nil
	}
	if err := ensureFrozenSchedule(config.Root, config.Schedule); err != nil {
		return PopulationExecution{}, err
	}
	progress, err := openPopulationProgress(filepath.Join(config.Root, PublicationProgressFile))
	if err != nil {
		return PopulationExecution{}, err
	}
	defer progress.Close()
	existing, err := loadPopulationProgress(filepath.Join(config.Root, PublicationProgressFile))
	if err != nil {
		return PopulationExecution{}, err
	}
	execution := PopulationExecution{
		ProtocolVersion: ProtocolVersion, Phase: config.Phase, Seed: config.Schedule.Seed,
		StartedAtUTC: config.Clock.Now().UTC().Format(time.RFC3339Nano),
	}
	type key struct {
		caseID protocol.CaseID
		probe  protocol.Probe
	}
	valid := make(map[key]int)
	attempted := make(map[key]int)
	executionOrdinal := 0
	execute := func(block PairBlock) error {
		stratum := key{caseID: block.Case, probe: block.Probe}
		if prior, ok := existing[block.PairID]; ok {
			execution.Records = append(execution.Records, prior)
			if prior.Admission == protocol.AdmissionValid {
				valid[stratum]++
			}
			if prior.Disposition != DispositionReserveNotRequired {
				attempted[stratum]++
				executionOrdinal++
			}
			return nil
		}
		executionOrdinal++
		record, err := executePopulationBlock(ctx, config, block, executionOrdinal)
		if err != nil {
			return err
		}
		if err := progress.Append(record); err != nil {
			return err
		}
		execution.Records = append(execution.Records, record)
		attempted[stratum]++
		if record.Admission == protocol.AdmissionValid {
			valid[stratum]++
		}
		return nil
	}
	primaryLimit := config.Registration.Population.MinimumValidPairsPerStratum
	for _, block := range config.Schedule.Blocks {
		if config.Phase == PhasePublication && block.Slot > primaryLimit {
			continue
		}
		if err := execute(block); err != nil {
			return PopulationExecution{}, err
		}
	}
	if config.Phase == PhasePublication {
		for _, block := range config.Schedule.Blocks {
			if block.Slot <= primaryLimit {
				continue
			}
			stratum := key{caseID: block.Case, probe: block.Probe}
			if valid[stratum] < primaryLimit {
				if err := execute(block); err != nil {
					return PopulationExecution{}, err
				}
				continue
			}
			if prior, ok := existing[block.PairID]; ok {
				execution.Records = append(execution.Records, prior)
				continue
			}
			record := PopulationRecord{
				Block: block, Disposition: DispositionReserveNotRequired,
				RecordedAtUTC: config.Clock.Now().UTC().Format(time.RFC3339Nano),
			}
			if err := progress.Append(record); err != nil {
				return PopulationExecution{}, err
			}
			execution.Records = append(execution.Records, record)
		}
	}
	execution.FinishedAtUTC = config.Clock.Now().UTC().Format(time.RFC3339Nano)
	execution.Strata = summarizePopulation(config, execution.Records)
	if err := writeJSONExclusive(filepath.Join(config.Root, PublicationPopulationFile), execution); err != nil {
		return PopulationExecution{}, err
	}
	if err := writePopulationInventory(config.Root, config.Phase); err != nil {
		return PopulationExecution{}, err
	}
	return execution, nil
}

func validatePopulationRunConfig(config PopulationRunConfig) error {
	if config.Root == "" || !config.Phase.valid() || config.Schedule.ProtocolVersion != ProtocolVersion ||
		config.Schedule.Algorithm != scheduleAlgorithm || config.Runner.Root == "" || config.Runner.Phase != config.Phase ||
		config.Runner.Root != filepath.Join(config.Root, "pairs") {
		return invalid("population runner configuration")
	}
	if err := config.Registration.Validate(); err != nil {
		return err
	}
	wantSeed := config.Registration.Population.PilotSeed
	if config.Phase == PhasePublication {
		wantSeed = config.Registration.Population.PublicationSeed
	}
	if config.Schedule.Seed != wantSeed || len(config.Schedule.Blocks) == 0 {
		return invalid("population schedule seed")
	}
	return nil
}

func executePopulationBlock(ctx context.Context, config PopulationRunConfig, block PairBlock, ordinal int) (PopulationRecord, error) {
	pairDirectory := filepath.Join(config.Runner.Root, PairDirectoryName(block.PairID))
	record := PopulationRecord{
		ExecutionOrdinal: ordinal, Block: block, PairDirectory: pairDirectory,
		RecordedAtUTC: config.Clock.Now().UTC().Format(time.RFC3339Nano),
	}
	if _, err := os.Stat(pairDirectory); err == nil {
		execution, loadErr := loadPairExecution(pairDirectory)
		if loadErr == nil {
			record.Disposition = DispositionRecoveredExisting
			record.Admission, record.ReasonCodes = execution.Admission, append([]string(nil), execution.ReasonCodes...)
			return record, nil
		}
		record.Disposition = DispositionPartialInvalid
		record.Admission = protocol.AdmissionInvalid
		record.ReasonCodes = []string{"partial_pair_evidence"}
		return record, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return PopulationRecord{}, err
	}
	execution, err := RunPair(ctx, config.Runner, block)
	if err != nil {
		return PopulationRecord{}, err
	}
	record.Disposition = DispositionExecuted
	record.Admission, record.ReasonCodes = execution.Admission, append([]string(nil), execution.ReasonCodes...)
	return record, nil
}

func loadPairExecution(directory string) (PairExecution, error) {
	data, err := os.ReadFile(filepath.Join(directory, PublicationExecutionFile))
	if err != nil {
		return PairExecution{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var execution PairExecution
	if err := decoder.Decode(&execution); err != nil {
		return PairExecution{}, err
	}
	if execution.ProtocolVersion != ProtocolVersion || execution.PairID == "" ||
		(execution.Admission != protocol.AdmissionValid && execution.Admission != protocol.AdmissionInvalid) {
		return PairExecution{}, invalid("persisted pair execution")
	}
	return execution, nil
}

type populationProgressWriter struct {
	file *os.File
	enc  *json.Encoder
}

func openPopulationProgress(path string) (*populationProgressWriter, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	return &populationProgressWriter{file: file, enc: json.NewEncoder(file)}, nil
}

func (w *populationProgressWriter) Append(record PopulationRecord) error {
	if err := w.enc.Encode(record); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *populationProgressWriter) Close() error { return w.file.Close() }

func loadPopulationProgress(path string) (map[string]PopulationRecord, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]PopulationRecord{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	result := make(map[string]PopulationRecord)
	decoder := json.NewDecoder(bufio.NewReader(file))
	for {
		var record PopulationRecord
		if err := decoder.Decode(&record); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, err
		}
		if record.Block.PairID == "" || result[record.Block.PairID].Block.PairID != "" {
			return nil, invalid("duplicate or incomplete population progress")
		}
		result[record.Block.PairID] = record
	}
	return result, nil
}

func ensureFrozenSchedule(root string, schedule Schedule) error {
	path := filepath.Join(root, PublicationScheduleFile)
	if err := writeJSONExclusive(path, schedule); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrExist) {
		return err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var existing Schedule
	if err := json.Unmarshal(data, &existing); err != nil || !reflect.DeepEqual(existing, schedule) {
		return invalid("existing population schedule differs")
	}
	return nil
}

func loadCompletedPopulation(root string, config PopulationRunConfig) (PopulationExecution, bool, error) {
	data, err := os.ReadFile(filepath.Join(root, PublicationPopulationFile))
	if errors.Is(err, os.ErrNotExist) {
		return PopulationExecution{}, false, nil
	}
	if err != nil {
		return PopulationExecution{}, false, err
	}
	var execution PopulationExecution
	if err := json.Unmarshal(data, &execution); err != nil {
		return PopulationExecution{}, false, err
	}
	if execution.ProtocolVersion != ProtocolVersion || execution.Phase != config.Phase || execution.Seed != config.Schedule.Seed {
		return PopulationExecution{}, false, invalid("completed population identity")
	}
	return execution, true, nil
}

func summarizePopulation(config PopulationRunConfig, records []PopulationRecord) []StratumSummary {
	type key struct {
		caseID protocol.CaseID
		probe  protocol.Probe
	}
	summaries := make(map[key]*StratumSummary)
	for _, benchmarkCase := range config.Registration.Cases {
		for _, probe := range config.Registration.Probes {
			value := &StratumSummary{Case: benchmarkCase, Probe: probe}
			summaries[key{caseID: benchmarkCase, probe: probe}] = value
		}
	}
	for _, record := range records {
		summary := summaries[key{caseID: record.Block.Case, probe: record.Block.Probe}]
		if record.Disposition == DispositionReserveNotRequired {
			summary.Excluded++
			continue
		}
		summary.Attempted++
		if record.Admission == protocol.AdmissionValid {
			summary.Valid++
		} else {
			summary.Invalid++
		}
	}
	result := make([]StratumSummary, 0, len(summaries))
	for _, benchmarkCase := range config.Registration.Cases {
		for _, probe := range config.Registration.Probes {
			result = append(result, *summaries[key{caseID: benchmarkCase, probe: probe}])
		}
	}
	return result
}

func writePopulationInventory(root string, phase Phase) error {
	hashes := make(map[string]string)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || entry.Name() == PublicationPopulationInventoryFile {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		hash, err := fileHash(path)
		if err != nil {
			return err
		}
		hashes[filepath.ToSlash(relative)] = hash
		return nil
	})
	if err != nil {
		return err
	}
	return writeJSONExclusive(filepath.Join(root, PublicationPopulationInventoryFile), PopulationInventory{
		ProtocolVersion: ProtocolVersion, Phase: phase, SHA256: hashes,
	})
}
