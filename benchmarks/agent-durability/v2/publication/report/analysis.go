package report

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/publication"
)

type AnalysisReport struct {
	ProtocolVersion           string            `json:"protocol_version"`
	Phase                     publication.Phase `json:"phase"`
	GeneratedAtUTC            string            `json:"generated_at_utc"`
	EvidenceRoot              string            `json:"evidence_root"`
	AnalyzerBinarySHA256      string            `json:"analyzer_binary_sha256"`
	PreregistrationSHA256     string            `json:"preregistration_sha256"`
	PopulationInventorySHA256 string            `json:"population_inventory_sha256"`
	AnalysisSeed              uint64            `json:"analysis_seed"`
	BootstrapResamples        int               `json:"bootstrap_resamples"`
	ConfidenceLevel           float64           `json:"confidence_level"`
	BootstrapMethod           string            `json:"bootstrap_method"`
	BinaryInterval            string            `json:"binary_interval"`
	MultiplicityPolicy        string            `json:"multiplicity_policy"`
	ScalarWinner              bool              `json:"scalar_winner"`
	Pairs                     int               `json:"valid_pairs"`
	InvalidPairs              int               `json:"invalid_pairs"`
	ExcludedReservePairs      int               `json:"excluded_reserve_pairs"`
	InventoryEntries          int               `json:"inventory_entries"`
	Strata                    []StratumAnalysis `json:"strata"`
	InterpretationLimits      []string          `json:"interpretation_limits"`
}

type StratumAnalysis struct {
	Case               protocol.CaseID  `json:"case"`
	Probe              protocol.Probe   `json:"probe"`
	Pairs              int              `json:"valid_pairs"`
	EfficiencyEligible bool             `json:"efficiency_eligible"`
	EligibilityReason  string           `json:"eligibility_reason"`
	BinaryOutcomes     []BinaryOutcome  `json:"binary_outcomes"`
	PrimaryMetrics     []MetricEstimate `json:"primary_metrics"`
	SupportingMetrics  []MetricEstimate `json:"supporting_metrics"`
}

type BinaryOutcome struct {
	SystemID  string   `json:"system_id"`
	Dimension string   `json:"dimension"`
	Successes int      `json:"successes"`
	Total     int      `json:"total"`
	Rate      float64  `json:"rate"`
	Wilson95  Interval `json:"wilson_95"`
}

type MetricEstimate struct {
	Name                      string            `json:"name"`
	Pairs                     int               `json:"pairs"`
	Temporal                  Distribution      `json:"temporal"`
	PostgreSQL                Distribution      `json:"postgresql_queue"`
	PairedDifference          Distribution      `json:"paired_difference_temporal_minus_postgresql"`
	BootstrapMedianDifference BootstrapEstimate `json:"bootstrap_median_difference_95"`
	TemporalToPostgreSQLRatio *Distribution     `json:"temporal_to_postgresql_ratio,omitempty"`
	EfficiencyEligible        bool              `json:"efficiency_eligible"`
	Interpretation            string            `json:"interpretation"`
}

type pairObservation struct {
	pairID   string
	verdicts map[string]protocol.Verdict
	metrics  map[string]PrimaryMetrics
}

var supportingMetrics = []string{
	"queue_latency_ms",
	"execution_latency_ms",
	"retry_delay_ms",
	"recovery_latency_ms",
	"end_to_end_latency_ms",
	"healthy_task_latency_ms",
	"throughput_per_second",
	"cost_units",
	"durable_record_count",
	"durable_bytes",
	"operator_intervention_count",
}

func AnalyzePopulation(root string, registration publication.Preregistration) (AnalysisReport, error) {
	if err := registration.Validate(); err != nil {
		return AnalysisReport{}, err
	}
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return AnalysisReport{}, err
	}
	inventory, inventoryHash, err := verifyPopulationInventory(absoluteRoot)
	if err != nil {
		return AnalysisReport{}, err
	}
	var population publication.PopulationExecution
	if err := readJSON(filepath.Join(absoluteRoot, publication.PublicationPopulationFile), &population); err != nil {
		return AnalysisReport{}, err
	}
	if population.ProtocolVersion != publication.ProtocolVersion || population.Phase != inventory.Phase || population.Seed == 0 {
		return AnalysisReport{}, fmt.Errorf("%w: publication population identity", protocol.ErrInvalidEvidence)
	}
	var schedule publication.Schedule
	if err := readJSON(filepath.Join(absoluteRoot, publication.PublicationScheduleFile), &schedule); err != nil {
		return AnalysisReport{}, err
	}
	if err := validateFrozenPopulationSchedule(registration, schedule, population); err != nil {
		return AnalysisReport{}, err
	}
	wantPerStratum := registration.Population.PilotPairsPerStratum
	wantExcluded := 0
	if population.Phase == publication.PhasePublication {
		wantPerStratum = registration.Population.MinimumValidPairsPerStratum
		wantExcluded = registration.Population.MaximumAttemptedPairsPerStratum - wantPerStratum
	}
	if population.Phase != publication.PhasePilot && population.Phase != publication.PhasePublication {
		return AnalysisReport{}, fmt.Errorf("%w: publication phase", protocol.ErrInvalidEvidence)
	}

	type stratumKey struct {
		caseID protocol.CaseID
		probe  protocol.Probe
	}
	validRecords := make(map[stratumKey][]publication.PopulationRecord)
	invalidPairs, excluded := 0, 0
	for _, record := range population.Records {
		key := stratumKey{caseID: record.Block.Case, probe: record.Block.Probe}
		switch record.Disposition {
		case publication.DispositionExecuted, publication.DispositionRecoveredExisting:
			if record.Admission == protocol.AdmissionValid {
				validRecords[key] = append(validRecords[key], record)
			} else {
				invalidPairs++
			}
		case publication.DispositionPartialInvalid:
			invalidPairs++
		case publication.DispositionReserveNotRequired:
			excluded++
		default:
			return AnalysisReport{}, fmt.Errorf("%w: population disposition %q", protocol.ErrInvalidEvidence, record.Disposition)
		}
	}
	if len(population.Strata) != len(registration.Cases)*len(registration.Probes) {
		return AnalysisReport{}, fmt.Errorf("%w: population stratum count", protocol.ErrInvalidEvidence)
	}
	for _, summary := range population.Strata {
		if summary.Valid != wantPerStratum || summary.Excluded != wantExcluded ||
			len(validRecords[stratumKey{caseID: summary.Case, probe: summary.Probe}]) != wantPerStratum {
			return AnalysisReport{}, fmt.Errorf("%w: incomplete stratum %s/%s", protocol.ErrInvalidEvidence, summary.Case, summary.Probe)
		}
	}

	report := AnalysisReport{
		ProtocolVersion: publication.ProtocolVersion, Phase: population.Phase, GeneratedAtUTC: time.Now().UTC().Format(time.RFC3339Nano),
		EvidenceRoot: absoluteRoot, PopulationInventorySHA256: inventoryHash,
		AnalysisSeed: registration.Analysis.AnalysisSeed, BootstrapResamples: registration.Analysis.BootstrapResamples,
		ConfidenceLevel: registration.Analysis.ConfidenceLevel, BootstrapMethod: registration.Analysis.BootstrapMethod,
		BinaryInterval: registration.Analysis.BinaryInterval, MultiplicityPolicy: registration.Analysis.MultiplicityPolicy,
		ScalarWinner: registration.Analysis.ScalarWinner, InvalidPairs: invalidPairs, ExcludedReservePairs: excluded,
		InventoryEntries: len(inventory.SHA256),
		InterpretationLimits: []string{
			"No scalar winner and no multiplicity-adjusted null-hypothesis claims are made.",
			"Unsafe-probe intervals describe negative-control mechanisms and are not efficiency comparisons.",
			"The 1000 ms healthy-task bound is an infrastructure liveness gate, not an efficiency estimand.",
			"Backlog integral is reconstructed from outage-failure and successful-retry anchors, not the legacy queue_depth oracle summary field.",
		},
	}
	executable, err := os.Executable()
	if err != nil {
		return AnalysisReport{}, err
	}
	report.AnalyzerBinarySHA256, err = protocol.FileSHA256(executable)
	if err != nil {
		return AnalysisReport{}, err
	}
	for _, benchmarkCase := range registration.Cases {
		for _, probe := range registration.Probes {
			key := stratumKey{caseID: benchmarkCase, probe: probe}
			observations, err := loadObservations(absoluteRoot, validRecords[key], registration, benchmarkCase)
			if err != nil {
				return AnalysisReport{}, err
			}
			stratum, err := analyzeStratum(benchmarkCase, probe, observations, registration, population.Phase)
			if err != nil {
				return AnalysisReport{}, err
			}
			report.Pairs += len(observations)
			report.Strata = append(report.Strata, stratum)
		}
	}
	return report, nil
}

func AnalyzePopulationFiles(root, preregistrationPath string) (AnalysisReport, error) {
	registration, err := publication.LoadPreregistration(preregistrationPath)
	if err != nil {
		return AnalysisReport{}, err
	}
	report, err := AnalyzePopulation(root, registration)
	if err != nil {
		return AnalysisReport{}, err
	}
	report.PreregistrationSHA256, err = protocol.FileSHA256(preregistrationPath)
	if err != nil {
		return AnalysisReport{}, err
	}
	return report, nil
}

func WriteAnalysis(path string, report AnalysisReport) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o640)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	encodeErr := encoder.Encode(report)
	closeErr := file.Close()
	if encodeErr != nil {
		return encodeErr
	}
	return closeErr
}

func loadObservations(root string, records []publication.PopulationRecord, registration publication.Preregistration, benchmarkCase protocol.CaseID) ([]pairObservation, error) {
	primary := registration.PrimaryEstimands[benchmarkCase]
	metrics := append([]string(nil), primary...)
	for _, name := range supportingMetrics {
		if !slices.Contains(metrics, name) {
			metrics = append(metrics, name)
		}
	}
	result := make([]pairObservation, 0, len(records))
	seen := make(map[string]bool, len(records))
	for _, record := range records {
		if seen[record.Block.PairID] {
			return nil, fmt.Errorf("%w: duplicate pair %q", protocol.ErrInvalidEvidence, record.Block.PairID)
		}
		seen[record.Block.PairID] = true
		pairDir, err := resolvePairDirectory(root, record)
		if err != nil {
			return nil, err
		}
		var pair publication.PairExecution
		if err := readJSON(filepath.Join(pairDir, publication.PublicationExecutionFile), &pair); err != nil {
			return nil, err
		}
		if pair.PairID != record.Block.PairID || pair.PairIndex != record.Block.Index || pair.Case != record.Block.Case ||
			pair.Probe != record.Block.Probe || pair.Admission != protocol.AdmissionValid || !slices.Equal(pair.SystemOrder, record.Block.SystemOrder) {
			return nil, fmt.Errorf("%w: pair execution differs from population record", protocol.ErrInvalidEvidence)
		}
		observation := pairObservation{pairID: pair.PairID, verdicts: make(map[string]protocol.Verdict, 2), metrics: make(map[string]PrimaryMetrics, 2)}
		for _, system := range pair.Systems {
			if system.SystemID != publication.SystemTemporal && system.SystemID != publication.SystemPostgreSQL {
				return nil, fmt.Errorf("%w: pair system %q", protocol.ErrInvalidEvidence, system.SystemID)
			}
			if err := system.Verdict.Validate(); err != nil {
				return nil, err
			}
			if system.Verdict.Admission != protocol.AdmissionValid || system.Verdict.Case != pair.Case || system.Verdict.Probe != pair.Probe || system.Verdict.Trial != pair.Slot {
				return nil, fmt.Errorf("%w: pair system verdict", protocol.ErrInvalidEvidence)
			}
			if _, exists := observation.verdicts[system.SystemID]; exists {
				return nil, fmt.Errorf("%w: duplicate pair system", protocol.ErrInvalidEvidence)
			}
			systemEvidenceDir, err := resolveSystemEvidenceDirectory(root, system)
			if err != nil {
				return nil, fmt.Errorf("%s %s: %w", pair.PairID, system.SystemID, err)
			}
			reconstructed, err := ReconstructPrimaryMetrics(systemEvidenceDir, metrics)
			if err != nil {
				return nil, fmt.Errorf("%s %s: %w", pair.PairID, system.SystemID, err)
			}
			observation.verdicts[system.SystemID] = system.Verdict
			observation.metrics[system.SystemID] = reconstructed
		}
		if len(observation.verdicts) != 2 {
			return nil, fmt.Errorf("%w: incomplete pair systems", protocol.ErrInvalidEvidence)
		}
		result = append(result, observation)
	}
	return result, nil
}

func validateFrozenPopulationSchedule(registration publication.Preregistration, schedule publication.Schedule, population publication.PopulationExecution) error {
	var expected publication.Schedule
	var err error
	switch population.Phase {
	case publication.PhasePilot:
		expected, err = publication.BuildPilotSchedule(registration)
	case publication.PhasePublication:
		expected, err = publication.BuildSchedule(registration)
	default:
		return fmt.Errorf("%w: population schedule phase", protocol.ErrInvalidEvidence)
	}
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(schedule, expected) || population.Seed != expected.Seed {
		return fmt.Errorf("%w: population schedule differs from preregistration", protocol.ErrInvalidEvidence)
	}

	ordered := slices.Clone(expected.Blocks)
	if population.Phase == publication.PhasePublication {
		primaryLimit := registration.Population.MinimumValidPairsPerStratum
		ordered = ordered[:0]
		for _, block := range expected.Blocks {
			if block.Slot <= primaryLimit {
				ordered = append(ordered, block)
			}
		}
		for _, block := range expected.Blocks {
			if block.Slot > primaryLimit {
				ordered = append(ordered, block)
			}
		}
	}
	if len(population.Records) != len(ordered) {
		return fmt.Errorf("%w: population record count differs from schedule", protocol.ErrInvalidEvidence)
	}
	attemptedOrdinal := 0
	seen := make(map[string]bool, len(ordered))
	for index, block := range ordered {
		record := population.Records[index]
		if seen[record.Block.PairID] || !reflect.DeepEqual(record.Block, block) {
			return fmt.Errorf("%w: population record %d differs from frozen schedule", protocol.ErrInvalidEvidence, index)
		}
		seen[record.Block.PairID] = true
		if record.Disposition == publication.DispositionReserveNotRequired {
			if record.ExecutionOrdinal != 0 {
				return fmt.Errorf("%w: excluded reserve has an execution ordinal", protocol.ErrInvalidEvidence)
			}
			continue
		}
		attemptedOrdinal++
		if record.ExecutionOrdinal != attemptedOrdinal {
			return fmt.Errorf("%w: population execution order differs from schedule", protocol.ErrInvalidEvidence)
		}
	}
	return nil
}

func resolvePairDirectory(root string, record publication.PopulationRecord) (string, error) {
	relative := filepath.Join("pairs", publication.PairDirectoryName(record.Block.PairID))
	if !pathEndsWith(record.PairDirectory, relative) {
		return "", fmt.Errorf("%w: pair directory is outside the sealed layout", protocol.ErrInvalidEvidence)
	}
	return filepath.Join(root, relative), nil
}

func resolveSystemEvidenceDirectory(root string, system publication.SystemRun) (string, error) {
	runID := system.Verdict.RunID
	if (system.SystemID != publication.SystemTemporal && system.SystemID != publication.SystemPostgreSQL) || runID == "" || filepath.Base(runID) != runID {
		return "", fmt.Errorf("%w: system evidence identity", protocol.ErrInvalidEvidence)
	}
	relative := filepath.Join("systems", system.SystemID, runID)
	if !pathEndsWith(system.EvidenceDir, relative) {
		return "", fmt.Errorf("%w: system evidence directory is outside the sealed layout", protocol.ErrInvalidEvidence)
	}
	return filepath.Join(root, relative), nil
}

func pathEndsWith(path, relative string) bool {
	if path == "" || relative == "" {
		return false
	}
	cleanPath := filepath.ToSlash(filepath.Clean(path))
	cleanRelative := filepath.ToSlash(filepath.Clean(relative))
	return cleanPath == cleanRelative || strings.HasSuffix(cleanPath, "/"+cleanRelative)
}

func analyzeStratum(benchmarkCase protocol.CaseID, probe protocol.Probe, observations []pairObservation, registration publication.Preregistration, phase publication.Phase) (StratumAnalysis, error) {
	stratum := StratumAnalysis{Case: benchmarkCase, Probe: probe, Pairs: len(observations)}
	allParity := true
	for _, observation := range observations {
		for _, systemID := range []string{publication.SystemTemporal, publication.SystemPostgreSQL} {
			if !fourOutcomePass(observation.verdicts[systemID]) {
				allParity = false
			}
		}
	}
	stratum.EfficiencyEligible = phase == publication.PhasePublication &&
		len(observations) == registration.Population.MinimumValidPairsPerStratum && allParity
	switch {
	case phase != publication.PhasePublication:
		stratum.EligibilityReason = "pilot evidence is excluded from publication efficiency"
	case len(observations) != registration.Population.MinimumValidPairsPerStratum:
		stratum.EligibilityReason = "fewer than thirty matched valid pairs"
	case !allParity:
		stratum.EligibilityReason = "at least one arm does not pass all four outcomes"
	default:
		stratum.EligibilityReason = "all thirty matched pairs are valid and both arms pass all four outcomes"
	}

	for _, systemID := range []string{publication.SystemTemporal, publication.SystemPostgreSQL} {
		for _, dimension := range []string{"correctness", "safety", "liveness", "diagnosability", "all_four_pass", "control_distinguished"} {
			successes := 0
			for _, observation := range observations {
				if outcomeSuccess(observation.verdicts[systemID], dimension) {
					successes++
				}
			}
			total := len(observations)
			rate := float64(0)
			if total > 0 {
				rate = float64(successes) / float64(total)
			}
			stratum.BinaryOutcomes = append(stratum.BinaryOutcomes, BinaryOutcome{
				SystemID: systemID, Dimension: dimension, Successes: successes, Total: total, Rate: rate, Wilson95: Wilson95(successes, total),
			})
		}
	}
	for _, name := range registration.PrimaryEstimands[benchmarkCase] {
		estimate, err := estimateMetric(benchmarkCase, probe, name, observations, registration, stratum.EfficiencyEligible)
		if err != nil {
			return StratumAnalysis{}, err
		}
		stratum.PrimaryMetrics = append(stratum.PrimaryMetrics, estimate)
	}
	for _, name := range supportingMetrics {
		if slices.Contains(registration.PrimaryEstimands[benchmarkCase], name) {
			continue
		}
		estimate, err := estimateMetric(benchmarkCase, probe, name, observations, registration, stratum.EfficiencyEligible)
		if err != nil {
			return StratumAnalysis{}, err
		}
		stratum.SupportingMetrics = append(stratum.SupportingMetrics, estimate)
	}
	return stratum, nil
}

func estimateMetric(benchmarkCase protocol.CaseID, probe protocol.Probe, name string, observations []pairObservation, registration publication.Preregistration, efficiencyEligible bool) (MetricEstimate, error) {
	temporalValues := make([]float64, len(observations))
	postgresValues := make([]float64, len(observations))
	differences := make([]float64, len(observations))
	for index, observation := range observations {
		var ok bool
		temporalValues[index], ok = observation.metrics[publication.SystemTemporal][name]
		if !ok {
			return MetricEstimate{}, fmt.Errorf("%w: missing Temporal metric %q", protocol.ErrInvalidEvidence, name)
		}
		postgresValues[index], ok = observation.metrics[publication.SystemPostgreSQL][name]
		if !ok {
			return MetricEstimate{}, fmt.Errorf("%w: missing PostgreSQL metric %q", protocol.ErrInvalidEvidence, name)
		}
		differences[index] = temporalValues[index] - postgresValues[index]
	}
	seed := metricSeed(registration.Analysis.AnalysisSeed, benchmarkCase, probe, name)
	bootstrap, err := PairedMedianDifference(temporalValues, postgresValues, registration.Analysis.BootstrapResamples, seed)
	if err != nil {
		return MetricEstimate{}, err
	}
	interpretation := "paired descriptive primary/supporting estimand"
	if !efficiencyEligible {
		interpretation = "negative-control or non-publication mechanism description; not an efficiency comparison"
	}
	return MetricEstimate{
		Name: name, Pairs: len(observations), Temporal: Summarize(temporalValues), PostgreSQL: Summarize(postgresValues),
		PairedDifference: Summarize(differences), BootstrapMedianDifference: bootstrap,
		TemporalToPostgreSQLRatio: PositivePairedRatio(temporalValues, postgresValues),
		EfficiencyEligible:        efficiencyEligible, Interpretation: interpretation,
	}, nil
}

func outcomeSuccess(verdict protocol.Verdict, dimension string) bool {
	switch dimension {
	case "correctness":
		return verdict.Correctness == protocol.OutcomePass
	case "safety":
		return verdict.Safety == protocol.OutcomePass
	case "liveness":
		return verdict.Liveness == protocol.OutcomePass
	case "diagnosability":
		return verdict.Diagnosability == protocol.OutcomePass
	case "all_four_pass":
		return fourOutcomePass(verdict)
	case "control_distinguished":
		return verdict.Correctness != protocol.OutcomePass || verdict.Safety != protocol.OutcomePass || verdict.Liveness != protocol.OutcomePass
	default:
		return false
	}
}

func fourOutcomePass(verdict protocol.Verdict) bool {
	return verdict.Correctness == protocol.OutcomePass && verdict.Safety == protocol.OutcomePass &&
		verdict.Liveness == protocol.OutcomePass && verdict.Diagnosability == protocol.OutcomePass
}

func metricSeed(seed uint64, benchmarkCase protocol.CaseID, probe protocol.Probe, metric string) uint64 {
	value := uint64(1469598103934665603)
	for _, character := range []byte(string(benchmarkCase) + "\x00" + string(probe) + "\x00" + metric) {
		value ^= uint64(character)
		value *= 1099511628211
	}
	value ^= seed
	if value == 0 {
		value = seed
	}
	return value
}

func verifyPopulationInventory(root string) (publication.PopulationInventory, string, error) {
	path := filepath.Join(root, publication.PublicationPopulationInventoryFile)
	var inventory publication.PopulationInventory
	if err := readJSON(path, &inventory); err != nil {
		return inventory, "", err
	}
	if inventory.ProtocolVersion != publication.ProtocolVersion || len(inventory.SHA256) == 0 {
		return inventory, "", fmt.Errorf("%w: population inventory identity", protocol.ErrInvalidEvidence)
	}
	seen := 0
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if relative == publication.PublicationPopulationInventoryFile {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("%w: non-regular population evidence %s", protocol.ErrInvalidEvidence, relative)
		}
		if _, ok := inventory.SHA256[relative]; !ok {
			return fmt.Errorf("%w: uninventoried population evidence %s", protocol.ErrInvalidEvidence, relative)
		}
		seen++
		return nil
	}); err != nil {
		return inventory, "", err
	}
	if seen != len(inventory.SHA256) {
		return inventory, "", fmt.Errorf("%w: incomplete population inventory", protocol.ErrInvalidEvidence)
	}
	for relative, expected := range inventory.SHA256 {
		clean := filepath.Clean(relative)
		if filepath.IsAbs(clean) || clean == "." || clean == ".." || len(clean) >= 3 && clean[:3] == ".."+string(filepath.Separator) {
			return inventory, "", fmt.Errorf("%w: unsafe inventory path %q", protocol.ErrInvalidEvidence, relative)
		}
		actual, err := protocol.FileSHA256(filepath.Join(root, clean))
		if err != nil {
			return inventory, "", err
		}
		if actual != expected {
			return inventory, "", fmt.Errorf("%w: population inventory mismatch for %s", protocol.ErrInvalidEvidence, relative)
		}
	}
	hash, err := protocol.FileSHA256(path)
	return inventory, hash, err
}
