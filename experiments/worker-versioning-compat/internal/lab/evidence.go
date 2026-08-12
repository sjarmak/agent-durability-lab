package lab

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"unicode/utf8"

	"go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	evidenceSchema       = "agent-session-worker-versioning-evidence-v1"
	maxJSONDocumentBytes = 16 << 20
	maxJSONDepth         = 64
	maxJSONItems         = 10_000
)

type ManifestEntry struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}

type EvidenceManifest struct {
	Schema  string          `json:"schema"`
	Entries []ManifestEntry `json:"entries"`
}

func preserveExperiment(root string, result ExperimentResult) error {
	for _, scenario := range result.Scenarios {
		prefix := evidencePrefix(scenario.Scenario, scenario.Trial)
		historyBytes, err := protojson.MarshalOptions{Indent: "  ", UseProtoNames: true}.Marshal(scenario.History)
		if err != nil {
			return fmt.Errorf("encode %s history: %w", scenario.Scenario, err)
		}
		if err := writeExclusive(filepath.Join(root, prefix+"-history.json"), append(historyBytes, '\n')); err != nil {
			return err
		}
		if err := writeJSONExclusive(filepath.Join(root, prefix+"-registry.json"), scenario.Registry); err != nil {
			return err
		}
		if err := writeJSONExclusive(filepath.Join(root, prefix+"-result.json"), scenario); err != nil {
			return err
		}
	}
	if err := writeJSONExclusive(filepath.Join(root, "report.json"), result); err != nil {
		return err
	}
	entries, err := inventory(root, false)
	if err != nil {
		return err
	}
	return writeJSONExclusive(filepath.Join(root, "manifest.json"), EvidenceManifest{Schema: evidenceSchema, Entries: entries})
}

func preserveFailure(root string, runErr error) {
	_ = writeJSONExclusive(filepath.Join(root, "failure.json"), struct {
		Schema string `json:"schema"`
		Error  string `json:"error"`
	}{Schema: evidenceSchema, Error: runErr.Error()})
}

func AuditEvidence(root string) (ExperimentResult, error) {
	rootInfo, err := os.Lstat(root)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return ExperimentResult{}, errors.New("evidence root must be a real directory")
	}
	manifestData, err := readRegular(filepath.Join(root, "manifest.json"))
	if err != nil {
		return ExperimentResult{}, err
	}
	var manifest EvidenceManifest
	if err := decodeStrictJSON(manifestData, &manifest); err != nil {
		return ExperimentResult{}, fmt.Errorf("decode manifest: %w", err)
	}
	if manifest.Schema != evidenceSchema {
		return ExperimentResult{}, fmt.Errorf("manifest schema = %q", manifest.Schema)
	}
	actualEntries, err := inventory(root, true)
	if err != nil {
		return ExperimentResult{}, err
	}
	if !equalEntries(manifest.Entries, actualEntries) {
		return ExperimentResult{}, errors.New("evidence inventory or digest differs from manifest")
	}
	if err := validateEvidencePaths(actualEntries); err != nil {
		return ExperimentResult{}, err
	}

	reportData, err := readRegular(filepath.Join(root, "report.json"))
	if err != nil {
		return ExperimentResult{}, err
	}
	var report ExperimentResult
	if err := decodeStrictJSON(reportData, &report); err != nil {
		return ExperimentResult{}, fmt.Errorf("decode report: %w", err)
	}
	if len(report.Scenarios) != 3*trialsPerScenario || !report.CompatibleHistoriesReplay || !report.IncompatibleWorkflowRejected {
		return ExperimentResult{}, errors.New("report does not contain the exact conformant scenario set")
	}
	if err := validateEvidenceEnvironment(report.Environment, filepath.Base(root)); err != nil {
		return ExperimentResult{}, err
	}
	index := 0
	for _, expected := range []Scenario{ScenarioAutoCompatible, ScenarioPinnedCompatible, ScenarioAutoIncompatible} {
		for trial := 1; trial <= trialsPerScenario; trial++ {
			actual := report.Scenarios[index]
			if actual.Scenario != expected || actual.Trial != trial {
				return ExperimentResult{}, fmt.Errorf("scenario %d = %q trial %d, want %q trial %d", index, actual.Scenario, actual.Trial, expected, trial)
			}
			index++
		}
	}
	for index := range report.Scenarios {
		scenario := &report.Scenarios[index]
		if err := validateScenarioVerdict(*scenario); err != nil {
			return ExperimentResult{}, err
		}
		prefix := evidencePrefix(scenario.Scenario, scenario.Trial)
		resultData, err := readRegular(filepath.Join(root, prefix+"-result.json"))
		if err != nil {
			return ExperimentResult{}, err
		}
		var storedResult ScenarioResult
		if err := decodeStrictJSON(resultData, &storedResult); err != nil {
			return ExperimentResult{}, err
		}
		if !equalJSON(storedResult, *scenario) {
			return ExperimentResult{}, fmt.Errorf("%s result views disagree", scenario.Scenario)
		}
		registryData, err := readRegular(filepath.Join(root, prefix+"-registry.json"))
		if err != nil {
			return ExperimentResult{}, err
		}
		var exported AgentRecord
		if err := decodeStrictJSON(registryData, &exported); err != nil {
			return ExperimentResult{}, err
		}
		durable, err := (Registry{Path: filepath.Join(root, prefix+"-registry.db")}).Read()
		if err != nil {
			return ExperimentResult{}, err
		}
		if !equalJSON(exported, durable) || !equalJSON(scenario.Registry, durable) {
			return ExperimentResult{}, fmt.Errorf("%s registry views disagree", scenario.Scenario)
		}
		historyData, err := readRegular(filepath.Join(root, prefix+"-history.json"))
		if err != nil {
			return ExperimentResult{}, err
		}
		historyValue := &history.History{}
		if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(historyData, historyValue); err != nil {
			return ExperimentResult{}, fmt.Errorf("decode %s history: %w", scenario.Scenario, err)
		}
		if err := replayCompatible(historyValue); err != nil {
			return ExperimentResult{}, fmt.Errorf("replay %s: %w", scenario.Scenario, err)
		}
		if err := validateScenario(scenario.Scenario, scenario.WorkflowResult, durable); err != nil {
			return ExperimentResult{}, err
		}
		builds := historyWorkerBuilds(historyValue)
		if err := validateHistoryBuilds(scenario.Scenario, builds); err != nil {
			return ExperimentResult{}, err
		}
		if !equalJSON(builds, scenario.HistoryWorkerBuilds) {
			return ExperimentResult{}, fmt.Errorf("%s stored history build summary differs", scenario.Scenario)
		}
		registryPath, err := inspectWorkflowStart(historyValue, scenario.Scenario, scenario.Trial, report.Environment.RunLabel, scenario.WorkflowID, scenario.RunID)
		if err != nil {
			return ExperimentResult{}, fmt.Errorf("inspect %s Workflow start: %w", scenario.Scenario, err)
		}
		receipts, err := inspectActivityHistory(historyValue, scenario.Scenario, scenario.WorkflowID, scenario.RunID, registryPath, scenario.ExpectedFailure)
		if err != nil {
			return ExperimentResult{}, fmt.Errorf("inspect %s Activity history: %w", scenario.Scenario, err)
		}
		if !equalJSON(receipts, scenario.ActivityReceipts) {
			return ExperimentResult{}, fmt.Errorf("%s stored Activity receipts differ from history", scenario.Scenario)
		}
		if !scenario.ExpectedFailure && !equalJSON(receipts, scenario.WorkflowResult.Receipts) {
			return ExperimentResult{}, fmt.Errorf("%s Workflow result receipts differ from history", scenario.Scenario)
		}
		terminalResult, err := inspectWorkflowTerminal(historyValue, scenario.ExpectedFailure)
		if err != nil {
			return ExperimentResult{}, fmt.Errorf("inspect %s Workflow terminal: %w", scenario.Scenario, err)
		}
		if !equalJSON(terminalResult, scenario.WorkflowResult) {
			return ExperimentResult{}, fmt.Errorf("%s stored Workflow result differs from history", scenario.Scenario)
		}
		if scenario.ExpectedFailure {
			if len(receipts) != 1 || len(durable.Attachments) != 0 || durable.SessionID != receipts[0].SessionID || durable.StartedByWorker != receipts[0].WorkerBuild || durable.AgentBuild != receipts[0].AgentBuild {
				return ExperimentResult{}, fmt.Errorf("%s rejected registry differs from phase-one history", scenario.Scenario)
			}
		} else if err := validateRegistryReceipts(durable, receipts); err != nil {
			return ExperimentResult{}, fmt.Errorf("%s registry/history binding: %w", scenario.Scenario, err)
		}
		scenario.History = historyValue
	}
	if err := replayIncompatible(report.Scenarios[0].History); err == nil {
		return ExperimentResult{}, errors.New("incompatible Workflow replay was accepted")
	}
	return report, nil
}

func validateEvidenceEnvironment(environment Environment, runLabel string) error {
	executableDigest, err := hex.DecodeString(environment.ExecutableSHA256)
	if environment.CapturedAt.IsZero() || environment.GoVersion == "" || environment.SDKVersion == "" || environment.TemporalCLI == "" || environment.RunLabel != runLabel || environment.OS == "" || environment.Architecture == "" || err != nil || len(executableDigest) != sha256.Size {
		return errors.New("report lacks source/runtime provenance")
	}
	return nil
}

func validateScenarioVerdict(result ScenarioResult) error {
	expectedFailure := result.Scenario == ScenarioAutoIncompatible
	if result.ExpectedFailure != expectedFailure || result.IncompatibleRejected != expectedFailure {
		return fmt.Errorf("%s rejection verdict is inconsistent", result.Scenario)
	}
	return nil
}

func validateEvidencePaths(entries []ManifestEntry) error {
	want := make(map[string]struct{}, 1+3*trialsPerScenario*4)
	want["report.json"] = struct{}{}
	for _, scenario := range []Scenario{ScenarioAutoCompatible, ScenarioPinnedCompatible, ScenarioAutoIncompatible} {
		for trial := 1; trial <= trialsPerScenario; trial++ {
			prefix := evidencePrefix(scenario, trial)
			for _, suffix := range []string{"-history.json", "-registry.db", "-registry.json", "-result.json"} {
				want[prefix+suffix] = struct{}{}
			}
		}
	}
	if len(entries) != len(want) {
		return fmt.Errorf("evidence inventory has %d entries, want %d", len(entries), len(want))
	}
	for _, entry := range entries {
		if _, ok := want[entry.Path]; !ok {
			return fmt.Errorf("unexpected evidence entry %q", entry.Path)
		}
	}
	return nil
}

func inventory(root string, excludeManifest bool) ([]ManifestEntry, error) {
	directoryEntries, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("read evidence root: %w", err)
	}
	entries := make([]ManifestEntry, 0, len(directoryEntries))
	for _, directoryEntry := range directoryEntries {
		if excludeManifest && directoryEntry.Name() == "manifest.json" {
			continue
		}
		info, err := directoryEntry.Info()
		if err != nil || !info.Mode().IsRegular() {
			return nil, fmt.Errorf("evidence entry %q is not a regular file", directoryEntry.Name())
		}
		data, err := readRegular(filepath.Join(root, directoryEntry.Name()))
		if err != nil {
			return nil, err
		}
		digest := sha256.Sum256(data)
		entries = append(entries, ManifestEntry{Path: directoryEntry.Name(), SHA256: hex.EncodeToString(digest[:]), Bytes: int64(len(data))})
	}
	sort.Slice(entries, func(left, right int) bool { return entries[left].Path < entries[right].Path })
	return entries, nil
}

func writeJSONExclusive(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode %s: %w", filepath.Base(path), err)
	}
	return writeExclusive(path, append(data, '\n'))
}

func writeExclusive(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create %s: %w", filepath.Base(path), err)
	}
	if _, err = file.Write(data); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}
	if closeErr != nil {
		return fmt.Errorf("close %s: %w", filepath.Base(path), closeErr)
	}
	return nil
}

func readRegular(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect %s: %w", filepath.Base(path), err)
	}
	if !info.Mode().IsRegular() || info.Size() > maxJSONDocumentBytes {
		return nil, fmt.Errorf("%s is not a bounded regular file", filepath.Base(path))
	}
	return os.ReadFile(path)
}

func decodeStrictJSON(data []byte, target any) error {
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return requireJSONEOF(decoder)
}

func rejectDuplicateJSONKeys(data []byte) error {
	if len(data) > maxJSONDocumentBytes || !utf8.Valid(data) {
		return errors.New("JSON document is not bounded UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := scanJSONValue(decoder, 1); err != nil {
		return err
	}
	return requireJSONEOF(decoder)
}

func scanJSONValue(decoder *json.Decoder, depth int) error {
	if depth > maxJSONDepth {
		return errors.New("JSON nesting exceeds limit")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	seen := map[string]struct{}{}
	items := 0
	for decoder.More() {
		items++
		if items > maxJSONItems {
			return errors.New("JSON collection exceeds limit")
		}
		if delimiter == '{' {
			keyToken, keyErr := decoder.Token()
			if keyErr != nil {
				return keyErr
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("JSON object key is not a string")
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("duplicate JSON key %q", key)
			}
			seen[key] = struct{}{}
		}
		if err := scanJSONValue(decoder, depth+1); err != nil {
			return err
		}
	}
	if delimiter != '{' && delimiter != '[' {
		return errors.New("unexpected JSON delimiter")
	}
	_, err = decoder.Token()
	return err
}

func requireJSONEOF(decoder *json.Decoder) error {
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("unexpected trailing JSON value")
		}
		return err
	}
	return nil
}

func equalEntries(left, right []ManifestEntry) bool {
	return equalJSON(left, right)
}

func equalJSON(left, right any) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}
