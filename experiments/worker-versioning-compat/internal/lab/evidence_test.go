package lab

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestStrictJSONRejectsMalformedBoundaries(t *testing.T) {
	for name, data := range map[string][]byte{
		"duplicate":    []byte(`{"schema":"one","schema":"two"}`),
		"trailing":     []byte(`{} {}`),
		"invalid-utf8": {0xff},
		"deep":         []byte(strings.Repeat("[", maxJSONDepth+1) + strings.Repeat("]", maxJSONDepth+1)),
	} {
		t.Run(name, func(t *testing.T) {
			var target map[string]any
			if err := decodeStrictJSON(data, &target); err == nil {
				t.Fatal("invalid JSON accepted")
			}
		})
	}
	if err := decodeStrictJSON([]byte(`{"unknown":true}`), &EvidenceManifest{}); err == nil {
		t.Fatal("unknown field accepted")
	}
}

func TestEvidenceInventoryIsExactAndRegular(t *testing.T) {
	entries := expectedEvidenceEntries()
	if err := validateEvidencePaths(entries); err != nil {
		t.Fatal(err)
	}
	for name, mutated := range map[string][]ManifestEntry{
		"extra":   append(append([]ManifestEntry{}, entries...), ManifestEntry{Path: "extra.json"}),
		"missing": entries[:len(entries)-1],
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateEvidencePaths(mutated); err == nil {
				t.Fatal("wrong inventory accepted")
			}
		})
	}
	root := t.TempDir()
	if err := os.Symlink("outside", filepath.Join(root, "report.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := inventory(root, false); err == nil {
		t.Fatal("symlinked evidence entry accepted")
	}
}

func TestRegistryDecoderRejectsUnknownDuplicateAndTrailingData(t *testing.T) {
	valid := AgentRecord{SessionID: "session-1", AgentBuild: "agent-v1", StartedByWorker: "worker-v1"}
	encoded, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	mutations := [][]byte{
		[]byte(strings.Replace(string(encoded), `"session_id":"session-1"`, `"session_id":"session-1","unknown":true`, 1)),
		[]byte(strings.Replace(string(encoded), `"session_id":"session-1"`, `"session_id":"session-1","session_id":"session-1"`, 1)),
		append(encoded, []byte(` {}`)...),
	}
	for _, mutation := range mutations {
		var decoded AgentRecord
		if err := decodeRegistry(mutation, &decoded); err == nil {
			t.Fatal("invalid registry accepted")
		}
	}
}

func TestRunExperimentRejectsInvalidOptionsAndExistingRoot(t *testing.T) {
	if _, err := RunExperiment(t.Context(), RunOptions{}); err == nil {
		t.Fatal("empty options accepted")
	}
	root := t.TempDir()
	marker := filepath.Join(root, "marker")
	if err := os.WriteFile(marker, []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := RunExperiment(t.Context(), RunOptions{Root: root}); err == nil {
		t.Fatal("existing root accepted")
	}
	data, err := os.ReadFile(marker)
	if err != nil || string(data) != "preserve" {
		t.Fatalf("existing root changed: %q, %v", data, err)
	}
}

func TestPreserveFailureIsExclusiveAndRedactsNothingBeyondError(t *testing.T) {
	root := t.TempDir()
	preserveFailure(root, errors.New("calibrated failure"))
	preserveFailure(root, errors.New("replacement"))
	data, err := os.ReadFile(filepath.Join(root, "failure.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "calibrated failure") || strings.Contains(string(data), "replacement") {
		t.Fatalf("failure evidence = %s", data)
	}
}

func TestEvidenceFileBoundariesRejectOverwriteDirectoryAndOversize(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "artifact.json")
	if err := writeExclusive(path, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := writeExclusive(path, []byte("second")); err == nil {
		t.Fatal("exclusive writer overwrote existing artifact")
	}
	if _, err := readRegular(root); err == nil {
		t.Fatal("directory accepted as regular evidence")
	}
	large := filepath.Join(root, "large.json")
	file, err := os.Create(large)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(maxJSONDocumentBytes + 1); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := readRegular(large); err == nil {
		t.Fatal("oversized evidence accepted")
	}
}

func TestScenarioAndHistoryBuildValidatorsFailClosed(t *testing.T) {
	validResult := func() WorkflowResult {
		return WorkflowResult{
			WorkflowBuilds: []string{"worker-v1", "worker-v2"},
			Receipts: []ActivityReceipt{
				{SessionID: "session-1", WorkerBuild: "worker-v1", AgentBuild: "agent-v1", Action: ActionStarted},
				{SessionID: "session-1", WorkerBuild: "worker-v2", AgentBuild: "agent-v1", Action: ActionAttached},
			},
		}
	}
	record := AgentRecord{SessionID: "session-1", AgentBuild: "agent-v1", StartedByWorker: "worker-v1", Attachments: []Attachment{{WorkerBuild: "worker-v2"}}}
	if err := validateScenario(ScenarioAutoCompatible, validResult(), record); err != nil {
		t.Fatal(err)
	}
	for name, check := range map[string]func() error{
		"agent": func() error {
			changed := record
			changed.AgentBuild = "wrong"
			return validateScenario(ScenarioAutoCompatible, validResult(), changed)
		},
		"receipts": func() error {
			changed := validResult()
			changed.Receipts = nil
			return validateScenario(ScenarioAutoCompatible, changed, record)
		},
		"workflow-build": func() error {
			changed := validResult()
			changed.WorkflowBuilds = []string{"worker-v1"}
			return validateScenario(ScenarioAutoCompatible, changed, record)
		},
		"receipt-worker": func() error {
			changed := validResult()
			changed.Receipts[1].WorkerBuild = "wrong"
			return validateScenario(ScenarioAutoCompatible, changed, record)
		},
		"history-build": func() error { return validateHistoryBuilds(ScenarioAutoCompatible, []string{"worker-v1"}) },
	} {
		t.Run(name, func(t *testing.T) {
			if err := check(); err == nil {
				t.Fatal("mutation accepted")
			}
		})
	}
}

func TestRegistryReceiptBindingRejectsEveryIdentityMismatch(t *testing.T) {
	receipts := []ActivityReceipt{
		{SessionID: "session-1", WorkerBuild: "worker-v1", AgentBuild: "agent-v1"},
		{SessionID: "session-1", WorkerBuild: "worker-v2", AgentBuild: "agent-v1"},
	}
	valid := AgentRecord{SessionID: "session-1", AgentBuild: "agent-v1", StartedByWorker: "worker-v1", Attachments: []Attachment{{WorkerBuild: "worker-v2"}}}
	if err := validateRegistryReceipts(valid, receipts); err != nil {
		t.Fatal(err)
	}
	for name, record := range map[string]AgentRecord{
		"start":      {SessionID: "session-1", AgentBuild: "agent-v1", StartedByWorker: "wrong", Attachments: []Attachment{{WorkerBuild: "worker-v2"}}},
		"count":      {SessionID: "session-1", AgentBuild: "agent-v1", StartedByWorker: "worker-v1"},
		"attachment": {SessionID: "session-1", AgentBuild: "agent-v1", StartedByWorker: "worker-v1", Attachments: []Attachment{{WorkerBuild: "wrong"}}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateRegistryReceipts(record, receipts); err == nil {
				t.Fatal("registry mismatch accepted")
			}
		})
	}
}

func TestEvidenceMetadataAndScenarioVerdictsFailClosed(t *testing.T) {
	validEnvironment := Environment{
		CapturedAt:       time.Now().UTC(),
		GoVersion:        runtime.Version(),
		SDKVersion:       "v1.47.0",
		TemporalCLI:      "1.5.1",
		ExecutableSHA256: strings.Repeat("0", 64),
		RunLabel:         "worker-versioning-test",
		OS:               runtime.GOOS,
		Architecture:     runtime.GOARCH,
	}
	if err := validateEvidenceEnvironment(validEnvironment, validEnvironment.RunLabel); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Environment){
		"missing-os":           func(environment *Environment) { environment.OS = "" },
		"missing-architecture": func(environment *Environment) { environment.Architecture = "" },
		"nonhex-executable":    func(environment *Environment) { environment.ExecutableSHA256 = strings.Repeat("z", 64) },
		"short-executable":     func(environment *Environment) { environment.ExecutableSHA256 = strings.Repeat("0", 62) },
	} {
		t.Run(name, func(t *testing.T) {
			mutated := validEnvironment
			mutate(&mutated)
			if err := validateEvidenceEnvironment(mutated, validEnvironment.RunLabel); err == nil {
				t.Fatal("invalid provenance accepted")
			}
		})
	}

	for _, scenario := range []Scenario{ScenarioAutoCompatible, ScenarioPinnedCompatible, ScenarioAutoIncompatible} {
		expectedFailure := scenario == ScenarioAutoIncompatible
		valid := ScenarioResult{
			Scenario:             scenario,
			ExpectedFailure:      expectedFailure,
			IncompatibleRejected: expectedFailure,
		}
		if err := validateScenarioVerdict(valid); err != nil {
			t.Fatalf("valid %s verdict: %v", scenario, err)
		}
		valid.IncompatibleRejected = !valid.IncompatibleRejected
		if err := validateScenarioVerdict(valid); err == nil {
			t.Fatalf("%s incompatible-rejection mutation accepted", scenario)
		}
	}
}

func expectedEvidenceEntries() []ManifestEntry {
	entries := []ManifestEntry{{Path: "report.json"}}
	for _, scenario := range []Scenario{ScenarioAutoCompatible, ScenarioPinnedCompatible, ScenarioAutoIncompatible} {
		for trial := 1; trial <= trialsPerScenario; trial++ {
			prefix := evidencePrefix(scenario, trial)
			for _, suffix := range []string{"-history.json", "-registry.db", "-registry.json", "-result.json"} {
				entries = append(entries, ManifestEntry{Path: prefix + suffix})
			}
		}
	}
	return entries
}
