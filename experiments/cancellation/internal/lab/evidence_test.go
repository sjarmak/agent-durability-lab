package lab

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/sjarmak/temporal_projects/internal/workstore"
	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestEvidenceWritersPublishAndReportBoundaryErrors(t *testing.T) {
	directory := t.TempDir()
	jsonPath := filepath.Join(directory, "value.json")
	if err := writeJSON(jsonPath, map[string]string{"status": "observed"}); err != nil {
		t.Fatalf("write JSON: %v", err)
	}
	if info, err := os.Stat(jsonPath); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("published JSON mode = %v, %v; want 0600", info, err)
	}
	if err := writeHistory(filepath.Join(directory, "history.json"), &historypb.History{}); err != nil {
		t.Fatalf("write history: %v", err)
	}
	if err := writeJSON(filepath.Join(directory, "invalid.json"), math.Inf(1)); err == nil {
		t.Fatal("writeJSON accepted an unsupported value")
	}
	if err := writeFileAtomically(filepath.Join(directory, "missing", "value"), []byte("x")); err == nil {
		t.Fatal("writeFileAtomically accepted a missing parent directory")
	}
	if got := moduleVersion("example.invalid/not-a-module"); got != "unknown" {
		t.Fatalf("unknown module version = %q", got)
	}
}

func TestPreservedFinalCancellationEvidence(t *testing.T) {
	root := cancellationRepositoryRoot(t)
	manifests, err := filepath.Glob(filepath.Join(
		root, "experiments", "cancellation", "evidence", "cancellation-20260807-v2-*", "manifest.json",
	))
	if err != nil {
		t.Fatalf("glob final evidence: %v", err)
	}
	const expectedRuns = 4 * 2 * 3
	if len(manifests) != expectedRuns {
		t.Fatalf("final manifests = %d; want %d", len(manifests), expectedRuns)
	}
	counts := make(map[string]int)
	for _, manifestPath := range manifests {
		var manifest Manifest
		readJSON(t, manifestPath, &manifest)
		key := string(manifest.Scenario) + "/wait=" + map[bool]string{false: "false", true: "true"}[manifest.WaitForCancellation]
		counts[key]++

		runDirectory := filepath.Dir(manifestPath)
		var snapshot workstore.Snapshot
		readJSON(t, filepath.Join(runDirectory, "application-state.json"), &snapshot)
		verdict := Verify(manifest.Scenario, snapshot)
		if !verdict.RunValid || !verdict.ExpectedObservation {
			t.Errorf("%s application evidence failed: %+v", runDirectory, verdict)
		}

		historyData, err := os.ReadFile(filepath.Join(runDirectory, "temporal-history.json"))
		if err != nil {
			t.Errorf("read %s history: %v", runDirectory, err)
			continue
		}
		var history historypb.History
		if err := protojson.Unmarshal(historyData, &history); err != nil {
			t.Errorf("decode %s history: %v", runDirectory, err)
			continue
		}
		if _, failures := VerifyHistory(manifest.Scenario, manifest.WaitForCancellation, &history); len(failures) > 0 {
			t.Errorf("%s Temporal history failed: %v", runDirectory, failures)
		}
	}
	for _, scenario := range []Scenario{
		ScenarioTemporalControl, ScenarioHealthySafe, ScenarioWorkerDeathSafe, ScenarioFrozenSafe,
	} {
		for _, wait := range []bool{false, true} {
			key := string(scenario) + "/wait=" + map[bool]string{false: "false", true: "true"}[wait]
			if counts[key] != 3 {
				t.Errorf("%s trials = %d; want 3", key, counts[key])
			}
		}
	}
}

func readJSON(t *testing.T, path string, target any) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer file.Close()
	if err := json.NewDecoder(file).Decode(target); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
}

func cancellationRepositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate cancellation evidence test")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", ".."))
}
