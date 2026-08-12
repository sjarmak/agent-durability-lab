package profile

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/conformance/evidence"
	legacyprotocol "github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

func TestRunCalibrationProfileProducesPassingBinaryReport(t *testing.T) {
	root := repositoryRoot(t)
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve test executable: %v", err)
	}
	evidenceRoot := filepath.Join(t.TempDir(), "suite")
	report, err := RunCalibration(context.Background(), Config{
		EvidenceRoot:   evidenceRoot,
		SourceRoot:     root,
		SchemaRoot:     filepath.Join(root, "specs/coding-agent-durability/v1/schema"),
		ExecutablePath: executable,
	})
	if err != nil {
		t.Fatalf("run calibration conformance: %v", err)
	}
	if report.Status != evidence.StatusConformant || len(report.Episodes) != 28 || len(report.InvalidControls) != 4 {
		t.Fatalf("report = status %q, episodes %d, controls %d", report.Status, len(report.Episodes), len(report.InvalidControls))
	}
	for _, episode := range report.Episodes {
		want := legacyprotocol.VerdictValidPass
		if episode.Probe == legacyprotocol.ProbeUnsafe {
			want = legacyprotocol.VerdictValidFail
		}
		if episode.Verdict != want {
			t.Errorf("%s verdict = %s, want %s", episode.RunID, episode.Verdict, want)
		}
		if episode.Replay.Captured || episode.Replay.Status != evidence.ReplayNotApplicable || episode.Replay.Explanation != evidence.CalibrationReplayExplanation {
			t.Errorf("%s replay = %+v, want explicit calibration N/A", episode.RunID, episode.Replay)
		}
	}
	for _, control := range report.InvalidControls {
		if control.Verdict != legacyprotocol.VerdictInvalid || !containsReason(control.ReasonCodes, control.ExpectedReason) {
			t.Errorf("control %s = %+v", control.ID, control)
		}
	}
	data, err := os.ReadFile(filepath.Join(evidenceRoot, evidence.ReportFile))
	if err != nil {
		t.Fatalf("read final report: %v", err)
	}
	var persisted evidence.Report
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatalf("decode final report: %v", err)
	}
	if persisted.Status != evidence.StatusConformant {
		t.Fatalf("persisted status = %q", persisted.Status)
	}
	for _, forbidden := range []string{"score", "metric", "latency", "duration", "percentile", "confidence", "rate"} {
		if strings.Contains(strings.ToLower(string(data)), forbidden) {
			t.Errorf("report contains forbidden field %q", forbidden)
		}
	}
	if _, err := RunCalibration(context.Background(), Config{
		EvidenceRoot: evidenceRoot, SourceRoot: root,
		SchemaRoot: filepath.Join(root, "specs/coding-agent-durability/v1/schema"), ExecutablePath: executable,
	}); !errors.Is(err, legacyprotocol.ErrEvidenceExists) {
		t.Fatalf("second run error = %v, want ErrEvidenceExists", err)
	}
}

func TestRunCalibrationRejectsUnsafeRootsAndPreservesCanceledRoot(t *testing.T) {
	t.Parallel()

	for _, root := range []string{"", ".", string(filepath.Separator)} {
		if _, err := RunCalibration(context.Background(), Config{EvidenceRoot: root}); !errors.Is(err, legacyprotocol.ErrInvalidEvidence) {
			t.Errorf("root %q error = %v, want ErrInvalidEvidence", root, err)
		}
	}
	repositoryRoot := repositoryRoot(t)
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve executable: %v", err)
	}
	evidenceRoot := filepath.Join(t.TempDir(), "canceled-suite")
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := RunCalibration(canceled, Config{
		EvidenceRoot: evidenceRoot, SourceRoot: repositoryRoot,
		SchemaRoot: filepath.Join(repositoryRoot, "specs/coding-agent-durability/v1/schema"), ExecutablePath: executable,
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled run error = %v", err)
	}
	if _, err := os.Stat(evidenceRoot); err != nil {
		t.Fatalf("canceled run did not preserve its claimed root: %v", err)
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	return root
}

func containsReason(reasons []string, want string) bool {
	for _, reason := range reasons {
		if reason == want {
			return true
		}
	}
	return false
}
