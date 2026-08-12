package evidence

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/calibration"
	legacyoracle "github.com/sjarmak/temporal_projects/benchmarks/agent-durability/oracle"
	legacyprotocol "github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

func TestInvalidControlsArePublishedWithUniqueIdentityAndRejectedForIntendedReason(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	sourceRoot := filepath.Join(root, "sources")
	invalidRoot := filepath.Join(root, "invalid-controls")
	if err := os.Mkdir(invalidRoot, 0o750); err != nil {
		t.Fatalf("create invalid root: %v", err)
	}
	seenRunIDs := map[string]bool{}
	wantReasons := map[string]string{
		"malformed":              legacyprotocol.ReasonEvidenceMalformed,
		"missed-boundary":        legacyprotocol.ReasonFaultNotBracketed,
		"wrong-process-identity": legacyprotocol.ReasonWrongProcessIdentity,
		"contradiction":          legacyprotocol.ReasonEvidenceInconsistent,
	}
	for _, spec := range InvalidControlSpecs() {
		wantReason, found := wantReasons[spec.ID]
		if !found || spec.ExpectedReason != wantReason {
			t.Fatalf("control %q expected reason = %q, want %q", spec.ID, spec.ExpectedReason, wantReason)
		}
		sourceDir, err := calibration.Run(context.Background(), calibration.Config{
			Root: sourceRoot, Case: spec.SourceCase, Probe: legacyprotocol.ProbeProtected, Trial: 1,
		})
		if err != nil {
			if !errors.Is(err, legacyprotocol.ErrEvidenceExists) {
				t.Fatalf("create source for %s: %v", spec.ID, err)
			}
			sourceDir = filepath.Join(sourceRoot, string(spec.SourceCase)+"-protected-trial-1")
		}
		controlDir, err := WriteInvalidControl(context.Background(), invalidRoot, spec, sourceDir)
		if err != nil {
			t.Fatalf("write %s: %v", spec.ID, err)
		}
		manifest := readControlJSON[legacyprotocol.Manifest](t, filepath.Join(controlDir, legacyprotocol.ManifestFile))
		if manifest.RunID != "invalid-control-"+spec.ID || seenRunIDs[manifest.RunID] {
			t.Fatalf("%s manifest run ID = %q, seen=%v", spec.ID, manifest.RunID, seenRunIDs[manifest.RunID])
		}
		seenRunIDs[manifest.RunID] = true
		if _, err := os.Stat(filepath.Join(controlDir, legacyprotocol.VerdictFile)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s publisher wrote oracle verdict: %v", spec.ID, err)
		}
		changedPayloads := 0
		for _, name := range legacyprotocol.RawEvidenceFiles()[1:] {
			sourceData, sourceErr := os.ReadFile(filepath.Join(sourceDir, name))
			controlData, controlErr := os.ReadFile(filepath.Join(controlDir, name))
			if sourceErr != nil || controlErr != nil {
				t.Fatalf("compare %s/%s: source=%v control=%v", spec.ID, name, sourceErr, controlErr)
			}
			if !bytes.Equal(sourceData, controlData) {
				changedPayloads++
			}
		}
		if changedPayloads != 1 {
			t.Fatalf("%s changed %d non-manifest payloads, want exactly 1", spec.ID, changedPayloads)
		}
		verdict, err := legacyoracle.EvaluateAndWrite(context.Background(), controlDir)
		if err != nil {
			t.Fatalf("evaluate %s: %v", spec.ID, err)
		}
		if verdict.Class != legacyprotocol.VerdictInvalid || !hasReason(verdict.ReasonCodes, spec.ExpectedReason) {
			t.Fatalf("%s verdict = %+v, want invalid with %s", spec.ID, verdict, spec.ExpectedReason)
		}
		if _, err := WriteInvalidControl(context.Background(), invalidRoot, spec, sourceDir); !errors.Is(err, legacyprotocol.ErrEvidenceExists) {
			t.Fatalf("rewrite %s error = %v, want ErrEvidenceExists", spec.ID, err)
		}
	}
}

func TestInvalidControlPublisherRejectsUnknownControlAndCancellation(t *testing.T) {
	t.Parallel()

	if _, err := WriteInvalidControl(context.Background(), t.TempDir(), InvalidControlSpec{ID: "../escape"}, t.TempDir()); !errors.Is(err, legacyprotocol.ErrInvalidEvidence) {
		t.Fatalf("unknown control error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := WriteInvalidControl(canceled, t.TempDir(), InvalidControlSpecs()[0], t.TempDir()); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled control error = %v", err)
	}
}

func readControlJSON[T any](t *testing.T, path string) T {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var value T
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return value
}

func hasReason(reasons []string, want string) bool {
	for _, reason := range reasons {
		if reason == want {
			return true
		}
	}
	return false
}
