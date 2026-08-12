package oracle_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/conformance/evidence"
	conformanceoracle "github.com/sjarmak/temporal_projects/benchmarks/agent-durability/conformance/oracle"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/conformance/profile"
	legacyprotocol "github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

func TestConformanceOracleRecomputesStoredVerdicts(t *testing.T) {
	repositoryRoot, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve executable: %v", err)
	}
	root := filepath.Join(t.TempDir(), "suite")
	report, err := profile.RunCalibration(context.Background(), profile.Config{
		EvidenceRoot: root, SourceRoot: repositoryRoot,
		SchemaRoot: filepath.Join(repositoryRoot, "specs/coding-agent-durability/v1/schema"), ExecutablePath: executable,
	})
	if err != nil {
		t.Fatalf("run calibration profile: %v", err)
	}
	for _, relative := range []string{filepath.Join("runs", "unexpected-run"), filepath.Join("invalid-controls", "unexpected-control")} {
		extra := filepath.Join(root, relative)
		if err := os.Mkdir(extra, 0o750); err != nil {
			t.Fatalf("add unexpected inventory entry: %v", err)
		}
		recomputed, err := conformanceoracle.Evaluate(context.Background(), root, report.Pins)
		if err == nil || recomputed.Status != evidence.StatusNonconformant {
			t.Fatalf("extra %s = status %q, error %v; want nonconformant", relative, recomputed.Status, err)
		}
		if err := os.Remove(extra); err != nil {
			t.Fatalf("remove temporary mutation: %v", err)
		}
	}
	verdictPath := filepath.Join(root, "runs", "ambiguous-effect-protected-trial-1", legacyprotocol.VerdictFile)
	data, err := os.ReadFile(verdictPath)
	if err != nil {
		t.Fatalf("read verdict: %v", err)
	}
	var verdict legacyprotocol.Verdict
	if err := json.Unmarshal(data, &verdict); err != nil {
		t.Fatalf("decode verdict: %v", err)
	}
	verdict.Class = legacyprotocol.VerdictValidFail
	data, err = json.MarshalIndent(verdict, "", "  ")
	if err != nil {
		t.Fatalf("encode altered verdict: %v", err)
	}
	if err := os.WriteFile(verdictPath, append(data, '\n'), 0o600); err != nil {
		t.Fatalf("alter verdict: %v", err)
	}

	recomputed, err := conformanceoracle.Evaluate(context.Background(), root, report.Pins)
	if err == nil || recomputed.Status != evidence.StatusNonconformant {
		t.Fatalf("Evaluate() = status %q, error %v; want nonconformant error", recomputed.Status, err)
	}
	if len(recomputed.Episodes) != 28 || len(recomputed.InvalidControls) != 4 {
		t.Fatalf("recomputed inventory = %d episodes, %d controls", len(recomputed.Episodes), len(recomputed.InvalidControls))
	}
}
