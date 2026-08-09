package lab

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

func TestRunExperimentWithFakeClaudeProvesUnsafeRetryViolation(t *testing.T) {
	if testing.Short() {
		t.Skip("process/service integration test")
	}
	temporalPath, err := exec.LookPath("temporal")
	if err != nil {
		t.Skip("Temporal CLI is not installed")
	}
	repositoryRoot := claudeDirectRepositoryRoot(t)
	directory := t.TempDir()
	workerBinary := filepath.Join(directory, "claude-direct-worker")
	effectBinary := filepath.Join(directory, "controlled-effect")
	launcherBinary := filepath.Join(directory, "claude-direct-launcher")
	buildBinary(t, repositoryRoot, workerBinary, "./experiments/durable-vendor-sessions/claude-direct/cmd/worker")
	buildBinary(t, repositoryRoot, effectBinary, "./experiments/durable-vendor-sessions/claude-direct/cmd/controlled-effect")
	buildBinary(t, repositoryRoot, launcherBinary, "./experiments/durable-vendor-sessions/claude-direct/cmd/claude-launcher")
	fakeClaude := filepath.Join(directory, "fake-claude")
	fakeBody := `#!/bin/sh
set -eu
if [ "${1:-}" = "--version" ]; then
  echo "fake-claude 1.0"
  exit 0
fi
IFS= read -r instruction
IFS= read -r controlled_command
printf '{"type":"system","subtype":"init","session_id":"fake-session-%s"}\n' "$$"
sh -c "$controlled_command" >/dev/null
printf '{"type":"result","subtype":"success","session_id":"fake-session-%s","is_error":false,"structured_output":{"status":"EFFECT_COMPLETE"}}\n' "$$"
`
	if err := os.WriteFile(fakeClaude, []byte(fakeBody), 0o700); err != nil {
		t.Fatalf("write fake Claude: %v", err)
	}

	result, err := RunExperiment(context.Background(), ExperimentOptions{
		EvidenceRoot: filepath.Join(directory, "evidence"), TemporalPath: temporalPath,
		WorkerBinary: workerBinary, EffectBinary: effectBinary, LauncherBinary: launcherBinary,
		ClaudeBinary: fakeClaude,
		Trials:       3, Timeout: 6 * time.Minute, Model: "fake", MaxBudgetUSD: "0.01", MaxTurns: 2,
	})
	if err != nil {
		t.Fatalf("run fake-Claude experiment: %v", err)
	}
	if len(result.RunDirectories) != 12 {
		t.Fatalf("run directories = %d, want 12", len(result.RunDirectories))
	}
	want := []protocol.VerdictClass{
		protocol.VerdictValidPass,
		protocol.VerdictValidFail,
		protocol.VerdictValidFail,
		protocol.VerdictValidFail,
	}
	wantBoundaries := []FaultBoundary{
		FaultNone, FaultBeforeVendorRegistration, FaultAfterToolEffect, FaultAfterFinalOutput,
	}
	for index, runDirectory := range result.RunDirectories {
		armIndex := index % len(want)
		data, err := os.ReadFile(filepath.Join(runDirectory, protocol.VerdictFile))
		if err != nil {
			t.Fatalf("read verdict %d: %v", index, err)
		}
		var verdict protocol.Verdict
		if err := json.Unmarshal(data, &verdict); err != nil {
			t.Fatalf("decode verdict %d: %v", index, err)
		}
		if verdict.Class != want[armIndex] {
			t.Fatalf("verdict %d = %+v, want %s", index, verdict, want[armIndex])
		}
		if armIndex > 0 && !containsReason(verdict.ReasonCodes, protocol.ReasonDuplicateEffect) {
			t.Fatalf("verdict %d lacks duplicate effect: %+v", index, verdict)
		}
		rawDirectory := filepath.Join(runDirectory, "raw")
		summary, err := readJSONFile[trialSummary](filepath.Join(rawDirectory, "trial-summary.json"))
		if err != nil {
			t.Fatalf("read trial summary %d: %v", index, err)
		}
		if summary.FaultBoundary != wantBoundaries[armIndex] || summary.Trial != (index/len(want))+1 ||
			len(summary.WorkspaceEffects) != indexEffectCount(armIndex) {
			t.Fatalf("trial summary %d = %+v", index, summary)
		}
		input, err := readJSONFile[protocol.EffectiveInput](filepath.Join(runDirectory, protocol.EffectiveInputFile))
		if err != nil {
			t.Fatalf("read effective input %d: %v", index, err)
		}
		if err := verifyRawInventory(rawDirectory, input.Settings["raw_inventory_sha256"]); err != nil {
			t.Fatalf("verify raw inventory %d: %v", index, err)
		}
	}
}

func indexEffectCount(index int) int {
	if index == 0 {
		return 1
	}
	return 2
}

func claudeDirectRepositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", "..", ".."))
}

func buildBinary(t *testing.T, root, output, packagePath string) {
	t.Helper()
	command := exec.Command("go", "build", "-o", output, packagePath)
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", packagePath, err, output)
	}
}
