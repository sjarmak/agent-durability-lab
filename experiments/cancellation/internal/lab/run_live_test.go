package lab

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func TestLiveTemporalCancellationMatrix(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("live process identity and pidfd evidence targets Linux")
	}
	temporalPath, err := exec.LookPath("temporal")
	if err != nil {
		t.Skip("Temporal CLI is required for live cancellation evidence")
	}
	root := cancellationRepositoryRoot(t)
	binDirectory := t.TempDir()
	workerBinary := buildCancellationTestBinary(t, root, binDirectory, "worker", "./cmd/worker")
	agentBinary := buildCancellationTestBinary(t, root, binDirectory, "agent", "./cmd/agent-simulator")
	outputRoot := filepath.Join(t.TempDir(), "evidence")
	for _, scenario := range []Scenario{
		ScenarioTemporalControl, ScenarioHealthySafe, ScenarioWorkerDeathSafe, ScenarioFrozenSafe,
	} {
		for _, wait := range []bool{false, true} {
			name := string(scenario) + map[bool]string{false: "/do-not-wait", true: "/wait"}[wait]
			t.Run(name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), defaultRunTimeout)
				defer cancel()
				result, err := Run(ctx, Options{
					Scenario: scenario, WaitForCancellation: wait, TemporalPath: temporalPath,
					WorkerBinary: workerBinary, AgentBinary: agentBinary, OutputRoot: outputRoot,
					RunID: "live-" + string(scenario) + map[bool]string{false: "-false", true: "-true"}[wait],
				})
				if err != nil {
					t.Fatalf("run live cancellation arm: %v", err)
				}
				if !result.Verdict.RunValid || !result.Verdict.ExpectedObservation ||
					(result.Verdict.InvariantSatisfied != scenario.Safe()) {
					t.Fatalf("verdict = %+v", result.Verdict)
				}
				for _, name := range []string{
					"events.jsonl", "application-state.json", "boundary-state.json",
					"verdict.json", "temporal-history.json", "manifest.json",
				} {
					if _, err := os.Stat(filepath.Join(result.RunDirectory, name)); err != nil {
						t.Errorf("missing evidence %s: %v", name, err)
					}
				}
			})
		}
	}
}

func buildCancellationTestBinary(t *testing.T, root, directory, name, packagePath string) string {
	t.Helper()
	path := filepath.Join(directory, name)
	command := exec.Command("go", "build", "-race", "-o", path, packagePath)
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", name, err, output)
	}
	return path
}
