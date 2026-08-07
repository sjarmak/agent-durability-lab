package lab

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/temporalio-labs/agent-durability-lab/internal/workstore"
)

func TestLiveTemporalWorkerDeathArms(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("live Worker SIGKILL experiment currently targets Linux")
	}
	temporalPath, err := exec.LookPath("temporal")
	if err != nil {
		t.Skip("Temporal CLI is required for live service evidence")
	}
	root := liveTestRepositoryRoot(t)
	binDir := t.TempDir()
	workerBinary := buildLiveTestBinary(t, root, binDir, "worker", "./cmd/worker")
	agentBinary := buildLiveTestBinary(t, root, binDir, "agent-simulator", "./cmd/agent-simulator")
	outputRoot := filepath.Join(t.TempDir(), "evidence")

	const trialCount = 2
	for _, mode := range []workstore.Mode{workstore.ModeUnsafe, workstore.ModeReattach, workstore.ModeFenced} {
		for trial := 1; trial <= trialCount; trial++ {
			t.Run(string(mode)+"/trial-"+strconv.Itoa(trial), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
				defer cancel()
				runID := "live-" + string(mode) + "-trial-" + strconv.Itoa(trial)
				result, err := Run(ctx, Options{
					Mode: mode, TemporalPath: temporalPath, WorkerBinary: workerBinary,
					AgentBinary: agentBinary, OutputRoot: outputRoot, RunID: runID,
				})
				if err != nil {
					t.Fatalf("run live %s arm trial %d: %v", mode, trial, err)
				}
				if !result.Verdict.RunValid || !result.Verdict.ExpectedObservation {
					t.Fatalf("verdict = %+v", result.Verdict)
				}
				if mode != workstore.ModeUnsafe && !result.Verdict.InvariantSatisfied {
					t.Fatalf("safe arm verdict = %+v; want invariant satisfied", result.Verdict)
				}
				for _, name := range []string{"events.jsonl", "application-state.json", "verdict.json", "temporal-history.json", "manifest.json"} {
					if _, err := os.Stat(filepath.Join(result.RunDirectory, name)); err != nil {
						t.Errorf("missing evidence %s: %v", name, err)
					}
				}
			})
		}
	}
}

func TestLiveTemporalLaunchRegistrationGapArms(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("live Worker SIGKILL experiment currently targets Linux")
	}
	temporalPath, err := exec.LookPath("temporal")
	if err != nil {
		t.Skip("Temporal CLI is required for live service evidence")
	}
	root := liveTestRepositoryRoot(t)
	binDir := t.TempDir()
	workerBinary := buildLiveTestBinary(t, root, binDir, "worker", "./cmd/worker")
	agentBinary := buildLiveTestBinary(t, root, binDir, "agent-simulator", "./cmd/agent-simulator")
	outputRoot := filepath.Join(t.TempDir(), "evidence")

	const trialCount = 2
	for _, arm := range []LaunchGapArm{LaunchGapControl, LaunchGapFencedRecovery} {
		for trial := 1; trial <= trialCount; trial++ {
			t.Run(string(arm)+"/trial-"+strconv.Itoa(trial), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
				defer cancel()
				runID := "live-launch-gap-" + string(arm) + "-trial-" + strconv.Itoa(trial)
				result, err := RunLaunchGap(ctx, LaunchGapOptions{
					Arm: arm,
					Options: Options{
						Mode: workstore.ModeFenced, TemporalPath: temporalPath, WorkerBinary: workerBinary,
						AgentBinary: agentBinary, OutputRoot: outputRoot, RunID: runID,
					},
				})
				if err != nil {
					t.Fatalf("run live %s arm trial %d: %v", arm, trial, err)
				}
				if !result.Verdict.RunValid || !result.Verdict.ExpectedObservation {
					t.Fatalf("verdict = %+v", result.Verdict)
				}
				if got := result.Verdict.InvariantSatisfied; got != (arm == LaunchGapFencedRecovery) {
					t.Fatalf("invariant satisfied = %v for %s", got, arm)
				}
				for _, name := range []string{
					"events.jsonl", "application-state.json", "verdict.json", "temporal-history.json", "manifest.json",
				} {
					if _, err := os.Stat(filepath.Join(result.RunDirectory, name)); err != nil {
						t.Errorf("missing evidence %s: %v", name, err)
					}
				}
			})
		}
	}
}

func buildLiveTestBinary(t *testing.T, root, binDir, name, packagePath string) string {
	t.Helper()
	path := filepath.Join(binDir, name)
	command := exec.Command("go", "build", "-race", "-o", path, packagePath)
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", name, err, output)
	}
	return path
}

func liveTestRepositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate live test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", ".."))
}
