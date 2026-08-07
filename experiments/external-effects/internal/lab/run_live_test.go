package lab

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestLiveTemporalExternalEffectArms(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("live Worker SIGKILL experiment currently targets Linux")
	}
	temporalPath, err := exec.LookPath("temporal")
	if err != nil {
		t.Skip("Temporal CLI is required for live service evidence")
	}
	repositoryRoot := liveTestRepositoryRoot(t)
	workerBinary := filepath.Join(t.TempDir(), "external-effect-worker")
	build := exec.Command("go", "build", "-race", "-o", workerBinary, "./experiments/external-effects/cmd/worker")
	build.Dir = repositoryRoot
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build external-effect Worker: %v\n%s", err, output)
	}
	outputRoot := filepath.Join(t.TempDir(), "evidence")
	for _, destination := range AllDestinations() {
		for _, mode := range []Mode{ModeUnsafe, ModeProtected} {
			t.Run(string(destination)+"/"+string(mode), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()
				runID := "live-" + string(destination) + "-" + string(mode)
				result, err := Run(ctx, Options{
					Destination: destination, Mode: mode, TemporalPath: temporalPath,
					WorkerBinary: workerBinary, OutputRoot: outputRoot, RunID: runID,
				})
				if err != nil {
					t.Fatalf("run live arm: %v", err)
				}
				if !result.Verdict.RunValid || !result.Verdict.ExpectedObservation {
					t.Fatalf("verdict = %+v", result.Verdict)
				}
				if got := result.Verdict.InvariantSatisfied; got != (mode == ModeProtected) {
					t.Fatalf("invariant satisfied = %v for %s", got, mode)
				}
				for _, name := range []string{
					"manifest.json", "observations.json", "destination-state.json", "verdict.json",
					"temporal-history.json", "temporal-server.log", filepath.Join("workers", "worker-1.log"),
					filepath.Join("workers", "worker-2.log"),
				} {
					if _, err := os.Stat(filepath.Join(result.RunDirectory, name)); err != nil {
						t.Errorf("missing evidence %s: %v", name, err)
					}
				}
			})
		}
	}
}

func liveTestRepositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate live test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", ".."))
}
