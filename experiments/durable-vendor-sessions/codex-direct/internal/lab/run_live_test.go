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

const codexDirectCoveragePackage = "./experiments/durable-vendor-sessions/codex-direct/internal/lab"

func TestLiveHermeticRecoveryMatrix(t *testing.T) {
	if os.Getenv("CODEX_DIRECT_LIVE_TEST") != "1" {
		t.Skip("set CODEX_DIRECT_LIVE_TEST=1 to run the real process/service matrix")
	}
	if testing.Short() {
		t.Skip("process/service integration test")
	}
	temporalPath, err := exec.LookPath("temporal")
	if err != nil {
		t.Skip("Temporal CLI is not installed")
	}
	repositoryRoot := codexDirectRepositoryRoot(t)
	directory := t.TempDir()
	binaryDirectory := filepath.Join(directory, "bin")
	if err := os.Mkdir(binaryDirectory, 0o750); err != nil {
		t.Fatalf("create binary directory: %v", err)
	}
	workerBinary := filepath.Join(binaryDirectory, "worker")
	effectBinary := filepath.Join(binaryDirectory, "controlled-effect")
	launcherBinary := filepath.Join(binaryDirectory, "codex-launcher")
	hermeticCodex := filepath.Join(binaryDirectory, "hermetic-codex")
	buildLiveBinary(t, repositoryRoot, workerBinary,
		"./experiments/durable-vendor-sessions/codex-direct/cmd/worker")
	buildLiveBinary(t, repositoryRoot, effectBinary,
		"./experiments/durable-vendor-sessions/codex-direct/cmd/controlled-effect")
	buildLiveBinary(t, repositoryRoot, launcherBinary,
		"./experiments/durable-vendor-sessions/codex-direct/cmd/codex-launcher")
	buildLiveBinary(t, repositoryRoot, hermeticCodex,
		"./experiments/durable-vendor-sessions/codex-direct/cmd/hermetic-codex")
	codexHome := filepath.Join(directory, "codex-home")
	if err := os.Mkdir(codexHome, 0o750); err != nil {
		t.Fatalf("create Codex home: %v", err)
	}
	schema := filepath.Join(repositoryRoot,
		"experiments/durable-vendor-sessions/codex-direct/schema/effect-result.schema.json")

	tests := []struct {
		name                               string
		mode                               RecoveryMode
		runs, validPasses, distinguishing  int
		processes, threads, effects        int
		attachments, replacements, cancels int
	}{
		{name: "unsafe-fresh", mode: RecoveryModeUnsafeFresh, runs: 4,
			validPasses: 2, distinguishing: 2, processes: 7, threads: 6, effects: 6},
		{name: "explicit-thread-resume", mode: RecoveryModeResumeOnly, runs: 4,
			validPasses: 2, distinguishing: 2, processes: 7, threads: 6, effects: 6},
		{name: "application-fenced", mode: RecoveryModeFenced, runs: 9,
			validPasses: 9, processes: 10, threads: 9, effects: 8,
			attachments: 7, replacements: 1, cancels: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			evidenceRoot := filepath.Join(directory, test.name+"-evidence")
			result, err := RunExperiment(context.Background(), ExperimentOptions{
				EvidenceRoot: evidenceRoot, TemporalPath: temporalPath,
				WorkerBinary: workerBinary, EffectBinary: effectBinary, LauncherBinary: launcherBinary,
				CodexBinary: hermeticCodex, CodexWrapper: hermeticCodex, CodexHome: codexHome,
				OutputSchema: schema, Trials: 1, Timeout: 10 * time.Minute,
				Model: "gpt-5.6-sol", ReasoningEffort: "low", RecoveryMode: test.mode, Hermetic: true,
			})
			if err != nil {
				t.Fatalf("run live %s experiment: %v", test.name, err)
			}
			if len(result.RunDirectories) != test.runs {
				t.Fatalf("run directories = %d, want %d", len(result.RunDirectories), test.runs)
			}
			report, err := AuditEvidence(context.Background(), evidenceRoot)
			if err != nil {
				t.Fatalf("audit live %s evidence: %v", test.name, err)
			}
			if !report.AllRequirementsVerified || report.Runs != test.runs ||
				report.ValidPassRuns != test.validPasses ||
				report.DistinguishingFailRuns != test.distinguishing ||
				report.HistoriesReplayed != test.runs || report.RawInventoriesVerified != test.runs ||
				report.ProcessesObserved != test.processes || report.ThreadsObserved != test.threads ||
				report.PhysicalEffects != test.effects || report.AttachmentsObserved != test.attachments ||
				report.ReplacementsObserved != test.replacements ||
				report.CancellationsObserved != test.cancels || report.CapabilityLeaks != 0 {
				t.Fatalf("live %s audit = %+v", test.name, report)
			}
		})
	}
}

func TestCoverageBuildEmitsChildProfile(t *testing.T) {
	t.Setenv("CODEX_DIRECT_EXTERNAL_COVERAGE", "1")
	directory := t.TempDir()
	binary := filepath.Join(directory, "hermetic-codex")
	buildLiveBinary(t, codexDirectRepositoryRoot(t), binary,
		"./experiments/durable-vendor-sessions/codex-direct/cmd/hermetic-codex")
	coverageDirectory := filepath.Join(directory, "coverage")
	if err := os.Mkdir(coverageDirectory, 0o750); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, "--version")
	command.Env = mergeEnvironment(os.Environ(), []string{"GOCOVERDIR=" + coverageDirectory})
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("run coverage binary: %v\n%s", err, output)
	}
	entries, err := os.ReadDir(coverageDirectory)
	if err != nil || len(entries) == 0 {
		t.Fatalf("child coverage directory is empty: %v", err)
	}
}

func codexDirectRepositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", "..", ".."))
}

func buildLiveBinary(t *testing.T, root, output, packagePath string) {
	t.Helper()
	arguments := []string{"build", "-trimpath"}
	if os.Getenv("CODEX_DIRECT_EXTERNAL_COVERAGE") == "1" {
		arguments = append(arguments, "-cover", "-covermode=atomic",
			"-coverpkg="+packagePath+","+codexDirectCoveragePackage)
	} else {
		arguments = append(arguments, "-race")
	}
	arguments = append(arguments, "-o", output, packagePath)
	command := exec.Command("go", arguments...)
	command.Dir = root
	if buildOutput, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", packagePath, err, buildOutput)
	}
}
