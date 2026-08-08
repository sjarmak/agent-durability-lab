package benchmark_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/livecommon"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/oracle"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

func TestLiveCommonCasesProduceExpectedIndependentVerdicts(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("exact process identity and process-tree control require Linux")
	}

	agentBinary := buildAgentSimulator(t)
	for _, benchmarkCase := range protocol.Cases() {
		for _, probe := range []protocol.Probe{protocol.ProbeUnfaulted, protocol.ProbeUnsafe, protocol.ProbeProtected} {
			t.Run(string(benchmarkCase)+"/"+string(probe), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				runDir, err := livecommon.Run(ctx, livecommon.Config{
					Root: t.TempDir(), Case: benchmarkCase, Probe: probe,
					Trial: 1, AgentBinary: agentBinary, AdapterVersion: "test-source-v1",
				})
				if err != nil {
					t.Fatalf("run live common adapter: %v", err)
				}
				assertRawEvidenceExistsWithoutVerdict(t, runDir)

				verdict, err := oracle.EvaluateAndWrite(ctx, runDir)
				if err != nil {
					t.Fatalf("evaluate live evidence: %v", err)
				}
				want := protocol.VerdictValidPass
				if probe == protocol.ProbeUnsafe {
					want = protocol.VerdictValidFail
				}
				if verdict.Class != want {
					t.Fatalf("verdict = %q, want %q; reasons=%v metrics=%+v", verdict.Class, want, verdict.ReasonCodes, verdict.Metrics)
				}
			})
		}
	}
}

func buildAgentSimulator(t *testing.T) string {
	t.Helper()
	root := benchmarkRepositoryRoot(t)
	binary := filepath.Join(t.TempDir(), "agent-simulator")
	command := exec.Command("go", "build", "-o", binary, "./cmd/agent-simulator")
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("build agent simulator: %v: %s", err, output)
	}
	if err := os.Chmod(binary, 0o700); err != nil {
		t.Fatalf("make agent simulator executable: %v", err)
	}
	return binary
}

func benchmarkRepositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate benchmark test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}
