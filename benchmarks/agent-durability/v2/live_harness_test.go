package v2_test

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/abalive"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/oracle"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
)

func TestLiveABAUsesRealDelayedProcessesAndDistinguishesFencing(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("exact process-start identity requires Linux")
	}

	clientBinary := buildABAClient(t)
	for _, probe := range []protocol.Probe{protocol.ProbeUnsafe, protocol.ProbeProtected} {
		for trial := 1; trial <= 3; trial++ {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			runDir, err := abalive.Run(ctx, abalive.Config{
				Root: t.TempDir(), Probe: probe, Trial: trial, ClientBinary: clientBinary,
				AdapterVersion: "source-sha256:" + strings.Repeat("a", 64),
			})
			cancel()
			if err != nil {
				t.Fatalf("%s trial %d: %v", probe, trial, err)
			}
			verdict, err := oracle.EvaluateAndWrite(context.Background(), runDir)
			if err != nil {
				t.Fatalf("evaluate %s trial %d: %v", probe, trial, err)
			}
			if verdict.Admission != protocol.AdmissionValid || verdict.Diagnosability != protocol.OutcomePass {
				t.Fatalf("%s trial %d admission = %+v", probe, trial, verdict)
			}
			if probe == protocol.ProbeUnsafe {
				if verdict.Safety != protocol.OutcomeFail || verdict.Liveness != protocol.OutcomeFail || verdict.Metrics.StaleActionAcceptCount != 4 {
					t.Fatalf("unsafe trial %d = %+v", trial, verdict)
				}
			} else if verdict.Correctness != protocol.OutcomePass || verdict.Safety != protocol.OutcomePass ||
				verdict.Liveness != protocol.OutcomePass || !verdict.EfficiencyEligible || verdict.Metrics.StaleActionAcceptCount != 0 {
				t.Fatalf("protected trial %d = %+v", trial, verdict)
			}
			processes := readJSONFile[[]protocol.ProcessObservation](t, filepath.Join(runDir, protocol.ProcessObservationsFile))
			if len(processes) != 3 {
				t.Fatalf("process observations = %d, want 3", len(processes))
			}
			for _, process := range processes {
				if !strings.HasPrefix(process.ProcessIdentity, "pid:") || strings.Contains(process.ProcessIdentity, "fixture") {
					t.Errorf("process identity = %q, want real PID/start identity", process.ProcessIdentity)
				}
			}
		}
	}
}

func buildABAClient(t *testing.T) string {
	t.Helper()
	root := benchmarkV2RepositoryRoot(t)
	binary := filepath.Join(t.TempDir(), "aba-client")
	command := exec.Command("go", "build", "-o", binary, "./benchmarks/agent-durability/v2/cmd/aba-client")
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("build ABA client: %v: %s", err, output)
	}
	if err := os.Chmod(binary, 0o700); err != nil {
		t.Fatalf("make ABA client executable: %v", err)
	}
	return binary
}

func benchmarkV2RepositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate v2 benchmark test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

func readJSONFile[T any](t *testing.T, path string) T {
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
