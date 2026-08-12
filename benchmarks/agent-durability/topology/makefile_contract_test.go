package topology_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

var timingShapes = []string{
	"unfaulted-direct-supersession-32",
	"unfaulted-child-outage-128",
	"protected-child-outage-128",
	"protected-child-silent-progress-8",
}

var correctnessIntegrations = []string{
	"TestTemporalExecutorRecoversJoinAcrossBothTopologyArms",
	"TestTemporalExecutorCoversFrozenSemanticsCasesAndBoundaries",
	"TestTemporalExecutorCoversFrozenRecoveryCasesAndBoundaries",
	"TestTemporalExecutorRecoveryScaleDoesNotDeadlockAdmission",
	"TestTemporalExecutorUnsafeQueuedChildScaleClosesHeldBarriers",
}

func TestDefaultCoverageExcludesControlledHostTimingProfiles(t *testing.T) {
	output := makeDryRun(t, "coverage")
	if strings.Contains(output, "TOPOLOGY_PILOT_V5_REGRESSION=1") {
		t.Fatal("default coverage still executes the controlled-host timing profiles")
	}
	for _, required := range append(correctnessIntegrations, "coverage.topology.out") {
		if !strings.Contains(output, required) {
			t.Fatalf("default coverage omits correctness gate %q", required)
		}
	}
}

func TestControlledHostCoverageRetainsEveryTimingProfile(t *testing.T) {
	output := makeDryRun(t, "coverage-topology-timing", "TOPOLOGY_CONTROLLED_HOST=1")
	if !strings.Contains(output, "TOPOLOGY_PILOT_V5_REGRESSION=1") {
		t.Fatal("controlled-host coverage does not enable the registered timing profile")
	}
	for _, shape := range timingShapes {
		if !strings.Contains(output, shape) {
			t.Fatalf("controlled-host coverage omits timing shape %q", shape)
		}
	}
	if !strings.Contains(output, "coverage.topology.timing.out") {
		t.Fatal("controlled-host coverage does not publish a distinct timing profile")
	}
	if !strings.Contains(output, `-run "TestTemporalExecutorPilotV5FailureShapesRecoverRepeatedly/$shape"`) {
		t.Fatal("controlled-host coverage changed the registered timing selector")
	}

	repoRoot := topologyRepositoryRoot(t)
	runtimeSource := readContractFile(t, filepath.Join(repoRoot, "benchmarks", "agent-durability", "topology", "semantics", "recovery_runtime.go"))
	if !strings.Contains(runtimeSource, "progressDeadlineMS           = 5000") {
		t.Fatal("controlled-host coverage changed the registered progress deadline")
	}
	integrationPath := filepath.Join(repoRoot, "benchmarks", "agent-durability", "topology", "semantics", "executor_integration_test.go")
	pilotBody := readFunctionBody(t, integrationPath, "TestTemporalExecutorPilotV5FailureShapesRecoverRepeatedly")
	if !strings.Contains(pilotBody, "if detectionMS > progressDeadlineMS") {
		t.Fatal("controlled-host coverage no longer enforces the registered detection-latency oracle")
	}
}

func TestControlledHostCoverageRequiresExplicitAdmission(t *testing.T) {
	for _, value := range []string{"", "0", "true"} {
		t.Run("value="+value, func(t *testing.T) {
			marker := runRejectedTimingTarget(t, value)
			if _, err := os.Stat(marker); !os.IsNotExist(err) {
				t.Fatalf("controlled-host rejection executed a coverage prerequisite: %v", err)
			}
		})
	}
}

func TestControlledHostAdmissionTreatsConfigurationAsData(t *testing.T) {
	for _, variable := range []string{"TOPOLOGY_CONTROLLED_HOST", "TOPOLOGY_LOADAVG_PATH"} {
		t.Run(variable, func(t *testing.T) {
			tempDir := t.TempDir()
			marker := filepath.Join(tempDir, "injected")
			payload := `invalid"; : > "` + marker + `"; false; echo "`
			args := []string{"check-topology-controlled-host", "TOPOLOGY_CONTROLLED_HOST=1"}
			if variable == "TOPOLOGY_CONTROLLED_HOST" {
				args[1] = variable + "=" + payload
			} else {
				args = append(args, variable+"="+payload)
			}
			cmd := exec.Command("make", args...)
			cmd.Dir = topologyRepositoryRoot(t)
			if output, err := cmd.CombinedOutput(); err == nil {
				t.Fatalf("quote-bearing %s was accepted\n%s", variable, output)
			}
			if _, err := os.Stat(marker); !os.IsNotExist(err) {
				t.Fatalf("quote-bearing %s executed injected shell source: %v", variable, err)
			}
		})
	}
}

func TestControlledHostCoverageRejectsMalformedTelemetry(t *testing.T) {
	repoRoot := topologyRepositoryRoot(t)
	makefile := readContractFile(t, filepath.Join(repoRoot, "Makefile"))
	for _, required := range []string{"TOPOLOGY_LOADAVG_PATH", "TOPOLOGY_CPU_PRESSURE_PATH", "controlled-host admission data is malformed"} {
		if !strings.Contains(makefile, required) {
			t.Fatalf("controlled-host admission cannot be tested fail-closed: missing %q", required)
		}
	}

	tests := []struct {
		name     string
		load     string
		pressure string
	}{
		{name: "empty load", pressure: "some avg10=0.00 avg60=0.00 avg300=0.00 total=1\n"},
		{name: "nonnumeric load", load: "busy 0.00 0.00 1/1 1\n", pressure: "some avg10=0.00 avg60=0.00 avg300=0.00 total=1\n"},
		{name: "missing some pressure", load: "0.00 0.00 0.00 1/1 1\n", pressure: "full avg10=0.00 avg60=0.00 avg300=0.00 total=1\n"},
		{name: "duplicate avg10", load: "0.00 0.00 0.00 1/1 1\n", pressure: "some avg10=0.00 avg10=0.00 avg60=0.00 avg300=0.00 total=1\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runMalformedTelemetryProbe(t, test.load, test.pressure)
		})
	}
}

func TestMixedCoverageGoalsShareOneCorrectnessRun(t *testing.T) {
	output := makeDryRun(t, "coverage-topology", "coverage-topology-timing", "TOPOLOGY_CONTROLLED_HOST=1", "-j2")
	if got := strings.Count(output, "-coverprofile=coverage.topology.packages.out"); got != 1 {
		t.Fatalf("mixed topology coverage goals schedule %d correctness writers, want 1", got)
	}
}

func TestCoverageDocumentationSeparatesCorrectnessFromTimingClaims(t *testing.T) {
	repoRoot := topologyRepositoryRoot(t)
	topologyREADME := readContractFile(t, filepath.Join(repoRoot, "benchmarks", "agent-durability", "topology", "README.md"))
	for _, required := range []string{
		"make coverage-topology",
		"make coverage-topology-timing TOPOLOGY_CONTROLLED_HOST=1",
		"does not renew current-source timing or performance evidence",
	} {
		if !strings.Contains(topologyREADME, required) {
			t.Fatalf("topology README omits coverage contract %q", required)
		}
	}

	productSpec := readContractFile(t, filepath.Join(repoRoot, "docs", "product", "coding-agent-durability-v1.md"))
	for _, required := range []string{
		"Default product coverage excludes controlled-host timing profiles",
		"does not block v1",
	} {
		if !strings.Contains(productSpec, required) {
			t.Fatalf("product specification omits release boundary %q", required)
		}
	}
}

func makeDryRun(t *testing.T, target string, args ...string) string {
	t.Helper()
	commandArgs := append([]string{"-n", target}, args...)
	cmd := exec.Command("make", commandArgs...)
	cmd.Dir = topologyRepositoryRoot(t)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("dry-run %s: %v\n%s", target, err, output)
	}
	return string(output)
}

func runRejectedTimingTarget(t *testing.T, admission string) string {
	t.Helper()
	tempDir := t.TempDir()
	marker := writeGoSentinel(t, tempDir)

	cmd := exec.Command("make", "coverage-topology-timing")
	cmd.Dir = topologyRepositoryRoot(t)
	cmd.Env = append(withoutEnvironment(os.Environ(), "TOPOLOGY_CONTROLLED_HOST"),
		"PATH="+tempDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"TOPOLOGY_CONTROLLED_HOST="+admission,
	)
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("controlled-host coverage accepted admission %q\n%s", admission, output)
	}
	if !strings.Contains(string(output), "TOPOLOGY_CONTROLLED_HOST=1 is required") {
		t.Fatalf("controlled-host coverage rejected admission %q for the wrong reason:\n%s", admission, output)
	}
	return marker
}

func runMalformedTelemetryProbe(t *testing.T, load, pressure string) {
	t.Helper()
	tempDir := t.TempDir()
	loadPath := filepath.Join(tempDir, "loadavg")
	pressurePath := filepath.Join(tempDir, "cpu-pressure")
	if err := os.WriteFile(loadPath, []byte(load), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pressurePath, []byte(pressure), 0o600); err != nil {
		t.Fatal(err)
	}
	marker := writeGoSentinel(t, tempDir)
	cmd := exec.Command("make", "coverage-topology-timing", "TOPOLOGY_CONTROLLED_HOST=1",
		"TOPOLOGY_LOADAVG_PATH="+loadPath, "TOPOLOGY_CPU_PRESSURE_PATH="+pressurePath)
	cmd.Dir = topologyRepositoryRoot(t)
	cmd.Env = append(os.Environ(), "PATH="+tempDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	output, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "controlled-host admission data is malformed") {
		t.Fatalf("malformed host telemetry did not fail closed: %v\n%s", err, output)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("malformed host telemetry executed a coverage prerequisite: %v", err)
	}
}

func writeGoSentinel(t *testing.T, directory string) string {
	t.Helper()
	marker := filepath.Join(directory, "go-ran")
	script := "#!/bin/sh\n: > \"" + marker + "\"\nexit 97\n"
	if err := os.WriteFile(filepath.Join(directory, "go"), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return marker
}

func withoutEnvironment(environment []string, name string) []string {
	prefix := name + "="
	filtered := make([]string, 0, len(environment))
	for _, entry := range environment {
		if !strings.HasPrefix(entry, prefix) {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}

func topologyRepositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve topology contract test path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

func readContractFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func readFunctionBody(t *testing.T, path, name string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, path, data, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Name.Name == name && function.Body != nil {
			start := fileSet.Position(function.Body.Pos()).Offset
			end := fileSet.Position(function.Body.End()).Offset
			return string(data[start:end])
		}
	}
	t.Fatalf("function %s not found in %s", name, path)
	return ""
}
