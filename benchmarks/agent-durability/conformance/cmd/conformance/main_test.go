package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/conformance/evidence"
)

func TestRunEmitsMachineReadableApparatusResult(t *testing.T) {
	repositoryRoot := commandRepositoryRoot(t)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exitCode := runMain(context.Background(), []string{
		"--evidence-root", filepath.Join(t.TempDir(), "suite"),
		"--source-root", repositoryRoot,
		"--schema-root", filepath.Join(repositoryRoot, "specs/coding-agent-durability/v1/schema"),
	}, &stdout, &stderr)
	if exitCode != 0 {
		t.Fatalf("run command exit = %d; stderr=%s", exitCode, stderr.String())
	}
	var report evidence.Report
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("decode stdout: %v; stdout=%s", err, stdout.String())
	}
	if report.Status != evidence.StatusConformant || report.ProfileKind != evidence.CalibrationProfileKind {
		t.Fatalf("report = %+v", report)
	}
}

func TestRunRequiresExplicitPaths(t *testing.T) {
	t.Parallel()

	if err := run(context.Background(), nil, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
		t.Fatal("run accepted missing paths")
	}
	var stderr bytes.Buffer
	if exitCode := runMain(context.Background(), nil, &bytes.Buffer{}, &stderr); exitCode != 1 || stderr.Len() == 0 {
		t.Fatalf("runMain missing paths = exit %d, stderr %q", exitCode, stderr.String())
	}
	if err := run(context.Background(), []string{"--unknown"}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
		t.Fatal("run accepted unknown flag")
	}
	if err := run(context.Background(), []string{"--executable", "/bin/true"}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
		t.Fatal("run accepted caller-selected executable provenance")
	}
	if err := run(context.Background(), []string{"positional"}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
		t.Fatal("run accepted positional argument")
	}
}

func TestExecutableRunsFromUnrelatedWorkingDirectory(t *testing.T) {
	repositoryRoot := commandRepositoryRoot(t)
	root := t.TempDir()
	binary := filepath.Join(root, "coding-agent-conformance")
	command := exec.Command("go", "build", "-trimpath", "-o", binary, "./benchmarks/agent-durability/conformance/cmd/conformance")
	command.Dir = repositoryRoot
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build command: %v\n%s", err, output)
	}
	evidenceRoot := filepath.Join(root, "suite")
	command = exec.Command(binary,
		"--evidence-root", evidenceRoot,
		"--source-root", repositoryRoot,
		"--schema-root", filepath.Join(repositoryRoot, "specs/coding-agent-durability/v1/schema"),
	)
	command.Dir = t.TempDir()
	command.Env = []string{"HOME=" + t.TempDir(), "TMPDIR=" + t.TempDir(), "TZ=UTC", "PATH=" + os.Getenv("PATH")}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("execute command: %v\n%s", err, output)
	}
	var report evidence.Report
	if err := json.Unmarshal(output, &report); err != nil {
		t.Fatalf("decode output: %v\n%s", err, output)
	}
	if report.Status != evidence.StatusConformant {
		t.Fatalf("status = %q", report.Status)
	}
}

func commandRepositoryRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../../../../..")
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	return root
}
