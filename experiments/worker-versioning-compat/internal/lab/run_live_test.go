package lab

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

	"go.temporal.io/sdk/testsuite"
)

func TestLiveWorkerVersioningSessionCompatibility(t *testing.T) {
	temporalPath, err := exec.LookPath("temporal")
	if err != nil {
		t.Skip("Temporal CLI is required for live Worker Versioning evidence")
	}
	root, err := os.MkdirTemp(".", ".worker-versioning-live-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	server, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath: temporalPath, DBFilename: filepath.Join(root, "temporal.db"), EnableUI: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	evidenceRoot := filepath.Join(root, "live-test")
	result, err := RunExperiment(ctx, RunOptions{
		Client: server.Client(), Root: evidenceRoot,
		Environment: Environment{CapturedAt: time.Now().UTC(), GoVersion: runtime.Version(), SDKVersion: "v1.47.0", TemporalCLI: "test-cli", ExecutableSHA256: strings.Repeat("0", 64), OS: runtime.GOOS, Architecture: runtime.GOARCH},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Scenarios) != 9 || !result.CompatibleHistoriesReplay || !result.IncompatibleWorkflowRejected {
		t.Fatalf("result = %+v", result)
	}
	audited, err := AuditEvidence(evidenceRoot)
	if err != nil {
		t.Fatalf("audit evidence: %v", err)
	}
	if len(audited.Scenarios) != 9 {
		t.Fatalf("audited scenarios = %d", len(audited.Scenarios))
	}
	mutated := audited.Scenarios[0].Registry
	mutated.AgentBuild = "attacker-build"
	mutatedData, err := json.MarshalIndent(mutated, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(evidenceRoot, "auto-compatible-trial-1-registry.json"), append(mutatedData, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	entries, err := inventory(evidenceRoot, true)
	if err != nil {
		t.Fatal(err)
	}
	manifestData, err := json.MarshalIndent(EvidenceManifest{Schema: evidenceSchema, Entries: entries}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(evidenceRoot, "manifest.json"), append(manifestData, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := AuditEvidence(evidenceRoot); err == nil {
		t.Fatal("coherently rehashed registry mutation was accepted")
	}
}
