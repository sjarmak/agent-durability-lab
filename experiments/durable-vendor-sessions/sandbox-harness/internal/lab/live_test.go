package lab

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLiveSandboxHarnessSuite(t *testing.T) {
	temporalPath := os.Getenv("SANDBOX_HARNESS_TEMPORAL_PATH")
	if temporalPath == "" {
		t.Skip("set SANDBOX_HARNESS_TEMPORAL_PATH to run the live Temporal suite")
	}
	result, err := Run(context.Background(), Options{
		EvidenceRoot: filepath.Join(t.TempDir(), "evidence"),
		TemporalPath: temporalPath,
		Trials:       3,
		Timeout:      2 * time.Minute,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(result.RunDirs) != 36 {
		t.Fatalf("admitted runs = %d, want 36", len(result.RunDirs))
	}
}
