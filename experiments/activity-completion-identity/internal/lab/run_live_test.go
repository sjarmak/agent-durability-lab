package lab

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestLiveTemporalCompletionIdentityArms(t *testing.T) {
	temporalPath, err := exec.LookPath("temporal")
	if err != nil {
		t.Skip("Temporal CLI is required for live service evidence")
	}
	outputRoot := os.Getenv("ADL_LIVE_EVIDENCE_ROOT")
	if outputRoot == "" {
		outputRoot = filepath.Join(t.TempDir(), "evidence")
	}
	for _, arm := range []Arm{ArmStaleTaskToken, ArmStaleByID, ArmFencedByID} {
		t.Run(string(arm), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			result, err := Run(ctx, Options{
				Arm: arm, TemporalPath: temporalPath, OutputRoot: outputRoot,
				RunID: "live-" + string(arm), Timeout: 25 * time.Second,
			})
			if err != nil {
				t.Fatalf("run %s arm: %v", arm, err)
			}
			if !result.Verdict.RunValid || !result.Verdict.ExpectedObservation {
				t.Fatalf("verdict = %+v", result.Verdict)
			}
			if got := result.Verdict.InvariantSatisfied; got != (arm != ArmStaleByID) {
				t.Fatalf("InvariantSatisfied = %v for %s", got, arm)
			}
			for _, name := range []string{
				"observations.json", "verdict.json", "temporal-history.json", "manifest.json", "temporal-server.log",
			} {
				if _, err := os.Stat(filepath.Join(result.RunDirectory, name)); err != nil {
					t.Errorf("missing evidence %s: %v", name, err)
				}
			}
		})
	}
}
