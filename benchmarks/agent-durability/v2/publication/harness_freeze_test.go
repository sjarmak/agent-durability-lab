package publication

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestHarnessFreezeScopesSourceHashToPublicationRunner(t *testing.T) {
	for _, workspaceManifest := range []string{"go.mod", "go.sum"} {
		if slices.Contains(frozenHarnessSources, workspaceManifest) {
			t.Fatalf("workspace-wide manifest %q makes unrelated dependencies invalidate the frozen publication runner", workspaceManifest)
		}
	}
}

func TestPostPilotHarnessFreezeMatchesPublicationInputs(t *testing.T) {
	freeze, err := LoadHarnessFreeze(filepath.Join("..", "..", "publication-harness-freeze-v2.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyHarnessFreezeTrackedInputs(
		freeze, filepath.Join("..", "..", "..", ".."),
		filepath.Join("..", "..", "publication-preregistration-v2.json"),
	); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyHarnessFreezeStillRejectsMismatchedRunnerBinary(t *testing.T) {
	freeze, err := LoadHarnessFreeze(filepath.Join("..", "..", "publication-harness-freeze-v2.json"))
	if err != nil {
		t.Fatal(err)
	}
	runnerBinary := filepath.Join(t.TempDir(), "publication-runner")
	if err := os.WriteFile(runnerBinary, []byte("not the frozen runner"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := VerifyHarnessFreeze(
		freeze, filepath.Join("..", "..", "..", ".."), runnerBinary,
		filepath.Join("..", "..", "publication-preregistration-v2.json"),
	); err == nil {
		t.Fatal("mismatched publication runner binary was accepted")
	}
}
