package publication

import (
	"path/filepath"
	"testing"
)

func TestPostPilotHarnessFreezeMatchesPublicationInputs(t *testing.T) {
	freeze, err := LoadHarnessFreeze(filepath.Join("..", "..", "publication-harness-freeze-v2.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyHarnessFreeze(
		freeze, filepath.Join("..", "..", "..", ".."),
		filepath.Join("..", "..", "..", "..", "bin", "agent-durability-v2-publication"),
		filepath.Join("..", "..", "publication-preregistration-v2.json"),
	); err != nil {
		t.Fatal(err)
	}
}
