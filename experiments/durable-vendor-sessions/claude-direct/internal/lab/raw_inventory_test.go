package lab

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRawInventoryBindsEveryPreservedArtifact(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "nested"), 0o750); err != nil {
		t.Fatalf("create nested directory: %v", err)
	}
	artifact := filepath.Join(root, "nested", "stdout.jsonl")
	if err := os.WriteFile(artifact, []byte("raw output\n"), 0o600); err != nil {
		t.Fatalf("write raw artifact: %v", err)
	}
	inventoryHash, err := writeRawInventory(root)
	if err != nil {
		t.Fatalf("write raw inventory: %v", err)
	}
	if inventoryHash == "" {
		t.Fatal("raw inventory hash is empty")
	}
	if err := verifyRawInventory(root, inventoryHash); err != nil {
		t.Fatalf("verify raw inventory: %v", err)
	}
	if err := verifyRawInventory(root, "wrong-hash"); err == nil {
		t.Fatal("wrong raw inventory hash passed verification")
	}
	if _, err := writeRawInventory(root); err == nil {
		t.Fatal("existing raw inventory was overwritten")
	}
	if err := os.WriteFile(artifact, []byte("changed\n"), 0o600); err != nil {
		t.Fatalf("change raw artifact: %v", err)
	}
	if err := verifyRawInventory(root, inventoryHash); err == nil {
		t.Fatal("mutated raw artifact passed inventory verification")
	}
}

func TestWriteTrialRawArtifactsPreservesExclusiveWriteFailures(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"temporal-history.json", "workspace-status.txt", "trial-summary.json"} {
		name := name
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, name), []byte("existing"), 0o600); err != nil {
				t.Fatalf("write existing %s: %v", name, err)
			}
			if _, err := writeTrialRawArtifacts(root, []byte("history"), []byte("status"), trialSummary{}); err == nil {
				t.Fatalf("existing %s did not fail", name)
			}
		})
	}
}

func TestWriteTrialRawArtifactsIncludesHistoryStatusAndSummary(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	hash, err := writeTrialRawArtifacts(root, []byte(`{"events":[]}`), []byte("dirty\n"), trialSummary{
		WorkflowID: "workflow-1", WorkflowRunID: "run-1", FaultBoundary: FaultAfterToolEffect,
	})
	if err != nil {
		t.Fatalf("write trial raw artifacts: %v", err)
	}
	if err := verifyRawInventory(root, hash); err != nil {
		t.Fatalf("verify trial raw artifacts: %v", err)
	}
	for _, name := range []string{"temporal-history.json", "workspace-status.txt", "trial-summary.json"} {
		if _, err := os.Stat(filepath.Join(root, name)); err != nil {
			t.Fatalf("stat %s: %v", name, err)
		}
	}
}
