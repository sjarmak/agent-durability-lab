package lab

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRawInventorySealsEveryRegularArtifact(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "nested"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "nested", "artifact"), []byte("evidence"), 0o600); err != nil {
		t.Fatal(err)
	}
	digest, err := writeRawInventory(root)
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := verifyRawInventory(root, digest)
	if err != nil {
		t.Fatal(err)
	}
	if len(inventory.Files) != 1 || inventory.Files[0].Path != "nested/artifact" {
		t.Fatalf("unexpected inventory: %+v", inventory)
	}
	if err := os.WriteFile(filepath.Join(root, "nested", "artifact"), []byte("tampered"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := verifyRawInventory(root, digest); err == nil {
		t.Fatal("tampered raw evidence passed inventory verification")
	}
}

func TestRawInventoryRejectsSymlinkArtifact(t *testing.T) {
	root := t.TempDir()
	if err := os.Symlink("missing", filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if _, err := writeRawInventory(root); err == nil {
		t.Fatal("symlinked raw evidence was inventoried")
	}
}
