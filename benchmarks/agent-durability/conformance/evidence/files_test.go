package evidence

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	legacyprotocol "github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

func TestExclusiveFileAndDirectorySyncFailurePaths(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	path := filepath.Join(root, "artifact.json")
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := writeExclusiveFile(canceled, path, []byte("data")); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled write error = %v", err)
	}
	if err := writeExclusiveFile(context.Background(), path, []byte("data")); err != nil {
		t.Fatalf("first exclusive write: %v", err)
	}
	if err := writeExclusiveFile(context.Background(), path, []byte("replacement")); !errors.Is(err, legacyprotocol.ErrEvidenceExists) {
		t.Fatalf("second exclusive write error = %v", err)
	}
	if err := syncDirectory(root); err != nil {
		t.Fatalf("sync directory: %v", err)
	}
	if err := syncDirectory(filepath.Join(root, "missing")); err == nil {
		t.Fatal("syncDirectory accepted missing directory")
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "data" {
		t.Fatalf("exclusive content = %q, error %v", data, err)
	}
}
