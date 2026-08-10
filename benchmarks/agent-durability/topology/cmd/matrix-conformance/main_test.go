package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/matrix"
)

func TestRequireDisjointPathsRejectsNestedAndSymlinkedRoots(t *testing.T) {
	root := t.TempDir()
	if err := matrix.ValidateDisjointPaths(filepath.Join(root, "evidence"), filepath.Join(root, "work")); err != nil {
		t.Fatal(err)
	}
	if err := matrix.ValidateDisjointPaths(filepath.Join(root, "nested"), filepath.Join(root, "nested", "work")); err == nil {
		t.Fatal("nested roots were accepted")
	}
	linked := filepath.Join(root, "linked")
	if err := os.MkdirAll(linked, 0o750); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(linked, link); err != nil {
		t.Fatal(err)
	}
	if err := matrix.ValidateDisjointPaths(filepath.Join(link, "evidence"), filepath.Join(linked, "evidence", "work")); err == nil {
		t.Fatal("symlink-overlapping roots were accepted")
	}
}
