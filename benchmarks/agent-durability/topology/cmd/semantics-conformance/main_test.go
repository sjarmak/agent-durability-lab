package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRequireDisjointRootsRejectsOverlapAndResolvesSymlinks(t *testing.T) {
	root := t.TempDir()
	if err := requireDisjointRoots(filepath.Join(root, "evidence"), filepath.Join(root, "work")); err != nil {
		t.Fatal(err)
	}
	if err := requireDisjointRoots(filepath.Join(root, "nested"), filepath.Join(root, "nested", "work")); err == nil {
		t.Fatal("nested work root was accepted")
	}
	link := filepath.Join(t.TempDir(), "evidence-link")
	linked := filepath.Join(root, "linked")
	if err := os.MkdirAll(linked, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(linked, link); err != nil {
		t.Fatal(err)
	}
	if err := requireDisjointRoots(link, filepath.Join(linked, "work")); err == nil {
		t.Fatal("symlink-overlapping roots were accepted")
	}
}
