package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRunWritesMergedProfileAtomically(t *testing.T) {
	directory := t.TempDir()
	first := filepath.Join(directory, "first.out")
	second := filepath.Join(directory, "second.out")
	output := filepath.Join(directory, "merged.out")
	if err := os.WriteFile(first, []byte("mode: atomic\na.go:1.1,1.2 1 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(second, []byte("mode: atomic\na.go:1.1,1.2 1 2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"--output", output, first, second}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if want := "mode: atomic\na.go:1.1,1.2 1 3\n"; string(got) != want {
		t.Fatalf("merged profile = %q, want %q", got, want)
	}
}

func TestRunDoesNotReplaceOutputOnInvalidInput(t *testing.T) {
	directory := t.TempDir()
	input := filepath.Join(directory, "invalid.out")
	output := filepath.Join(directory, "merged.out")
	if err := os.WriteFile(input, []byte("not a profile\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(output, []byte("preserved\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"--output", output, input}); err == nil {
		t.Fatal("invalid profile was accepted")
	}
	got, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "preserved\n" {
		t.Fatalf("output changed after failed merge: %q", got)
	}
}
