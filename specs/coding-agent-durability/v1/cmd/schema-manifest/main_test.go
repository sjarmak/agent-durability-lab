package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunWritesReproducibleManifest(t *testing.T) {
	directory := t.TempDir()
	wantHashes := make(map[string]string, len(schemaFiles))
	for index, name := range schemaFiles {
		contents := []byte(strings.Repeat(string(rune('a'+index)), index+1))
		if err := os.WriteFile(filepath.Join(directory, name), contents, 0o644); err != nil {
			t.Fatalf("write input schema %s: %v", name, err)
		}
		sum := sha256.Sum256(contents)
		wantHashes[name] = "sha256:" + hex.EncodeToString(sum[:])
	}

	if err := run(directory); err != nil {
		t.Fatalf("first generation failed: %v", err)
	}
	first := readManifest(t, directory)
	if err := run(directory); err != nil {
		t.Fatalf("second generation failed: %v", err)
	}
	second := readManifest(t, directory)
	if !bytes.Equal(first, second) {
		t.Fatalf("repeated generation changed bytes:\nfirst: %s\nsecond: %s", first, second)
	}

	var got manifest
	if err := json.Unmarshal(first, &got); err != nil {
		t.Fatalf("decode generated manifest: %v", err)
	}
	if got.SchemaVersion != schemaVersion {
		t.Errorf("schema version = %q; want %q", got.SchemaVersion, schemaVersion)
	}
	for name, want := range wantHashes {
		if got.Files[name] != want {
			t.Errorf("hash for %s = %q; want %q", name, got.Files[name], want)
		}
	}
}

func TestRunReportsMissingSchema(t *testing.T) {
	err := run(t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "read event.schema.json") {
		t.Fatalf("run error = %v; want missing-schema context", err)
	}
}

func TestRunReportsManifestWriteFailure(t *testing.T) {
	directory := t.TempDir()
	for _, name := range schemaFiles {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(name), 0o644); err != nil {
			t.Fatalf("write input schema %s: %v", name, err)
		}
	}
	if err := os.Mkdir(filepath.Join(directory, "schema-manifest.json"), 0o755); err != nil {
		t.Fatalf("create blocking output directory: %v", err)
	}
	err := run(directory)
	if err == nil || !strings.Contains(err.Error(), "write manifest") {
		t.Fatalf("run error = %v; want manifest-write context", err)
	}
}

func TestRunMainReturnsSuccess(t *testing.T) {
	directory := t.TempDir()
	for _, name := range schemaFiles {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(name), 0o644); err != nil {
			t.Fatalf("write input schema %s: %v", name, err)
		}
	}
	var stderr bytes.Buffer
	if code := runMain(&stderr, directory); code != 0 {
		t.Fatalf("runMain code = %d, stderr = %q; want success", code, stderr.String())
	}
}

func TestRunMainReportsFailure(t *testing.T) {
	var stderr bytes.Buffer
	if code := runMain(&stderr, t.TempDir()); code != 1 {
		t.Fatalf("runMain code = %d; want 1", code)
	}
	if !strings.Contains(stderr.String(), "generate schema manifest: read event.schema.json") {
		t.Fatalf("runMain stderr = %q; want contextual error", stderr.String())
	}
}

func readManifest(t *testing.T, directory string) []byte {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join(directory, "schema-manifest.json"))
	if err != nil {
		t.Fatalf("read generated manifest: %v", err)
	}
	return contents
}
