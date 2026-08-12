package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const schemaVersion = "1.0.0"

var schemaFiles = []string{
	"event.schema.json",
	"evidence.schema.json",
	"identity.schema.json",
	"transition.schema.json",
}

type manifest struct {
	SchemaVersion string            `json:"schema_version"`
	Files         map[string]string `json:"files"`
}

func main() {
	os.Exit(runMain(os.Stderr, "schema"))
}

func runMain(stderr io.Writer, schemaDirectory string) int {
	if err := run(schemaDirectory); err != nil {
		fmt.Fprintf(stderr, "generate schema manifest: %v\n", err)
		return 1
	}
	return 0
}

func run(schemaDirectory string) error {
	hashes := make(map[string]string, len(schemaFiles))
	for _, name := range schemaFiles {
		contents, err := os.ReadFile(filepath.Join(schemaDirectory, name))
		if err != nil {
			return fmt.Errorf("read %s: %w", name, err)
		}
		sum := sha256.Sum256(contents)
		hashes[name] = "sha256:" + hex.EncodeToString(sum[:])
	}
	contents, err := json.MarshalIndent(manifest{SchemaVersion: schemaVersion, Files: hashes}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode manifest: %w", err)
	}
	contents = append(contents, '\n')
	if err := os.WriteFile(filepath.Join(schemaDirectory, "schema-manifest.json"), contents, 0o644); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	return nil
}
