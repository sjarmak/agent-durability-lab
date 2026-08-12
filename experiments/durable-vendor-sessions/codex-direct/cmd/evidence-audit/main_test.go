package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sjarmak/temporal_projects/experiments/durable-vendor-sessions/codex-direct/internal/lab"
)

func TestRunWritesAuditOutsideSealedEvidence(t *testing.T) {
	parent := t.TempDir()
	evidence := filepath.Join(parent, "evidence")
	if err := os.Mkdir(evidence, 0o750); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(parent, "audit.json")
	called := ""
	err := run(context.Background(), []string{"--evidence", evidence, "--output", output}, &bytes.Buffer{},
		func(_ context.Context, root string) (lab.EvidenceAudit, error) {
			called = root
			return lab.EvidenceAudit{Version: "fixture", AllRequirementsVerified: true}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if called != evidence {
		t.Fatalf("audited %q, want %q", called, evidence)
	}
	if _, err := os.Stat(output); err != nil {
		t.Fatal(err)
	}
}

func TestRunRejectsOutputInsideEvidence(t *testing.T) {
	root := t.TempDir()
	if err := run(context.Background(), []string{
		"--evidence", root, "--output", filepath.Join(root, "audit.json"),
	}, &bytes.Buffer{}, func(context.Context, string) (lab.EvidenceAudit, error) {
		t.Fatal("audit must not run")
		return lab.EvidenceAudit{}, nil
	}); err == nil {
		t.Fatal("inside-root audit output was accepted")
	}
}

func TestRunRejectsSymlinkedOutputParentIntoEvidence(t *testing.T) {
	evidence := t.TempDir()
	alias := filepath.Join(t.TempDir(), "output-alias")
	if err := os.Symlink(evidence, alias); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(alias, "escaped-audit.json")
	if err := run(context.Background(), []string{
		"--evidence", evidence, "--output", output,
	}, &bytes.Buffer{}, func(context.Context, string) (lab.EvidenceAudit, error) {
		t.Fatal("audit must not run")
		return lab.EvidenceAudit{}, nil
	}); err == nil {
		t.Fatal("symlinked output parent into evidence was accepted")
	}
	if _, err := os.Lstat(filepath.Join(evidence, "escaped-audit.json")); !os.IsNotExist(err) {
		t.Fatalf("audit escaped into sealed evidence: %v", err)
	}
}

func TestRunPinsValidatedOutputDirectoryAcrossAudit(t *testing.T) {
	evidence := t.TempDir()
	parent := t.TempDir()
	safeOutputDirectory := filepath.Join(parent, "safe-output")
	if err := os.Mkdir(safeOutputDirectory, 0o750); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(parent, "output-alias")
	if err := os.Symlink(safeOutputDirectory, alias); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(alias, "audit.json")
	err := run(context.Background(), []string{"--evidence", evidence, "--output", output}, &bytes.Buffer{},
		func(context.Context, string) (lab.EvidenceAudit, error) {
			if err := os.Remove(alias); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(evidence, alias); err != nil {
				t.Fatal(err)
			}
			return lab.EvidenceAudit{Version: "fixture", AllRequirementsVerified: true}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(evidence, "audit.json")); !os.IsNotExist(err) {
		t.Fatalf("audit write escaped through swapped output parent: %v", err)
	}
	if _, err := os.Stat(filepath.Join(safeOutputDirectory, "audit.json")); err != nil {
		t.Fatalf("audit was not written through the pinned safe directory: %v", err)
	}
}
