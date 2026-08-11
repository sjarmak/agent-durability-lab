package lab

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNormalizeExperimentOptionsMakesFilesystemInputsAbsolute(t *testing.T) {
	options, err := normalizeExperimentOptions(ExperimentOptions{
		EvidenceRoot: "evidence", TemporalPath: "bin/temporal", WorkerBinary: "bin/worker",
		EffectBinary: "bin/effect", LauncherBinary: "bin/launcher", ClaudeBinary: "bin/claude",
	})
	if err != nil {
		t.Fatalf("normalize options: %v", err)
	}
	for name, path := range map[string]string{
		"evidence": options.EvidenceRoot, "temporal": options.TemporalPath,
		"worker": options.WorkerBinary, "effect": options.EffectBinary,
		"launcher": options.LauncherBinary, "claude": options.ClaudeBinary,
	} {
		if !filepath.IsAbs(path) || !strings.HasSuffix(path, filepath.Join("bin", filepath.Base(path))) && name != "evidence" {
			t.Fatalf("%s path = %q, want absolute normalized path", name, path)
		}
	}
}

func TestValidateOptionsRequiresPinnedExecutableInputsAndNewEvidenceRoot(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	binary := filepath.Join(directory, "binary")
	if err := os.WriteFile(binary, []byte("binary"), 0o700); err != nil {
		t.Fatalf("write binary: %v", err)
	}
	options := ExperimentOptions{
		EvidenceRoot: filepath.Join(directory, "evidence"), TemporalPath: binary,
		WorkerBinary: binary, EffectBinary: binary, LauncherBinary: binary, ClaudeBinary: binary,
		Trials: 3, Timeout: time.Minute, Model: "haiku", MaxBudgetUSD: "0.25", MaxTurns: 2,
	}
	if err := validateExperimentOptions(options); err != nil {
		t.Fatalf("validate options: %v", err)
	}
	invalidBudget := options
	invalidBudget.EvidenceRoot = filepath.Join(directory, "invalid-budget-evidence")
	invalidBudget.MaxBudgetUSD = "NaN"
	if err := validateExperimentOptions(invalidBudget); err == nil {
		t.Fatal("non-finite budget returned nil error")
	}
	nonExecutable := filepath.Join(directory, "non-executable")
	if err := os.WriteFile(nonExecutable, []byte("data"), 0o600); err != nil {
		t.Fatalf("write non-executable: %v", err)
	}
	invalidBinary := options
	invalidBinary.EvidenceRoot = filepath.Join(directory, "invalid-binary-evidence")
	invalidBinary.ClaudeBinary = nonExecutable
	if err := validateExperimentOptions(invalidBinary); err == nil {
		t.Fatal("non-executable binary returned nil error")
	}
	if err := os.Mkdir(options.EvidenceRoot, 0o750); err != nil {
		t.Fatalf("create evidence root: %v", err)
	}
	if err := validateExperimentOptions(options); err == nil {
		t.Fatal("existing evidence root returned nil error")
	}
	if err := validateExperimentOptions(ExperimentOptions{}); err == nil {
		t.Fatal("empty options returned nil error")
	}
}
