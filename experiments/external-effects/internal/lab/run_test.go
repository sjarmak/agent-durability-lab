package lab

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidateOptionsRejectsUnsafeRunID(t *testing.T) {
	t.Parallel()
	executable := filepath.Join(t.TempDir(), "tool")
	if err := writeFileAtomically(executable, []byte("tool")); err != nil {
		t.Fatalf("write executable: %v", err)
	}
	if err := os.Chmod(executable, 0o700); err != nil {
		t.Fatalf("set executable: %v", err)
	}
	err := validateOptions(Options{
		Destination: DestinationDatabase, Mode: ModeProtected,
		TemporalPath: executable, WorkerBinary: executable, OutputRoot: t.TempDir(), RunID: "../escape",
	})
	if err == nil {
		t.Fatal("unsafe run ID passed validation")
	}
}

func TestValidateOptionsAcceptsExecutableInputsAndRejectsNonExecutableFiles(t *testing.T) {
	t.Parallel()
	executable := filepath.Join(t.TempDir(), "tool")
	if err := writeFileAtomically(executable, []byte("tool")); err != nil {
		t.Fatalf("write executable: %v", err)
	}
	if err := os.Chmod(executable, 0o700); err != nil {
		t.Fatalf("set executable: %v", err)
	}
	options := Options{
		Destination: DestinationArtifact, Mode: ModeUnsafe,
		TemporalPath: executable, WorkerBinary: executable, OutputRoot: t.TempDir(), RunID: "safe-run_1",
	}
	if err := validateOptions(options); err != nil {
		t.Fatalf("valid options: %v", err)
	}
	nonExecutable := filepath.Join(t.TempDir(), "not-executable")
	if err := writeFileAtomically(nonExecutable, []byte("tool")); err != nil {
		t.Fatalf("write non-executable: %v", err)
	}
	options.WorkerBinary = nonExecutable
	if err := validateOptions(options); err == nil {
		t.Fatal("non-executable Worker binary passed validation")
	}
	options.WorkerBinary = filepath.Join(t.TempDir(), "missing")
	if err := validateOptions(options); err == nil {
		t.Fatal("missing Worker binary passed validation")
	}
}
