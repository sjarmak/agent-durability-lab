package lab

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestExecutableIdentityRejectsSymlinkAndMutation(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "executable")
	if err := os.WriteFile(path, []byte("binary"), 0o700); err != nil {
		t.Fatalf("write executable: %v", err)
	}
	digest, size, err := executableIdentity(path)
	if err != nil || len(digest) != 64 || size != 6 {
		t.Fatalf("executableIdentity = %q, %d, %v", digest, size, err)
	}
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(path, alias); err != nil {
		t.Fatalf("create executable symlink: %v", err)
	}
	if _, _, err := executableIdentity(alias); err == nil {
		t.Fatal("symlinked executable accepted")
	}
}

func TestRuntimeProvenanceValidationFailsClosed(t *testing.T) {
	t.Parallel()

	valid := testRuntimeProvenance(t)
	if err := validateRuntimeProvenance(valid); err != nil {
		t.Fatalf("valid provenance: %v", err)
	}
	for name, mutate := range map[string]func(*RuntimeProvenance){
		"digest":    func(value *RuntimeProvenance) { value.WorkerSHA256 = strings.Repeat("z", 64) },
		"timestamp": func(value *RuntimeProvenance) { value.CapturedAt = time.Time{} },
		"version":   func(value *RuntimeProvenance) { value.TemporalVersion = "" },
		"size":      func(value *RuntimeProvenance) { value.WorkerBytes = 0 },
	} {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			changed := valid
			mutate(&changed)
			if err := validateRuntimeProvenance(changed); err == nil {
				t.Fatal("invalid provenance accepted")
			}
		})
	}
}

func TestCurrentRuntimePreregistrationRejectsWellFormedForgedDigest(t *testing.T) {
	registered, err := LoadRuntimePreregistration()
	if err != nil {
		t.Fatalf("LoadRuntimePreregistration: %v", err)
	}
	valid := registered.Provenance(time.Now().UTC())
	if err := ValidateCurrentRuntimeProvenance(valid); err != nil {
		t.Fatalf("registered provenance: %v", err)
	}
	coverage := valid
	coverage.WorkerSHA256 = registered.CoverageWorkerSHA256
	coverage.WorkerBytes = registered.CoverageWorkerBytes
	if err := ValidateCurrentRuntimeProvenance(coverage); err != nil {
		t.Fatalf("registered coverage provenance: %v", err)
	}
	forged := valid
	forged.WorkerSHA256 = strings.Repeat("c", 64)
	if err := ValidateCurrentRuntimeProvenance(forged); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("forged provenance error = %v, want ErrArtifactConflict", err)
	}
}

func TestCaptureRuntimeProvenancePropagatesVersionFailure(t *testing.T) {
	t.Parallel()

	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	_, err = CaptureRuntimeProvenance(context.Background(), executable, executable)
	if err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("CaptureRuntimeProvenance error = %v", err)
	}
}

func testRuntimeProvenance(t *testing.T) RuntimeProvenance {
	t.Helper()
	registered, err := LoadRuntimePreregistration()
	if err != nil {
		t.Fatalf("LoadRuntimePreregistration: %v", err)
	}
	return registered.Provenance(time.Now().UTC())
}
