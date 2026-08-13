package lab

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"go.temporal.io/sdk/converter"
)

func TestValidateManifestInventoryRejectsExtraAndTamperedFiles(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	path := filepath.Join(root, "evidence.json")
	if err := os.WriteFile(path, []byte("original\n"), 0o600); err != nil {
		t.Fatalf("write evidence: %v", err)
	}
	files := map[string]string{"evidence.json": digestBytes([]byte("original\n"))}
	if err := validateManifestInventory(root, files); err != nil {
		t.Fatalf("valid inventory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "extra.json"), []byte("extra\n"), 0o600); err != nil {
		t.Fatalf("write extra: %v", err)
	}
	if err := validateManifestInventory(root, files); err == nil {
		t.Fatal("extra evidence file accepted")
	}
	if err := os.Remove(filepath.Join(root, "extra.json")); err != nil {
		t.Fatalf("remove extra: %v", err)
	}
	if err := os.WriteFile(path, []byte("tampered\n"), 0o600); err != nil {
		t.Fatalf("tamper evidence: %v", err)
	}
	if err := validateManifestInventory(root, files); err == nil {
		t.Fatal("tampered evidence file accepted")
	}
}

func TestAuditBundleRejectsSymlinksAndInventoryDrift(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	if _, _, err := loadAuditBundle(root); err == nil {
		t.Fatal("bundle without manifest accepted")
	}
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(root, alias); err != nil {
		t.Fatalf("create evidence-root symlink: %v", err)
	}
	if _, _, err := loadAuditBundle(alias); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("symlinked root error = %v, want ErrInvalidArtifact", err)
	}

	manifest := Manifest{SchemaVersion: 1, Experiment: "large-artifact-durability", RunID: filepath.Base(root),
		Boundary: BoundaryReferencePublished, Mode: ModeProtected, Files: map[string]string{}, Directories: []string{"nested"}}
	manifestData, err := encodeRecord(manifest)
	if err != nil {
		t.Fatalf("encode manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "manifest.json"), manifestData, 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if _, _, err := loadAuditBundle(root); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("directory drift error = %v, want ErrArtifactConflict", err)
	}
	if err := os.Mkdir(filepath.Join(root, "nested"), 0o750); err != nil {
		t.Fatalf("create nested directory: %v", err)
	}
	if err := os.Symlink(filepath.Join(root, "manifest.json"), filepath.Join(root, "nested", "link")); err != nil {
		t.Fatalf("create evidence symlink: %v", err)
	}
	if _, _, err := loadAuditBundle(root); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("evidence symlink error = %v, want ErrInvalidArtifact", err)
	}
}

func TestBundleStorageDriverFailsClosed(t *testing.T) {
	t.Parallel()

	driver := bundleStorageDriver{mode: ModeProtected, files: map[string][]byte{}}
	if _, err := driver.Store(converter.StorageDriverStoreContext{}, nil); err == nil {
		t.Fatal("audit replay Store accepted")
	}
	if _, err := driver.Retrieve(converter.StorageDriverRetrieveContext{}, []converter.StorageDriverClaim{{}}); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("nil-context Retrieve error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := driver.Retrieve(converter.StorageDriverRetrieveContext{Context: canceled}, []converter.StorageDriverClaim{{}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Retrieve error = %v", err)
	}
	claim := converter.StorageDriverClaim{ClaimData: map[string]string{
		"key": "missing.payload", "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "size": "1",
	}}
	if _, err := driver.Retrieve(converter.StorageDriverRetrieveContext{Context: context.Background()}, []converter.StorageDriverClaim{claim}); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("missing object error = %v, want ErrArtifactConflict", err)
	}
}

func TestDecodeStrictJSONRejectsDuplicateAndTrailingValues(t *testing.T) {
	t.Parallel()

	for _, data := range [][]byte{
		[]byte(`{"run_valid":true,"run_valid":false}`),
		[]byte(`{"run_valid":true}{"run_valid":true}`),
	} {
		var verdict Verdict
		if err := decodeStrictJSON(data, &verdict); err == nil {
			t.Fatalf("invalid JSON accepted: %s", data)
		}
	}
}

func TestAuditRunFromEnvironment(t *testing.T) {
	root := os.Getenv("LARGE_ARTIFACT_AUDIT_ROOT")
	if root == "" {
		t.Skip("set LARGE_ARTIFACT_AUDIT_ROOT to audit a preserved run")
	}
	verdict, err := AuditRun(root)
	if err != nil {
		t.Fatalf("AuditRun: %v", err)
	}
	if !verdict.RunValid || !verdict.ExpectedObservation {
		t.Fatalf("verdict = %+v", verdict)
	}
}
