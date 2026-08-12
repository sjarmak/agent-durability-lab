package evidence

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	legacyprotocol "github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

func TestReportPinsExecutableAndVerifiedSchemaInventory(t *testing.T) {
	t.Parallel()

	executable, sourceRoot, schemaRoot := writePinFixture(t)
	if _, err := CapturePins(t.TempDir(), sourceRoot, schemaRoot); !errors.Is(err, legacyprotocol.ErrInvalidEvidence) {
		t.Fatalf("directory executable error = %v, want ErrInvalidEvidence", err)
	}
	pins, err := CapturePins(executable, sourceRoot, schemaRoot)
	if err != nil {
		t.Fatalf("capture pins: %v", err)
	}
	if len(pins.Schemas) != 4 || pins.Executable.SHA256 == "" || len(pins.Sources) != 2 || pins.ConfigurationSHA256 == "" {
		t.Fatalf("pins = %+v, want executable, two sources, four schemas, and configuration", pins)
	}

	if err := os.WriteFile(filepath.Join(schemaRoot, "event.schema.json"), []byte("changed"), 0o600); err != nil {
		t.Fatalf("change schema: %v", err)
	}
	if _, err := CapturePins(executable, sourceRoot, schemaRoot); err == nil {
		t.Fatal("CapturePins accepted schema bytes that differ from the manifest")
	}
}

func TestCapturePinsRejectsIncompleteAndAmbiguousInputs(t *testing.T) {
	t.Parallel()

	if _, err := CapturePins("", "", ""); err == nil {
		t.Fatal("CapturePins accepted empty paths")
	}
	executable, sourceRoot, schemaRoot := writePinFixture(t)
	manifestPath := filepath.Join(schemaRoot, "schema-manifest.json")
	manifest, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	manifest = []byte(strings.Replace(string(manifest), `"schema_version": "1.0.0"`, `"schema_version": "1.0.0", "schema_version": "1.0.0"`, 1))
	if err := os.WriteFile(manifestPath, manifest, 0o600); err != nil {
		t.Fatalf("write ambiguous manifest: %v", err)
	}
	if _, err := CapturePins(executable, sourceRoot, schemaRoot); err == nil || !strings.Contains(err.Error(), "canonical") {
		t.Fatalf("CapturePins error = %v, want canonical-manifest rejection", err)
	}
}

func TestValidatePinsRejectsIncompleteAndMalformedDigests(t *testing.T) {
	t.Parallel()

	if err := ValidatePins(Pins{}); err == nil {
		t.Fatal("ValidatePins accepted empty inventory")
	}
	executable, sourceRoot, schemaRoot := writePinFixture(t)
	pins, err := CapturePins(executable, sourceRoot, schemaRoot)
	if err != nil {
		t.Fatalf("capture pins: %v", err)
	}
	pins.Executable.SHA256 = strings.Repeat("z", 64)
	if err := ValidatePins(pins); err == nil {
		t.Fatal("ValidatePins accepted non-hex digest")
	}
}

func TestPreserveExecutablePublishesAndVerifiesExactPinnedBytes(t *testing.T) {
	t.Parallel()

	executable, sourceRoot, schemaRoot := writePinFixture(t)
	pins, err := CapturePins(executable, sourceRoot, schemaRoot)
	if err != nil {
		t.Fatalf("capture pins: %v", err)
	}
	root := t.TempDir()
	path, err := PreserveExecutable(context.Background(), root, executable, pins.Executable)
	if err != nil {
		t.Fatalf("preserve executable: %v", err)
	}
	if path != filepath.Join(root, filepath.FromSlash(ExecutableArtifactPath)) {
		t.Fatalf("preserved path = %q", path)
	}
	if err := VerifyPreservedExecutable(root, pins.Executable); err != nil {
		t.Fatalf("verify preserved executable: %v", err)
	}
	if _, err := PreserveExecutable(context.Background(), root, executable, pins.Executable); !errors.Is(err, legacyprotocol.ErrEvidenceExists) {
		t.Fatalf("second preserve error = %v, want ErrEvidenceExists", err)
	}
	if err := os.WriteFile(path, []byte("tampered"), 0o600); err != nil {
		t.Fatalf("tamper preserved executable: %v", err)
	}
	if err := VerifyPreservedExecutable(root, pins.Executable); err == nil {
		t.Fatal("VerifyPreservedExecutable accepted tampered bytes")
	}
}

func TestReportWriterRefusesOverwriteAndOmitsComparativeMetrics(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	executable, sourceRoot, schemaRoot := writePinFixture(t)
	pins, err := CapturePins(executable, sourceRoot, schemaRoot)
	if err != nil {
		t.Fatalf("capture pins: %v", err)
	}
	report := Report{
		ContractVersion: ContractVersion,
		ProfileKind:     CalibrationProfileKind,
		Status:          StatusNonconformant,
		ClaimBoundary:   CalibrationClaimBoundary,
		Pins:            pins,
	}
	path, err := WriteReport(context.Background(), root, report)
	if err != nil {
		t.Fatalf("write report: %v", err)
	}
	if _, err := WriteReport(context.Background(), root, report); !errors.Is(err, legacyprotocol.ErrEvidenceExists) {
		t.Fatalf("second write error = %v, want ErrEvidenceExists", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	for _, forbidden := range []string{"score", "metric", "latency", "duration", "percentile", "confidence", "rate"} {
		if strings.Contains(strings.ToLower(string(data)), forbidden) {
			t.Errorf("report contains forbidden comparative field %q: %s", forbidden, data)
		}
	}
}

func TestReportWriterRejectsCancellationAndIncompleteReport(t *testing.T) {
	t.Parallel()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := WriteReport(canceled, t.TempDir(), Report{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled write error = %v", err)
	}
	if _, err := WriteReport(context.Background(), t.TempDir(), Report{}); !errors.Is(err, legacyprotocol.ErrInvalidEvidence) {
		t.Fatalf("incomplete write error = %v, want ErrInvalidEvidence", err)
	}
	executable, sourceRoot, schemaRoot := writePinFixture(t)
	pins, err := CapturePins(executable, sourceRoot, schemaRoot)
	if err != nil {
		t.Fatalf("capture pins: %v", err)
	}
	emptyConformant := Report{
		ContractVersion: ContractVersion, ProfileKind: CalibrationProfileKind,
		Status: StatusConformant, ClaimBoundary: CalibrationClaimBoundary, Pins: pins,
	}
	if _, err := WriteReport(context.Background(), t.TempDir(), emptyConformant); !errors.Is(err, legacyprotocol.ErrInvalidEvidence) {
		t.Fatalf("empty conformant write error = %v, want ErrInvalidEvidence", err)
	}
}

func writePinFixture(t *testing.T) (string, string, string) {
	t.Helper()
	root := t.TempDir()
	executable := filepath.Join(root, "conformance")
	if err := os.WriteFile(executable, []byte("executable"), 0o700); err != nil {
		t.Fatalf("write executable: %v", err)
	}
	sourceRoot, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatalf("resolve source root: %v", err)
	}
	schemaRoot := filepath.Join(root, "schema")
	if err := os.Mkdir(schemaRoot, 0o750); err != nil {
		t.Fatalf("create schema directory: %v", err)
	}
	canonicalSchemaRoot := filepath.Join(sourceRoot, "specs/coding-agent-durability/v1/schema")
	for _, name := range append(SchemaFiles(), "schema-manifest.json") {
		data, err := os.ReadFile(filepath.Join(canonicalSchemaRoot, name))
		if err != nil {
			t.Fatalf("read canonical schema %s: %v", name, err)
		}
		if err := os.WriteFile(filepath.Join(schemaRoot, name), data, 0o600); err != nil {
			t.Fatalf("write schema: %v", err)
		}
	}
	return executable, sourceRoot, schemaRoot
}
