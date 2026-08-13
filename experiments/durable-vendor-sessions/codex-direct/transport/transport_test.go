package transport

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

func TestBuildVerifyRestoreIsDeterministic(t *testing.T) {
	source, lineagePath := writeTransportFixture(t)
	parent := t.TempDir()
	first := filepath.Join(parent, "transport-one")
	second := filepath.Join(parent, "transport-two")
	firstIndex, err := Build(context.Background(), BuildConfig{SourceRoot: source, LineagePath: lineagePath, OutputRoot: first})
	if err != nil {
		t.Fatal(err)
	}
	secondIndex, err := Build(context.Background(), BuildConfig{SourceRoot: source, LineagePath: lineagePath, OutputRoot: second})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(firstIndex, secondIndex) {
		t.Fatalf("deterministic indexes differ: %+v / %+v", firstIndex, secondIndex)
	}
	for _, name := range transportFiles(t, first) {
		firstData, err := os.ReadFile(filepath.Join(first, name))
		if err != nil {
			t.Fatal(err)
		}
		secondData, err := os.ReadFile(filepath.Join(second, name))
		if err != nil {
			t.Fatal(err)
		}
		if string(firstData) != string(secondData) {
			t.Fatalf("transport artifact %s is not deterministic", name)
		}
	}
	if _, err := Verify(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(parent, "restored")
	if err := Restore(context.Background(), first, restored); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(restored, "bundle-v1", "run-1", "fixture", ".git", "HEAD")); err != nil {
		t.Fatalf("nested Git evidence was not restored: %v", err)
	}
	if _, err := os.Stat(filepath.Join(restored, "bundle-v1-audit.json")); err != nil {
		t.Fatalf("bound audit was not restored: %v", err)
	}
}

func TestRestorePreservesManifestModesUnderRestrictiveUmask(t *testing.T) {
	const helperEnvironment = "CODEX_TRANSPORT_RESTRICTIVE_UMASK_HELPER"
	if os.Getenv(helperEnvironment) == "1" {
		if err := Restore(
			context.Background(),
			os.Getenv("CODEX_TRANSPORT_ROOT"),
			os.Getenv("CODEX_TRANSPORT_DESTINATION"),
		); err != nil {
			t.Fatal(err)
		}
		return
	}

	source, lineagePath := writeTransportFixture(t)
	writableArtifact := filepath.Join(source, "bundle-v1", "run-1", "fixture", ".git", "HEAD")
	if err := os.Chmod(writableArtifact, 0o664); err != nil {
		t.Fatal(err)
	}
	transportRoot := filepath.Join(t.TempDir(), "transport")
	if _, err := Build(context.Background(), BuildConfig{
		SourceRoot: source, LineagePath: lineagePath, OutputRoot: transportRoot,
	}); err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(t.TempDir(), "restored")
	command := exec.Command("sh", "-c", `umask 0022; exec "$1" -test.run=^TestRestorePreservesManifestModesUnderRestrictiveUmask$`, "sh", os.Args[0])
	command.Env = append(os.Environ(),
		helperEnvironment+"=1",
		"CODEX_TRANSPORT_ROOT="+transportRoot,
		"CODEX_TRANSPORT_DESTINATION="+restored,
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("restore with restrictive umask: %v\n%s", err, output)
	}
	info, err := os.Stat(filepath.Join(restored, "bundle-v1", "run-1", "fixture", ".git", "HEAD"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o664 {
		t.Fatalf("restored mode = %04o, want 0664", info.Mode().Perm())
	}
}

func TestBuildVerifyRestorePreservesRejectedFailureWithoutSuccessAudit(t *testing.T) {
	source, lineagePath := writeTransportFixture(t)
	if err := os.Rename(filepath.Join(source, "bundle-v1"), filepath.Join(source, "bundle-v2")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(source, "bundle-v1-audit.json"), filepath.Join(source, "bundle-v2-audit.json")); err != nil {
		t.Fatal(err)
	}
	rejected := filepath.Join(source, "bundle-v1")
	if err := os.Mkdir(rejected, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := writeJSONExclusive(filepath.Join(rejected, "failure.json"), map[string]any{
		"recorded_at": "2026-08-12T03:31:56Z", "error": "missing command execution", "preserved": true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rejected, "raw-stdout.jsonl"), []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	failureHash, err := hashFile(filepath.Join(rejected, "failure.json"), maxJSONBytes)
	if err != nil {
		t.Fatal(err)
	}
	rejectionAudit := "bundle-v1-rejection-audit.json"
	if err := writeJSONExclusive(filepath.Join(source, rejectionAudit), rejectionAuditReport{
		Version: rejectionAuditVersion, EvidenceRoot: rejected,
		FailureSHA256: failureHash, FailurePreserved: true,
	}); err != nil {
		t.Fatal(err)
	}
	lineage := Lineage{SchemaVersion: LineageVersion, Entries: []LineageEntry{
		{Bundle: "bundle-v1", Audit: rejectionAudit, Disposition: DispositionRejected, Reason: "provider claimed success without executing the effect"},
		{Bundle: "bundle-v2", Audit: "bundle-v2-audit.json", Disposition: DispositionAdmitted, Reason: "corrected command-bound population"},
	}}
	if err := os.Remove(lineagePath); err != nil {
		t.Fatal(err)
	}
	if err := writeJSONExclusive(lineagePath, lineage); err != nil {
		t.Fatal(err)
	}
	transportRoot := filepath.Join(t.TempDir(), "transport")
	index, err := Build(context.Background(), BuildConfig{
		SourceRoot: source, LineagePath: lineagePath, OutputRoot: transportRoot,
	})
	if err != nil {
		t.Fatalf("build rejected lineage: %v", err)
	}
	if len(index.Bundles) != 2 || index.Bundles[0].Audit != rejectionAudit || index.Bundles[0].AuditSHA256 == "" {
		t.Fatalf("rejected bundle lacks its rejection audit: %+v", index.Bundles)
	}
	if _, err := Verify(context.Background(), transportRoot); err != nil {
		t.Fatalf("verify rejected lineage: %v", err)
	}
	restored := filepath.Join(t.TempDir(), "restored")
	if err := Restore(context.Background(), transportRoot, restored); err != nil {
		t.Fatalf("restore rejected lineage: %v", err)
	}
	if _, err := os.Stat(filepath.Join(restored, "bundle-v1", "failure.json")); err != nil {
		t.Fatalf("rejected failure record was not restored: %v", err)
	}
}

func TestLineageRequiresSafeCompleteAcyclicDisposition(t *testing.T) {
	admitted := LineageEntry{
		Bundle: "bundle-v2", Audit: "bundle-v2-audit.json", Disposition: DispositionAdmitted,
		Reason: "current source evidence",
	}
	superseded := LineageEntry{
		Bundle: "bundle-v1", Audit: "bundle-v1-audit.json", Disposition: DispositionSuperseded,
		SupersededBy: admitted.Bundle, Reason: "corrected by v2",
	}
	if err := validLineage(Lineage{SchemaVersion: LineageVersion, Entries: []LineageEntry{superseded, admitted}}); err != nil {
		t.Fatalf("valid lineage: %v", err)
	}
	tests := []struct {
		name    string
		lineage Lineage
	}{
		{name: "unsupported", lineage: Lineage{}},
		{name: "unsafe-name", lineage: Lineage{SchemaVersion: LineageVersion,
			Entries: []LineageEntry{{Bundle: "../bundle", Audit: "audit.json", Disposition: DispositionAdmitted, Reason: "bad"}}}},
		{name: "duplicate", lineage: Lineage{SchemaVersion: LineageVersion, Entries: []LineageEntry{admitted, admitted}}},
		{name: "admitted-superseded", lineage: Lineage{SchemaVersion: LineageVersion,
			Entries: []LineageEntry{{Bundle: "bundle", Audit: "audit.json", Disposition: DispositionAdmitted,
				SupersededBy: "other", Reason: "bad"}}}},
		{name: "superseded-unsafe", lineage: Lineage{SchemaVersion: LineageVersion,
			Entries: []LineageEntry{{Bundle: "bundle", Audit: "audit.json", Disposition: DispositionSuperseded,
				SupersededBy: "../other", Reason: "bad"}}}},
		{name: "unknown-disposition", lineage: Lineage{SchemaVersion: LineageVersion,
			Entries: []LineageEntry{{Bundle: "bundle", Audit: "audit.json", Disposition: "unknown", Reason: "bad"}}}},
		{name: "missing-successor", lineage: Lineage{SchemaVersion: LineageVersion, Entries: []LineageEntry{superseded}}},
		{name: "non-final-admitted", lineage: Lineage{SchemaVersion: LineageVersion, Entries: []LineageEntry{
			admitted,
			{Bundle: "bundle-v3", Audit: "bundle-v3-audit.json", Disposition: DispositionAdmitted, Reason: "later"},
		}}},
		{name: "cycle", lineage: Lineage{SchemaVersion: LineageVersion, Entries: []LineageEntry{
			{Bundle: "bundle-v1", Audit: "bundle-v1-audit.json", Disposition: DispositionSuperseded,
				SupersededBy: "bundle-v2", Reason: "first"},
			{Bundle: "bundle-v2", Audit: "bundle-v2-audit.json", Disposition: DispositionSuperseded,
				SupersededBy: "bundle-v1", Reason: "second"},
		}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validLineage(test.lineage); !errors.Is(err, ErrInvalidTransport) {
				t.Fatalf("invalid lineage = %v", err)
			}
		})
	}
}

func TestManifestAndIndexValidationRejectBindingDrift(t *testing.T) {
	source, lineagePath := writeTransportFixture(t)
	root := filepath.Join(t.TempDir(), "transport")
	index, err := Build(context.Background(), BuildConfig{
		SourceRoot: source, LineagePath: lineagePath, OutputRoot: root,
	})
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := readJSON[BundleManifest](filepath.Join(root, index.Bundles[0].Manifest))
	if err != nil {
		t.Fatal(err)
	}
	cloneManifest := func() BundleManifest {
		clone := manifest
		clone.Files = append([]Artifact(nil), manifest.Files...)
		clone.Runs = append([]RunBinding(nil), manifest.Runs...)
		return clone
	}
	manifestTests := []struct {
		name   string
		mutate func(*BundleManifest)
	}{
		{name: "header", mutate: func(value *BundleManifest) { value.SchemaVersion = "wrong" }},
		{name: "disposition", mutate: func(value *BundleManifest) { value.Disposition = DispositionRejected }},
		{name: "artifact-order", mutate: func(value *BundleManifest) { value.Files[0].Path = "../escape" }},
		{name: "total", mutate: func(value *BundleManifest) { value.TotalBytes++ }},
		{name: "run", mutate: func(value *BundleManifest) { value.Runs[0].RunID = "../run" }},
		{name: "binding", mutate: func(value *BundleManifest) { value.Runs[0].SummarySHA256 = value.ArchiveSHA256 }},
	}
	for _, test := range manifestTests {
		t.Run("manifest-"+test.name, func(t *testing.T) {
			value := cloneManifest()
			test.mutate(&value)
			if err := validateManifest(value); !errors.Is(err, ErrInvalidTransport) {
				t.Fatalf("invalid manifest = %v", err)
			}
		})
	}
	invalidIndex := index
	invalidIndex.Bundles = append([]BundleEntry(nil), index.Bundles...)
	invalidIndex.Bundles[0].Manifest = "wrong.manifest.json"
	if err := validateIndex(invalidIndex); !errors.Is(err, ErrInvalidTransport) {
		t.Fatalf("invalid index = %v", err)
	}
	if err := validateIndex(Index{}); !errors.Is(err, ErrInvalidTransport) {
		t.Fatalf("empty index = %v", err)
	}
}

func TestTransportOperationsRejectCanceledAndExistingDestinations(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Build(ctx, BuildConfig{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled build = %v", err)
	}
	if _, err := Build(context.Background(), BuildConfig{}); !errors.Is(err, ErrInvalidTransport) {
		t.Fatalf("incomplete build = %v", err)
	}
	source, lineagePath := writeTransportFixture(t)
	existing := t.TempDir()
	if _, err := Build(context.Background(), BuildConfig{
		SourceRoot: source, LineagePath: lineagePath, OutputRoot: existing,
	}); !errors.Is(err, ErrDestinationExists) {
		t.Fatalf("existing transport destination = %v", err)
	}
	if _, err := Verify(ctx, source); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled verification = %v", err)
	}
	if _, err := Verify(context.Background(), filepath.Join(source, "missing")); !errors.Is(err, ErrInvalidTransport) {
		t.Fatalf("missing transport root = %v", err)
	}
}

func TestRestoreAndVerifyRejectUnsafeDestinationsAndFileSets(t *testing.T) {
	source, lineagePath := writeTransportFixture(t)
	parent := t.TempDir()
	root := filepath.Join(parent, "transport")
	if _, err := Build(context.Background(), BuildConfig{
		SourceRoot: source, LineagePath: lineagePath, OutputRoot: root,
	}); err != nil {
		t.Fatal(err)
	}
	if err := Restore(context.Background(), root, ""); !errors.Is(err, ErrInvalidTransport) {
		t.Fatalf("empty restoration destination = %v", err)
	}
	if err := Restore(context.Background(), root, parent); !errors.Is(err, ErrDestinationExists) {
		t.Fatalf("existing restoration destination = %v", err)
	}
	if err := Restore(context.Background(), root, filepath.Join(root, "restored")); !errors.Is(err, ErrInvalidTransport) {
		t.Fatalf("restoration inside transport = %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "unexpected"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(context.Background(), root); !errors.Is(err, ErrInvalidTransport) {
		t.Fatalf("transport with unexpected file = %v", err)
	}
}

func TestTransportJSONAndArtifactReadersFailClosed(t *testing.T) {
	type record struct {
		Value string `json:"value"`
	}
	root := t.TempDir()
	unknown := filepath.Join(root, "unknown.json")
	if err := os.WriteFile(unknown, []byte(`{"value":"ok","extra":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readJSON[record](unknown); err == nil {
		t.Fatal("unknown strict JSON field was accepted")
	}
	if value, err := readJSONProjection[record](unknown); err != nil || value.Value != "ok" {
		t.Fatalf("projection JSON = %+v, err=%v", value, err)
	}
	trailing := filepath.Join(root, "trailing.json")
	if err := os.WriteFile(trailing, []byte(`{"value":"ok"} {}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readJSON[record](trailing); err == nil {
		t.Fatal("trailing JSON was accepted")
	}
	if _, err := readJSON[record](root); !errors.Is(err, ErrInvalidTransport) {
		t.Fatalf("directory JSON = %v", err)
	}
	if _, err := hashFile(root, maxJSONBytes); !errors.Is(err, ErrInvalidTransport) {
		t.Fatalf("directory hash = %v", err)
	}
	if _, err := readArchiveBytes(root); !errors.Is(err, ErrInvalidTransport) {
		t.Fatalf("directory archive = %v", err)
	}
	missing := filepath.Join(root, "missing")
	if _, err := readJSON[record](missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing JSON = %v", err)
	}
	if _, err := hashFile(missing, maxJSONBytes); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing hash input = %v", err)
	}
	if _, err := readArchiveBytes(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing archive = %v", err)
	}
	parentFile := filepath.Join(root, "parent-file")
	if err := os.WriteFile(parentFile, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := newStagingDirectory(filepath.Join(parentFile, "destination")); err == nil {
		t.Fatal("staging directory under a regular file was created")
	}
}

func TestVerifyRejectsTrailingArchivePayload(t *testing.T) {
	source, lineagePath := writeTransportFixture(t)
	root := filepath.Join(t.TempDir(), "transport")
	if _, err := Build(context.Background(), BuildConfig{SourceRoot: source, LineagePath: lineagePath, OutputRoot: root}); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(root, "bundle-v1.tar.gz")
	file, err := os.OpenFile(archive, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("unmanifested")); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(context.Background(), root); err == nil {
		t.Fatal("transport with trailing archive payload verified")
	}
}

func TestBuildRejectsRawInventoryMismatch(t *testing.T) {
	source, lineagePath := writeTransportFixture(t)
	artifact := filepath.Join(source, "bundle-v1", "run-1", "fixture", ".git", "HEAD")
	if err := os.WriteFile(artifact, []byte("tampered\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Build(context.Background(), BuildConfig{
		SourceRoot: source, LineagePath: lineagePath, OutputRoot: filepath.Join(t.TempDir(), "transport"),
	}); err == nil {
		t.Fatal("raw inventory mismatch produced a transport")
	}
}

func TestBuildRejectsOutputInsideSourceBundle(t *testing.T) {
	source, lineagePath := writeTransportFixture(t)
	if _, err := Build(context.Background(), BuildConfig{
		SourceRoot: source, LineagePath: lineagePath,
		OutputRoot: filepath.Join(source, "bundle-v1", "transport"),
	}); err == nil {
		t.Fatal("transport output inside the source bundle was accepted")
	}
}

func TestBuildRejectsOverlappingTreesAndUnlistedSourceBundles(t *testing.T) {
	source, lineagePath := writeTransportFixture(t)
	if _, err := Build(context.Background(), BuildConfig{
		SourceRoot: source, LineagePath: lineagePath,
		OutputRoot: filepath.Join(source, "unlisted-output"),
	}); err == nil {
		t.Fatal("transport output elsewhere inside the source root was accepted")
	}
	parent := t.TempDir()
	if err := os.Mkdir(filepath.Join(parent, "output"), 0o750); err != nil {
		t.Fatal(err)
	}
	nestedSource := filepath.Join(parent, "output", "source")
	if err := os.Rename(source, nestedSource); err != nil {
		t.Fatal(err)
	}
	if _, err := Build(context.Background(), BuildConfig{
		SourceRoot: nestedSource, LineagePath: lineagePath, OutputRoot: filepath.Join(parent, "output"),
	}); err == nil {
		t.Fatal("transport output containing the source root was accepted")
	}

	source, lineagePath = writeTransportFixture(t)
	if err := os.Mkdir(filepath.Join(source, "unlisted-bundle"), 0o750); err != nil {
		t.Fatal(err)
	}
	if _, err := Build(context.Background(), BuildConfig{
		SourceRoot: source, LineagePath: lineagePath, OutputRoot: filepath.Join(t.TempDir(), "transport"),
	}); err == nil {
		t.Fatal("unlisted source bundle was silently omitted")
	}
}

func TestTransportRejectsSymlinkedDestinationParentsIntoProtectedTrees(t *testing.T) {
	source, lineagePath := writeTransportFixture(t)
	buildAlias := filepath.Join(t.TempDir(), "build-alias")
	if err := os.Symlink(source, buildAlias); err != nil {
		t.Fatal(err)
	}
	if _, err := Build(context.Background(), BuildConfig{
		SourceRoot: source, LineagePath: lineagePath, OutputRoot: filepath.Join(buildAlias, "escaped-transport"),
	}); err == nil {
		t.Fatal("symlinked build destination parent into source was accepted")
	}
	if _, err := os.Lstat(filepath.Join(source, "escaped-transport")); !os.IsNotExist(err) {
		t.Fatalf("transport was published inside source: %v", err)
	}

	transportRoot := filepath.Join(t.TempDir(), "transport")
	if _, err := Build(context.Background(), BuildConfig{
		SourceRoot: source, LineagePath: lineagePath, OutputRoot: transportRoot,
	}); err != nil {
		t.Fatal(err)
	}
	restoreAlias := filepath.Join(t.TempDir(), "restore-alias")
	if err := os.Symlink(transportRoot, restoreAlias); err != nil {
		t.Fatal(err)
	}
	if err := Restore(context.Background(), transportRoot, filepath.Join(restoreAlias, "escaped-restore")); err == nil {
		t.Fatal("symlinked restore destination parent into transport was accepted")
	}
	if _, err := os.Lstat(filepath.Join(transportRoot, "escaped-restore")); !os.IsNotExist(err) {
		t.Fatalf("restore was published inside transport: %v", err)
	}
}

func TestSafeRelativePathRejectsPortableTraversal(t *testing.T) {
	for _, path := range []string{
		"../escape", "a/../../escape", `/absolute`, `a\\..\\escape`, "C:/escape", "a//b", ".", "",
	} {
		if safeRelativePath(path) {
			t.Fatalf("unsafe path %q accepted", path)
		}
	}
	if !safeRelativePath("run-1/fixture/.git/HEAD") {
		t.Fatal("safe nested Git artifact rejected")
	}
}

func TestBuildRejectsUntrustedSourceAndLineageInputs(t *testing.T) {
	t.Run("symlinked-source-root", func(t *testing.T) {
		source, lineagePath := writeTransportFixture(t)
		alias := filepath.Join(t.TempDir(), "source-alias")
		if err := os.Symlink(source, alias); err != nil {
			t.Fatal(err)
		}
		output := filepath.Join(t.TempDir(), "transport")
		_, err := Build(context.Background(), BuildConfig{
			SourceRoot: alias, LineagePath: lineagePath, OutputRoot: output,
		})
		if !errors.Is(err, ErrInvalidTransport) {
			t.Fatalf("symlinked source root = %v", err)
		}
		if _, statErr := os.Lstat(output); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("rejected build published output: %v", statErr)
		}
	})

	t.Run("symlinked-source-ancestor", func(t *testing.T) {
		source, lineagePath := writeTransportFixture(t)
		alias := filepath.Join(t.TempDir(), "source-parent-alias")
		if err := os.Symlink(filepath.Dir(source), alias); err != nil {
			t.Fatal(err)
		}
		output := filepath.Join(t.TempDir(), "transport")
		_, err := Build(context.Background(), BuildConfig{
			SourceRoot:  filepath.Join(alias, filepath.Base(source)),
			LineagePath: lineagePath, OutputRoot: output,
		})
		if !errors.Is(err, ErrInvalidTransport) {
			t.Fatalf("symlinked source ancestor = %v", err)
		}
		if _, statErr := os.Lstat(output); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("rejected build published output: %v", statErr)
		}
	})

	t.Run("symlinked-lineage-ancestor", func(t *testing.T) {
		source, lineagePath := writeTransportFixture(t)
		alias := filepath.Join(t.TempDir(), "lineage-parent-alias")
		if err := os.Symlink(filepath.Dir(lineagePath), alias); err != nil {
			t.Fatal(err)
		}
		output := filepath.Join(t.TempDir(), "transport")
		_, err := Build(context.Background(), BuildConfig{
			SourceRoot:  source,
			LineagePath: filepath.Join(alias, filepath.Base(lineagePath)), OutputRoot: output,
		})
		if !errors.Is(err, ErrInvalidTransport) {
			t.Fatalf("symlinked lineage ancestor = %v", err)
		}
		if _, statErr := os.Lstat(output); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("rejected build published output: %v", statErr)
		}
	})

	t.Run("malformed-lineage", func(t *testing.T) {
		source, _ := writeTransportFixture(t)
		lineagePath := filepath.Join(t.TempDir(), "lineage.json")
		writeFixtureFile(t, lineagePath, []byte("{"))
		output := filepath.Join(t.TempDir(), "transport")
		if _, err := Build(context.Background(), BuildConfig{
			SourceRoot: source, LineagePath: lineagePath, OutputRoot: output,
		}); err == nil {
			t.Fatal("malformed lineage was accepted")
		}
		if _, statErr := os.Lstat(output); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("rejected build published output: %v", statErr)
		}
	})

	t.Run("unsupported-lineage", func(t *testing.T) {
		source, _ := writeTransportFixture(t)
		lineagePath := filepath.Join(t.TempDir(), "lineage.json")
		if err := writeJSONExclusive(lineagePath, Lineage{SchemaVersion: "unsupported"}); err != nil {
			t.Fatal(err)
		}
		if _, err := Build(context.Background(), BuildConfig{
			SourceRoot: source, LineagePath: lineagePath, OutputRoot: filepath.Join(t.TempDir(), "transport"),
		}); !errors.Is(err, ErrInvalidTransport) {
			t.Fatalf("unsupported lineage = %v", err)
		}
	})
}

func TestSourceInventoryRejectsSymlinksAndWrongEntryTypes(t *testing.T) {
	lineage := []LineageEntry{{
		Bundle: "bundle-v1", Audit: "bundle-v1-audit.json",
		Disposition: DispositionAdmitted, Reason: "fixture admitted",
	}}
	t.Run("symlinked-audit", func(t *testing.T) {
		root := t.TempDir()
		if err := os.Mkdir(filepath.Join(root, "bundle-v1"), 0o750); err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(t.TempDir(), "audit.json")
		writeFixtureFile(t, target, []byte("{}\n"))
		if err := os.Symlink(target, filepath.Join(root, "bundle-v1-audit.json")); err != nil {
			t.Fatal(err)
		}
		if err := validateSourceEntries(root, lineage); !errors.Is(err, ErrInvalidTransport) {
			t.Fatalf("symlinked audit = %v", err)
		}
	})
	t.Run("bundle-is-file", func(t *testing.T) {
		root := t.TempDir()
		writeFixtureFile(t, filepath.Join(root, "bundle-v1"), []byte("not a directory\n"))
		writeFixtureFile(t, filepath.Join(root, "bundle-v1-audit.json"), []byte("{}\n"))
		if err := validateSourceEntries(root, lineage); !errors.Is(err, ErrInvalidTransport) {
			t.Fatalf("file-backed bundle = %v", err)
		}
	})
}

func writeTransportFixture(t *testing.T) (string, string) {
	t.Helper()
	source := t.TempDir()
	bundle := filepath.Join(source, "bundle-v1")
	run := filepath.Join(bundle, "run-1")
	gitDirectory := filepath.Join(run, "fixture", ".git")
	if err := os.MkdirAll(gitDirectory, 0o750); err != nil {
		t.Fatal(err)
	}
	writeFixtureFile(t, filepath.Join(gitDirectory, "HEAD"), []byte("ref: refs/heads/main\n"))
	if err := writeJSONExclusive(filepath.Join(run, "trial-summary.json"), map[string]any{
		"schema_version": "codex-direct-trial-v1", "mode": "unsafe-fresh", "fault_boundary": "unfaulted",
		"trial": 1, "logical_session_id": "run-1", "extra_evidence": true,
	}); err != nil {
		t.Fatal(err)
	}
	writeFixtureFile(t, filepath.Join(run, "workflow-history.json"), []byte("{\"events\":[]}\n"))
	artifacts, err := inventoryFixtureRun(run)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeJSONExclusive(filepath.Join(run, "raw-inventory.json"), rawInventory{
		Version: RawInventoryVersion, Files: artifacts,
	}); err != nil {
		t.Fatal(err)
	}
	if err := writeJSONExclusive(filepath.Join(bundle, "suite-summary.json"), suiteSummary{
		EvidenceRoot: "/relocated/bundle-v1", RunDirectories: []string{"/relocated/bundle-v1/run-1"},
	}); err != nil {
		t.Fatal(err)
	}
	auditName := "bundle-v1-audit.json"
	if err := writeJSONExclusive(filepath.Join(source, auditName), map[string]any{
		"version": "codex-direct-disk-audit-v1", "evidence_root": "/relocated/bundle-v1",
		"runs": 1, "all_requirements_verified": true, "extra_metrics": 7,
	}); err != nil {
		t.Fatal(err)
	}
	lineagePath := filepath.Join(t.TempDir(), "lineage.json")
	if err := writeJSONExclusive(lineagePath, Lineage{SchemaVersion: LineageVersion, Entries: []LineageEntry{{
		Bundle: "bundle-v1", Audit: auditName, Disposition: DispositionAdmitted, Reason: "fixture admitted",
	}}}); err != nil {
		t.Fatal(err)
	}
	return source, lineagePath
}

func inventoryFixtureRun(root string) ([]rawArtifact, error) {
	files, _, err := inventoryTree(context.Background(), root)
	if err != nil {
		return nil, err
	}
	artifacts := make([]rawArtifact, 0, len(files))
	for _, file := range files {
		if file.Path == "raw-inventory.json" {
			continue
		}
		artifacts = append(artifacts, rawArtifact{Path: file.Path, Size: file.Size, SHA256: file.SHA256})
	}
	return artifacts, nil
}

func writeFixtureFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func transportFiles(t *testing.T, root string) []string {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	return names
}
