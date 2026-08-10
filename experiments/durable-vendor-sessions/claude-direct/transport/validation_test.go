package transport

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func TestRepositoryTransportVerifiesRestoresAndRetainsGitRepositories(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate repository transport test")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", "evidence-transport"))
	index, err := Verify(context.Background(), root)
	if err != nil {
		t.Fatalf("verify repository transport: %v", err)
	}
	if len(index.Bundles) != 5 || index.Lineage[4].Disposition != DispositionAdmitted {
		t.Fatalf("repository transport index = %+v", index)
	}
	fileCount := 0
	runCount := 0
	for _, entry := range index.Bundles {
		manifest, err := readJSON[BundleManifest](filepath.Join(root, entry.Manifest))
		if err != nil {
			t.Fatalf("read %s: %v", entry.Manifest, err)
		}
		fileCount += manifest.FileCount
		runCount += len(manifest.Runs)
	}
	if fileCount != 2206 || runCount != 56 {
		t.Fatalf("repository transport files/runs = %d/%d, want 2206/56", fileCount, runCount)
	}

	restored := filepath.Join(t.TempDir(), "evidence")
	if err := Restore(context.Background(), root, restored); err != nil {
		t.Fatalf("restore repository transport: %v", err)
	}
	gitDirectories := 0
	err = filepath.WalkDir(restored, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() || entry.Name() != ".git" {
			return nil
		}
		gitDirectories++
		command := exec.Command("git", "--git-dir", path, "fsck", "--full", "--no-progress")
		if output, err := command.CombinedOutput(); err != nil {
			return errors.New("restored nested Git repository failed fsck: " + err.Error() + ": " + string(output))
		}
		return filepath.SkipDir
	})
	if err != nil {
		t.Fatal(err)
	}
	if gitDirectories != 57 {
		t.Fatalf("restored nested Git directories = %d, want 57", gitDirectories)
	}
}

func TestLineageValidationRejectsEveryBrokenChainShape(t *testing.T) {
	t.Parallel()
	valid := Lineage{SchemaVersion: SchemaVersion, Entries: []LineageEntry{
		{Bundle: "v1", Disposition: DispositionRejected, SupersededBy: "v2", Reason: "correction"},
		{Bundle: "v2", Disposition: DispositionAdmitted, Reason: "admitted"},
	}}
	if err := validateLineage(valid); err != nil {
		t.Fatalf("valid lineage: %v", err)
	}
	tests := map[string]func(Lineage) Lineage{
		"schema":             func(value Lineage) Lineage { value.SchemaVersion = "wrong"; return value },
		"empty":              func(value Lineage) Lineage { value.Entries = nil; return value },
		"duplicate":          func(value Lineage) Lineage { value.Entries[1].Bundle = "v1"; return value },
		"reason":             func(value Lineage) Lineage { value.Entries[0].Reason = " "; return value },
		"non-final admitted": func(value Lineage) Lineage { value.Entries[0].Disposition = DispositionAdmitted; return value },
		"broken successor":   func(value Lineage) Lineage { value.Entries[0].SupersededBy = "v3"; return value },
		"final disposition":  func(value Lineage) Lineage { value.Entries[1].Disposition = DispositionSuperseded; return value },
		"final successor":    func(value Lineage) Lineage { value.Entries[1].SupersededBy = "v3"; return value },
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			candidate := Lineage{SchemaVersion: valid.SchemaVersion, Entries: append([]LineageEntry(nil), valid.Entries...)}
			if err := validateLineage(mutate(candidate)); err == nil {
				t.Fatal("invalid lineage passed")
			}
		})
	}
}

func TestManifestAndIndexValidationRejectsBrokenBindings(t *testing.T) {
	t.Parallel()
	digest := strings.Repeat("a", 64)
	validManifest := BundleManifest{
		SchemaVersion: SchemaVersion,
		Bundle:        "bundle",
		Archive:       "bundle.tar.gz",
		ArchiveSHA256: digest,
		FileCount:     4,
		TotalBytes:    4,
		Files: []Artifact{
			{Path: "run/effective-input.json", Size: 1, Mode: 0o600, SHA256: digest},
			{Path: "run/manifest.json", Size: 1, Mode: 0o600, SHA256: digest},
			{Path: "run/raw/raw-inventory.json", Size: 1, Mode: 0o600, SHA256: digest},
			{Path: "run/verdict.json", Size: 1, Mode: 0o600, SHA256: digest},
		},
		Runs: []RunBinding{{
			RunID:            "run",
			RawInventoryPath: "run/raw/raw-inventory.json", RawInventorySHA256: digest, DeclaredRawInventorySHA256: digest,
			EffectiveInputPath: "run/effective-input.json", EffectiveInputSHA256: digest,
			CommonManifestPath: "run/manifest.json", CommonManifestSHA256: digest,
			VerdictPath: "run/verdict.json", VerdictSHA256: digest,
		}},
	}
	if err := validateBundleManifest(validManifest); err != nil {
		t.Fatalf("valid manifest: %v", err)
	}
	tests := map[string]func(BundleManifest) BundleManifest{
		"identity":       func(value BundleManifest) BundleManifest { value.SchemaVersion = "wrong"; return value },
		"archive":        func(value BundleManifest) BundleManifest { value.Archive = "other.tar.gz"; return value },
		"archive digest": func(value BundleManifest) BundleManifest { value.ArchiveSHA256 = "bad"; return value },
		"count":          func(value BundleManifest) BundleManifest { value.FileCount++; return value },
		"bytes":          func(value BundleManifest) BundleManifest { value.TotalBytes++; return value },
		"unsafe path":    func(value BundleManifest) BundleManifest { value.Files[0].Path = "../escape"; return value },
		"unordered path": func(value BundleManifest) BundleManifest { value.Files[1].Path = value.Files[0].Path; return value },
		"large file":     func(value BundleManifest) BundleManifest { value.Files[0].Size = maxArtifactBytes + 1; return value },
		"special mode":   func(value BundleManifest) BundleManifest { value.Files[0].Mode = fs.ModeSymlink | 0o777; return value },
		"file digest":    func(value BundleManifest) BundleManifest { value.Files[0].SHA256 = "bad"; return value },
		"declared inventory": func(value BundleManifest) BundleManifest {
			value.Runs[0].DeclaredRawInventorySHA256 = strings.Repeat("b", 64)
			return value
		},
		"run path": func(value BundleManifest) BundleManifest { value.Runs[0].VerdictPath = "absent"; return value },
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			candidate := cloneManifest(validManifest)
			if err := validateBundleManifest(mutate(candidate)); err == nil {
				t.Fatal("invalid manifest passed")
			}
		})
	}

	validIndex := Index{
		SchemaVersion: SchemaVersion,
		Lineage:       []LineageEntry{{Bundle: "bundle", Disposition: DispositionAdmitted, Reason: "admitted"}},
		Bundles:       []BundleEntry{{Bundle: "bundle", Archive: "bundle.tar.gz", ArchiveSHA256: digest, Manifest: "bundle.manifest.json", ManifestSHA256: digest}},
	}
	if err := validateIndex(validIndex); err != nil {
		t.Fatalf("valid index: %v", err)
	}
	for name, candidate := range map[string]Index{
		"missing bundle": {SchemaVersion: validIndex.SchemaVersion, Lineage: validIndex.Lineage},
		"wrong archive":  {SchemaVersion: validIndex.SchemaVersion, Lineage: validIndex.Lineage, Bundles: []BundleEntry{{Bundle: "bundle", Archive: "wrong", ArchiveSHA256: digest, Manifest: "bundle.manifest.json", ManifestSHA256: digest}}},
	} {
		name, candidate := name, candidate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := validateIndex(candidate); err == nil {
				t.Fatal("invalid index passed")
			}
		})
	}
}

func TestRegularFileAndBuildBoundariesFailClosed(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	regular := filepath.Join(directory, "regular")
	writeTestFile(t, regular, []byte("content"), 0o600)
	if _, err := readRegularFile(regular, 3); err == nil {
		t.Fatal("oversized regular file passed")
	}
	if _, err := hashRegularFile(regular, 3); err == nil {
		t.Fatal("oversized regular file passed hashing")
	}
	symlink := filepath.Join(directory, "symlink")
	if err := os.Symlink("regular", symlink); err != nil {
		t.Fatalf("create symlink: %v", err)
	}
	if _, err := readRegularFile(symlink, 100); err == nil {
		t.Fatal("symlink passed regular-file read")
	}
	if _, err := hashRegularFile(symlink, 100); err == nil {
		t.Fatal("symlink passed regular-file hashing")
	}
	if err := ensureDirectory(regular); err == nil {
		t.Fatal("regular file passed directory validation")
	}
	if _, err := resolveBuildConfig(BuildConfig{}); err == nil {
		t.Fatal("empty build config passed")
	}
	source := filepath.Join(directory, "source")
	if err := os.Mkdir(source, 0o700); err != nil {
		t.Fatalf("create source: %v", err)
	}
	if _, err := resolveBuildConfig(BuildConfig{SourceRoot: source, LineagePath: regular, OutputRoot: filepath.Join(source, "output")}); err == nil {
		t.Fatal("overlapping source and output passed")
	}
	alias := filepath.Join(directory, "source-alias")
	if err := os.Symlink(source, alias); err != nil {
		t.Fatalf("create source alias: %v", err)
	}
	if _, err := resolveBuildConfig(BuildConfig{SourceRoot: source, LineagePath: regular, OutputRoot: filepath.Join(alias, "output")}); err == nil {
		t.Fatal("symlink-aliased overlapping output passed")
	}
}

func TestPublicOperationsRejectExistingOutputCanceledVerificationAndNonregularPackageFiles(t *testing.T) {
	t.Parallel()
	source, lineagePath := writeTestCollection(t)
	existing := t.TempDir()
	if _, err := Build(context.Background(), BuildConfig{SourceRoot: source, LineagePath: lineagePath, OutputRoot: existing}); !errors.Is(err, ErrDestinationExists) {
		t.Fatalf("build existing output error = %v, want destination exists", err)
	}

	transportRoot := filepath.Join(t.TempDir(), "transport")
	if _, err := Build(context.Background(), BuildConfig{SourceRoot: source, LineagePath: lineagePath, OutputRoot: transportRoot}); err != nil {
		t.Fatalf("build transport: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Verify(canceled, transportRoot); !errors.Is(err, context.Canceled) {
		t.Fatalf("verify canceled context error = %v, want canceled", err)
	}

	index := readTestJSON[Index](t, filepath.Join(transportRoot, IndexFile))
	manifestPath := filepath.Join(transportRoot, index.Bundles[0].Manifest)
	if _, err := loadBoundManifest(transportRoot, index.Bundles[0]); err != nil {
		t.Fatalf("load bound manifest: %v", err)
	}
	if err := os.Chmod(manifestPath, 0); err != nil {
		t.Fatalf("chmod manifest: %v", err)
	}
	if _, err := Verify(context.Background(), transportRoot); err == nil {
		t.Fatal("unreadable manifest passed verification")
	}
}

func TestCrossPlatformPathsAndExpansionLimitsFailClosed(t *testing.T) {
	t.Parallel()
	for _, unsafe := range []string{"../escape", "dir/../escape", `dir\escape`, `/absolute`, "."} {
		if safeRelativePath(unsafe) {
			t.Fatalf("unsafe relative path %q passed", unsafe)
		}
	}

	lineage := Lineage{SchemaVersion: SchemaVersion, Entries: make([]LineageEntry, maxBundles+1)}
	for index := range lineage.Entries {
		lineage.Entries[index] = LineageEntry{Bundle: "bundle-" + strconv.Itoa(index), Disposition: DispositionSuperseded, Reason: "bounded"}
		if index+1 < len(lineage.Entries) {
			lineage.Entries[index].SupersededBy = "bundle-" + strconv.Itoa(index+1)
		}
	}
	lineage.Entries[len(lineage.Entries)-1].Disposition = DispositionAdmitted
	lineage.Entries[len(lineage.Entries)-1].SupersededBy = ""
	if err := validateLineage(lineage); err == nil {
		t.Fatal("oversized bundle lineage passed")
	}

	digest := strings.Repeat("a", 64)
	manifest := BundleManifest{
		SchemaVersion: SchemaVersion, Bundle: "bundle", Archive: "bundle.tar.gz", ArchiveSHA256: digest,
		FileCount: 17, TotalBytes: 17 * maxArtifactBytes,
	}
	for index := 0; index < manifest.FileCount; index++ {
		manifest.Files = append(manifest.Files, Artifact{Path: "artifact-" + string(rune('a'+index)), Size: maxArtifactBytes, Mode: 0o600, SHA256: digest})
	}
	if err := validateBundleManifest(manifest); err == nil {
		t.Fatal("oversized expanded bundle passed")
	}
}

func cloneManifest(value BundleManifest) BundleManifest {
	value.Files = append([]Artifact(nil), value.Files...)
	value.Runs = append([]RunBinding(nil), value.Runs...)
	return value
}
