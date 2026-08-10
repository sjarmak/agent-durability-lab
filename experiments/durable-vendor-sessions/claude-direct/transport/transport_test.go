package transport

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
)

func TestBuildIsDeterministicAndRestoresNestedGitEvidenceFromCleanClone(t *testing.T) {
	t.Parallel()
	source, lineagePath := writeTestCollection(t)
	parent := t.TempDir()
	first := filepath.Join(parent, "transport-one")
	second := filepath.Join(parent, "transport-two")

	firstIndex, err := Build(context.Background(), BuildConfig{SourceRoot: source, LineagePath: lineagePath, OutputRoot: first})
	if err != nil {
		t.Fatalf("build first transport: %v", err)
	}
	secondIndex, err := Build(context.Background(), BuildConfig{SourceRoot: source, LineagePath: lineagePath, OutputRoot: second})
	if err != nil {
		t.Fatalf("build second transport: %v", err)
	}
	if !slices.Equal(firstIndex.Lineage, secondIndex.Lineage) {
		t.Fatalf("lineage differs: first=%+v second=%+v", firstIndex.Lineage, secondIndex.Lineage)
	}
	assertTreesEqual(t, first, second)

	verified, err := Verify(context.Background(), first)
	if err != nil {
		t.Fatalf("verify transport: %v", err)
	}
	if len(verified.Bundles) != 2 || verified.Bundles[0].Bundle != "claude-direct-v1" || verified.Bundles[1].Bundle != "claude-direct-v2" {
		t.Fatalf("verified bundles = %+v", verified.Bundles)
	}

	clone := commitAndCloneTransport(t, first)
	restored := filepath.Join(t.TempDir(), "evidence")
	if err := Restore(context.Background(), clone, restored); err != nil {
		t.Fatalf("restore clean-clone transport: %v", err)
	}
	assertTreesEqual(t, source, restored)
	if _, err := os.Stat(filepath.Join(restored, "claude-direct-v2", "run-one", "raw", "fixture", ".git", "objects", "aa", "object")); err != nil {
		t.Fatalf("restored nested Git object: %v", err)
	}
	if _, err := os.Stat(filepath.Join(restored, "claude-direct-v2", "run-one", "raw", "destination.db")); err != nil {
		t.Fatalf("restored ignored database: %v", err)
	}
}

func TestBuildBindsCorrectionLineageAndFinalizedRunEvidence(t *testing.T) {
	t.Parallel()
	source, lineagePath := writeTestCollection(t)
	output := filepath.Join(t.TempDir(), "transport")
	index, err := Build(context.Background(), BuildConfig{SourceRoot: source, LineagePath: lineagePath, OutputRoot: output})
	if err != nil {
		t.Fatalf("build transport: %v", err)
	}
	if got := index.Lineage[0]; got.Disposition != DispositionRejected || got.SupersededBy != "claude-direct-v2" {
		t.Fatalf("first lineage entry = %+v", got)
	}
	if got := index.Lineage[1]; got.Disposition != DispositionAdmitted || got.SupersededBy != "" {
		t.Fatalf("final lineage entry = %+v", got)
	}

	manifest := readTestJSON[BundleManifest](t, filepath.Join(output, index.Bundles[1].Manifest))
	if manifest.ArchiveSHA256 != index.Bundles[1].ArchiveSHA256 || len(manifest.Runs) != 1 {
		t.Fatalf("manifest bindings = %+v", manifest)
	}
	run := manifest.Runs[0]
	for name, digest := range map[string]string{
		"raw inventory":   run.RawInventorySHA256,
		"effective input": run.EffectiveInputSHA256,
		"common manifest": run.CommonManifestSHA256,
		"verdict":         run.VerdictSHA256,
	} {
		if !validSHA256(digest) {
			t.Fatalf("%s digest = %q", name, digest)
		}
	}
	if run.DeclaredRawInventorySHA256 != run.RawInventorySHA256 {
		t.Fatalf("declared raw inventory = %s, actual = %s", run.DeclaredRawInventorySHA256, run.RawInventorySHA256)
	}
	if run.RunID != "run-one" {
		t.Fatalf("run ID = %q", run.RunID)
	}
}

func TestBuildRejectsInvalidOrIncompleteSourceWithoutPublishing(t *testing.T) {
	t.Parallel()
	tests := map[string]func(t *testing.T, source string){
		"raw artifact changed": func(t *testing.T, source string) {
			path := filepath.Join(source, "claude-direct-v2", "run-one", "raw", "destination.db")
			writeTestFile(t, path, []byte("changed"), 0o600)
		},
		"source symlink": func(t *testing.T, source string) {
			path := filepath.Join(source, "claude-direct-v2", "run-one", "raw", "linked")
			if err := os.Symlink("destination.db", path); err != nil {
				t.Fatalf("create source symlink: %v", err)
			}
		},
		"unlisted bundle": func(t *testing.T, source string) {
			writeTestFile(t, filepath.Join(source, "omitted-v3", "artifact"), []byte("omitted"), 0o600)
		},
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			source, lineagePath := writeTestCollection(t)
			mutate(t, source)
			output := filepath.Join(t.TempDir(), "transport")
			if _, err := Build(context.Background(), BuildConfig{SourceRoot: source, LineagePath: lineagePath, OutputRoot: output}); err == nil {
				t.Fatal("invalid source produced a transport")
			}
			if _, err := os.Lstat(output); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("output exists after rejected build: %v", err)
			}
		})
	}
}

func TestVerifyRejectsTamperingUnexpectedFilesAndUnknownFields(t *testing.T) {
	t.Parallel()
	tests := map[string]func(t *testing.T, output string){
		"archive byte": func(t *testing.T, output string) {
			index := readTestJSON[Index](t, filepath.Join(output, IndexFile))
			path := filepath.Join(output, index.Bundles[0].Archive)
			file, err := os.OpenFile(path, os.O_WRONLY, 0)
			if err != nil {
				t.Fatalf("open archive: %v", err)
			}
			if _, err := file.WriteAt([]byte{0xff}, 12); err != nil {
				t.Fatalf("tamper archive: %v", err)
			}
			if err := file.Close(); err != nil {
				t.Fatalf("close archive: %v", err)
			}
		},
		"unexpected file": func(t *testing.T, output string) {
			writeTestFile(t, filepath.Join(output, "unsealed.txt"), []byte("extra"), 0o600)
		},
		"unknown index field": func(t *testing.T, output string) {
			path := filepath.Join(output, IndexFile)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read index: %v", err)
			}
			data = bytes.Replace(data, []byte("{\n"), []byte("{\n  \"unknown\": true,\n"), 1)
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatalf("rewrite test index: %v", err)
			}
		},
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			source, lineagePath := writeTestCollection(t)
			output := filepath.Join(t.TempDir(), "transport")
			if _, err := Build(context.Background(), BuildConfig{SourceRoot: source, LineagePath: lineagePath, OutputRoot: output}); err != nil {
				t.Fatalf("build transport: %v", err)
			}
			mutate(t, output)
			if _, err := Verify(context.Background(), output); err == nil {
				t.Fatal("tampered transport passed verification")
			}
		})
	}
}

func TestRestoreRefusesExistingDestinationAndUnsafeArchiveEntries(t *testing.T) {
	t.Parallel()
	source, lineagePath := writeTestCollection(t)
	output := filepath.Join(t.TempDir(), "transport")
	if _, err := Build(context.Background(), BuildConfig{SourceRoot: source, LineagePath: lineagePath, OutputRoot: output}); err != nil {
		t.Fatalf("build transport: %v", err)
	}
	existing := t.TempDir()
	if err := Restore(context.Background(), output, existing); !errors.Is(err, ErrDestinationExists) {
		t.Fatalf("restore existing destination error = %v, want destination exists", err)
	}

	for _, entry := range []tar.Header{
		{Name: "../escape", Mode: 0o600, Size: 1, Typeflag: tar.TypeReg},
		{Name: "bundle/link", Linkname: "../../escape", Typeflag: tar.TypeSymlink},
		{Name: "/absolute", Mode: 0o600, Size: 1, Typeflag: tar.TypeReg},
	} {
		entry := entry
		t.Run(strings.ReplaceAll(entry.Name, "/", "_"), func(t *testing.T) {
			t.Parallel()
			archive := writeTestArchive(t, entry)
			if err := verifyArchive(context.Background(), archive, BundleManifest{Bundle: "bundle"}); err == nil {
				t.Fatalf("unsafe archive entry %q passed verification", entry.Name)
			}
		})
	}
}

func TestVerifyArchiveRejectsPayloadAfterTarEndMarkers(t *testing.T) {
	t.Parallel()
	archive := filepath.Join(t.TempDir(), "trailing.tar.gz")
	file, err := os.Create(archive)
	if err != nil {
		t.Fatalf("create archive: %v", err)
	}
	gzipWriter := gzip.NewWriter(file)
	gzipWriter.ModTime = archiveEpoch
	gzipWriter.OS = 255
	tarWriter := tar.NewWriter(gzipWriter)
	header := tar.Header{Name: "bundle/artifact", Mode: 0o600, Size: 1, ModTime: archiveEpoch, Typeflag: tar.TypeReg, Format: tar.FormatPAX}
	if err := tarWriter.WriteHeader(&header); err != nil {
		t.Fatalf("write archive header: %v", err)
	}
	if _, err := tarWriter.Write([]byte("x")); err != nil {
		t.Fatalf("write archive artifact: %v", err)
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}
	if _, err := gzipWriter.Write([]byte("hidden-after-tar")); err != nil {
		t.Fatalf("write trailing payload: %v", err)
	}
	if err := errors.Join(gzipWriter.Close(), file.Close()); err != nil {
		t.Fatalf("close archive: %v", err)
	}
	digest := sha256.Sum256([]byte("x"))
	manifest := BundleManifest{Bundle: "bundle", Files: []Artifact{{Path: "artifact", Size: 1, Mode: 0o600, SHA256: hex.EncodeToString(digest[:])}}}
	if err := verifyArchive(context.Background(), archive, manifest); err == nil {
		t.Fatal("payload after tar end markers passed verification")
	}
}

func TestBuildHonorsCanceledContext(t *testing.T) {
	t.Parallel()
	source, lineagePath := writeTestCollection(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	output := filepath.Join(t.TempDir(), "transport")
	if _, err := Build(ctx, BuildConfig{SourceRoot: source, LineagePath: lineagePath, OutputRoot: output}); !errors.Is(err, context.Canceled) {
		t.Fatalf("build error = %v, want canceled", err)
	}
	if _, err := os.Lstat(output); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("output exists after canceled build: %v", err)
	}
}

func writeTestCollection(t *testing.T) (string, string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "evidence")
	for _, bundle := range []string{"claude-direct-v1", "claude-direct-v2"} {
		writeTestBundle(t, filepath.Join(root, bundle), bundle == "claude-direct-v1")
	}
	lineage := Lineage{
		SchemaVersion: SchemaVersion,
		Entries: []LineageEntry{
			{Bundle: "claude-direct-v1", Disposition: DispositionRejected, SupersededBy: "claude-direct-v2", Reason: "preserved admission correction"},
			{Bundle: "claude-direct-v2", Disposition: DispositionAdmitted, Reason: "source-matched admitted evidence"},
		},
	}
	lineagePath := filepath.Join(t.TempDir(), "lineage.json")
	writeTestJSON(t, lineagePath, lineage)
	return root, lineagePath
}

func writeTestBundle(t *testing.T, root string, includeIncomplete bool) {
	t.Helper()
	raw := filepath.Join(root, "run-one", "raw")
	writeTestFile(t, filepath.Join(raw, "destination.db"), []byte("bolt-database"), 0o600)
	writeTestFile(t, filepath.Join(raw, "fixture", ".git", "objects", "aa", "object"), []byte("git-object"), 0o444)
	writeTestFile(t, filepath.Join(raw, "fixture", ".git", "config"), []byte("[core]\n\trepositoryformatversion = 0\n"), 0o600)
	writeTestFile(t, filepath.Join(raw, "fixture", "effects.jsonl"), []byte("{\"effect\":1}\n"), 0o600)

	files := []rawArtifact{}
	err := filepath.WalkDir(raw, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		relative, err := filepath.Rel(raw, path)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		digest := testFileSHA256(t, path)
		files = append(files, rawArtifact{Path: filepath.ToSlash(relative), Size: info.Size(), SHA256: digest})
		return nil
	})
	if err != nil {
		t.Fatalf("inventory test raw evidence: %v", err)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	rawInventoryPath := filepath.Join(raw, RawInventoryFile)
	writeTestJSON(t, rawInventoryPath, rawInventory{Version: RawInventoryVersion, Files: files})
	rawInventorySHA := testFileSHA256(t, rawInventoryPath)

	effectiveInputPath := filepath.Join(root, "run-one", "effective-input.json")
	writeTestJSON(t, effectiveInputPath, effectiveInput{Settings: map[string]string{"raw_inventory_sha256": rawInventorySHA}})
	effectiveInputSHA := testFileSHA256(t, effectiveInputPath)
	writeTestJSON(t, filepath.Join(root, "run-one", "manifest.json"), commonManifest{RunID: "run-one", EffectiveInputSHA256: effectiveInputSHA})
	writeTestJSON(t, filepath.Join(root, "run-one", "verdict.json"), verdict{RunID: "run-one"})
	writeTestJSON(t, filepath.Join(root, "suite-summary.json"), map[string]any{"run_directories": []string{"/original/workspace/run-one"}})
	writeTestFile(t, filepath.Join(root, "temporal.db"), []byte("temporal-database"), 0o600)
	if includeIncomplete {
		writeTestFile(t, filepath.Join(root, ".staging-incomplete", "fixture", ".git", "HEAD"), []byte("ref: refs/heads/main\n"), 0o600)
		writeTestJSON(t, filepath.Join(root, "failure.json"), map[string]string{"reason": "preserved failure"})
	}
}

func commitAndCloneTransport(t *testing.T, source string) string {
	t.Helper()
	repository := filepath.Join(t.TempDir(), "repository")
	if err := os.MkdirAll(repository, 0o700); err != nil {
		t.Fatalf("create repository: %v", err)
	}
	copyTestTree(t, source, filepath.Join(repository, "transport"))
	runGit(t, repository, "init", "-q")
	runGit(t, repository, "add", "transport")
	staged := runGit(t, repository, "ls-files", "--stage")
	for _, line := range strings.Split(strings.TrimSpace(staged), "\n") {
		if strings.HasPrefix(line, "160000 ") {
			t.Fatalf("transport staged a gitlink: %s", line)
		}
	}
	runGit(t, repository, "-c", "user.name=Evidence Test", "-c", "user.email=evidence@example.invalid", "commit", "-qm", "transport")
	clone := filepath.Join(t.TempDir(), "clone")
	command := exec.Command("git", "clone", "-q", repository, clone)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("clone transport: %v\n%s", err, output)
	}
	return filepath.Join(clone, "transport")
}

func runGit(t *testing.T, directory string, args ...string) string {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", directory}, args...)...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, output)
	}
	return string(output)
}

func assertTreesEqual(t *testing.T, first, second string) {
	t.Helper()
	firstTree := testTree(t, first)
	secondTree := testTree(t, second)
	if !slices.Equal(firstTree, secondTree) {
		t.Fatalf("trees differ:\nfirst: %s\nsecond: %s", strings.Join(firstTree, "\n"), strings.Join(secondTree, "\n"))
	}
}

func testTree(t *testing.T, root string) []string {
	t.Helper()
	var result []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		result = append(result, fmt.Sprintf("%s %04o %d %s", filepath.ToSlash(relative), info.Mode().Perm(), info.Size(), testFileSHA256(t, path)))
		return nil
	})
	if err != nil {
		t.Fatalf("inventory tree %s: %v", root, err)
	}
	sort.Strings(result)
	return result
}

func copyTestTree(t *testing.T, source, destination string) {
	t.Helper()
	err := filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o700)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o600)
	})
	if err != nil {
		t.Fatalf("copy transport tree: %v", err)
	}
}

func writeTestArchive(t *testing.T, header tar.Header) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "unsafe.tar.gz")
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create archive: %v", err)
	}
	gzipWriter := gzip.NewWriter(file)
	tarWriter := tar.NewWriter(gzipWriter)
	if err := tarWriter.WriteHeader(&header); err != nil {
		t.Fatalf("write archive header: %v", err)
	}
	if header.Size > 0 {
		if _, err := tarWriter.Write(bytes.Repeat([]byte{'x'}, int(header.Size))); err != nil {
			t.Fatalf("write archive data: %v", err)
		}
	}
	if err := errors.Join(tarWriter.Close(), gzipWriter.Close(), file.Close()); err != nil {
		t.Fatalf("close archive: %v", err)
	}
	return path
}

func writeTestJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatalf("marshal %s: %v", path, err)
	}
	data = append(data, '\n')
	writeTestFile(t, path, data, 0o600)
}

func readTestJSON[T any](t *testing.T, path string) T {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var value T
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return value
}

func writeTestFile(t *testing.T, path string, data []byte, mode fs.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("create parent for %s: %v", path, err)
	}
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func testFileSHA256(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}
