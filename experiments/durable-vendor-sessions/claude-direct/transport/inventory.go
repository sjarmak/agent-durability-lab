package transport

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strings"
)

func inspectBundle(ctx context.Context, root, bundle string) (BundleManifest, error) {
	files, total, err := inventoryTree(ctx, root)
	if err != nil {
		return BundleManifest{}, err
	}
	runs, err := bindFinalizedRuns(root, files)
	if err != nil {
		return BundleManifest{}, err
	}
	return BundleManifest{
		SchemaVersion: SchemaVersion,
		Bundle:        bundle,
		FileCount:     len(files),
		TotalBytes:    total,
		Files:         files,
		Runs:          runs,
	}, nil
}

func inventoryTree(ctx context.Context, root string) ([]Artifact, int64, error) {
	if err := ensureDirectory(root); err != nil {
		return nil, 0, err
	}
	var artifacts []Artifact
	var total int64
	err := filepath.WalkDir(root, func(filePath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := checkContext(ctx); err != nil {
			return err
		}
		if filePath == root || entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Mode()&fs.ModeSymlink != 0 || info.Size() < 0 || info.Size() > maxArtifactBytes {
			return fmt.Errorf("%w: source artifact %s is not a bounded regular file", ErrInvalidTransport, filePath)
		}
		relative, err := filepath.Rel(root, filePath)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if !safeRelativePath(relative) {
			return fmt.Errorf("%w: unsafe source path %q", ErrInvalidTransport, relative)
		}
		digest, err := hashRegularFile(filePath, maxArtifactBytes)
		if err != nil {
			return err
		}
		if total > int64(^uint64(0)>>1)-info.Size() {
			return fmt.Errorf("%w: source size overflow", ErrInvalidTransport)
		}
		total += info.Size()
		artifacts = append(artifacts, Artifact{Path: relative, Size: info.Size(), Mode: info.Mode().Perm(), SHA256: digest})
		return nil
	})
	sort.Slice(artifacts, func(i, j int) bool { return artifacts[i].Path < artifacts[j].Path })
	return artifacts, total, err
}

func bindFinalizedRuns(root string, files []Artifact) ([]RunBinding, error) {
	byPath := make(map[string]Artifact, len(files))
	for _, artifact := range files {
		byPath[artifact.Path] = artifact
	}
	var runs []RunBinding
	for _, artifact := range files {
		if path.Base(artifact.Path) != RawInventoryFile || path.Base(path.Dir(artifact.Path)) != "raw" {
			continue
		}
		runDirectory := path.Dir(path.Dir(artifact.Path))
		runID := path.Base(runDirectory)
		binding, err := bindRun(root, runDirectory, runID, artifact, byPath)
		if err != nil {
			return nil, err
		}
		runs = append(runs, binding)
	}
	sort.Slice(runs, func(i, j int) bool { return runs[i].RunID < runs[j].RunID })
	return runs, nil
}

func bindRun(root, runDirectory, runID string, inventoryArtifact Artifact, byPath map[string]Artifact) (RunBinding, error) {
	if err := verifyRawInventoryBinding(root, runDirectory, runID, inventoryArtifact, byPath); err != nil {
		return RunBinding{}, err
	}

	effectivePath := runDirectory + "/effective-input.json"
	commonPath := runDirectory + "/manifest.json"
	verdictPath := runDirectory + "/verdict.json"
	effectiveArtifact, effectiveOK := byPath[effectivePath]
	commonArtifact, commonOK := byPath[commonPath]
	verdictArtifact, verdictOK := byPath[verdictPath]
	if !effectiveOK || !commonOK || !verdictOK {
		return RunBinding{}, fmt.Errorf("%w: finalized run files for %s", ErrInvalidTransport, runID)
	}
	effective, err := readJSON[effectiveInput](filepath.Join(root, filepath.FromSlash(effectivePath)))
	if err != nil {
		return RunBinding{}, err
	}
	common, err := readJSON[commonManifest](filepath.Join(root, filepath.FromSlash(commonPath)))
	if err != nil {
		return RunBinding{}, err
	}
	observedVerdict, err := readJSON[verdict](filepath.Join(root, filepath.FromSlash(verdictPath)))
	if err != nil {
		return RunBinding{}, err
	}
	declared := effective.Settings["raw_inventory_sha256"]
	if !validSHA256(declared) || declared != inventoryArtifact.SHA256 || common.RunID != runID || observedVerdict.RunID != runID ||
		common.EffectiveInputSHA256 != effectiveArtifact.SHA256 {
		return RunBinding{}, fmt.Errorf("%w: finalized run binding for %s", ErrInvalidTransport, runID)
	}
	return RunBinding{
		RunID:                      runID,
		RawInventoryPath:           inventoryArtifact.Path,
		RawInventorySHA256:         inventoryArtifact.SHA256,
		DeclaredRawInventorySHA256: declared,
		EffectiveInputPath:         effectivePath,
		EffectiveInputSHA256:       effectiveArtifact.SHA256,
		CommonManifestPath:         commonPath,
		CommonManifestSHA256:       commonArtifact.SHA256,
		VerdictPath:                verdictPath,
		VerdictSHA256:              verdictArtifact.SHA256,
	}, nil
}

func verifyRawInventoryBinding(root, runDirectory, runID string, inventoryArtifact Artifact, byPath map[string]Artifact) error {
	inventoryPath := filepath.Join(root, filepath.FromSlash(inventoryArtifact.Path))
	inventory, err := readJSON[rawInventory](inventoryPath)
	if err != nil {
		return err
	}
	if inventory.Version != RawInventoryVersion || len(inventory.Files) == 0 {
		return fmt.Errorf("%w: raw inventory identity for %s", ErrInvalidTransport, runID)
	}
	wantRaw := make([]rawArtifact, 0, len(inventory.Files))
	prefix := runDirectory + "/raw/"
	for relative, artifact := range byPath {
		if !strings.HasPrefix(relative, prefix) || relative == inventoryArtifact.Path {
			continue
		}
		wantRaw = append(wantRaw, rawArtifact{
			Path: strings.TrimPrefix(relative, prefix), Size: artifact.Size, SHA256: artifact.SHA256,
		})
	}
	sort.Slice(wantRaw, func(i, j int) bool { return wantRaw[i].Path < wantRaw[j].Path })
	if !validRawInventory(inventory.Files) || !slices.Equal(inventory.Files, wantRaw) {
		return fmt.Errorf("%w: raw inventory differs for %s", ErrInvalidTransport, runID)
	}
	return nil
}

func validRawInventory(files []rawArtifact) bool {
	previous := ""
	for _, artifact := range files {
		if !safeRelativePath(artifact.Path) || artifact.Path <= previous || artifact.Size < 0 || !validSHA256(artifact.SHA256) {
			return false
		}
		previous = artifact.Path
	}
	return true
}

func hashStableInto(writer io.Writer, path string, want Artifact) (returnErr error) {
	before, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !before.Mode().IsRegular() || before.Size() != want.Size || before.Mode().Perm() != want.Mode {
		return fmt.Errorf("%w: source artifact changed before archive: %s", ErrInvalidTransport, path)
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	hasher := newSHA256Writer(writer)
	written, err := io.Copy(hasher, io.LimitReader(file, want.Size+1))
	if err != nil {
		return err
	}
	after, err := file.Stat()
	if err != nil || written != want.Size || !os.SameFile(before, after) || after.Size() != want.Size ||
		after.Mode().Perm() != want.Mode || !after.ModTime().Equal(before.ModTime()) || hasher.Sum() != want.SHA256 {
		return fmt.Errorf("%w: source artifact changed during archive: %s", ErrInvalidTransport, path)
	}
	return nil
}

type hashingWriter struct {
	destination io.Writer
	hash        hash.Hash
}

func newSHA256Writer(destination io.Writer) *hashingWriter {
	return &hashingWriter{destination: destination, hash: sha256.New()}
}

func (w *hashingWriter) Write(data []byte) (int, error) {
	n, err := w.destination.Write(data)
	if n > 0 {
		_, _ = w.hash.Write(data[:n])
	}
	return n, err
}

func (w *hashingWriter) Sum() string {
	return hex.EncodeToString(w.hash.Sum(nil))
}
