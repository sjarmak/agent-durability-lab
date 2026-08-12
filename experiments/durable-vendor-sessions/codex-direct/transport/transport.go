package transport

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
)

func Build(ctx context.Context, config BuildConfig) (Index, error) {
	resolved, lineage, err := prepareBuild(ctx, config)
	if err != nil {
		return Index{}, err
	}
	staging, err := newStagingDirectory(resolved.OutputRoot)
	if err != nil {
		return Index{}, err
	}
	published := false
	defer func() {
		if !published {
			_ = os.RemoveAll(staging)
		}
	}()
	index := Index{SchemaVersion: SchemaVersion, Lineage: slices.Clone(lineage.Entries)}
	for _, entry := range lineage.Entries {
		bundle, err := writeBundle(ctx, resolved.SourceRoot, staging, entry)
		if err != nil {
			return Index{}, fmt.Errorf("write evidence bundle %s (%s): %w", entry.Bundle, entry.Disposition, err)
		}
		index.Bundles = append(index.Bundles, bundle)
	}
	if err := writeJSONExclusive(filepath.Join(staging, IndexFile), index); err != nil {
		return Index{}, err
	}
	if _, err := Verify(ctx, staging); err != nil {
		return Index{}, err
	}
	if err := os.Rename(staging, resolved.OutputRoot); err != nil {
		return Index{}, err
	}
	published = true
	return index, nil
}

func prepareBuild(ctx context.Context, config BuildConfig) (BuildConfig, Lineage, error) {
	if err := ctx.Err(); err != nil {
		return BuildConfig{}, Lineage{}, err
	}
	paths := []*string{&config.SourceRoot, &config.LineagePath, &config.OutputRoot}
	for _, path := range paths {
		if *path == "" {
			return BuildConfig{}, Lineage{}, fmt.Errorf("%w: build paths are required", ErrInvalidTransport)
		}
		absolute, err := filepath.Abs(*path)
		if err != nil {
			return BuildConfig{}, Lineage{}, err
		}
		*path = filepath.Clean(absolute)
	}
	canonicalSource, err := filepath.EvalSymlinks(config.SourceRoot)
	if err != nil || canonicalSource != config.SourceRoot {
		return BuildConfig{}, Lineage{}, fmt.Errorf("%w: source root contains a symlink", ErrInvalidTransport)
	}
	canonicalLineage, err := filepath.EvalSymlinks(config.LineagePath)
	if err != nil || canonicalLineage != config.LineagePath {
		return BuildConfig{}, Lineage{}, fmt.Errorf("%w: lineage path contains a symlink", ErrInvalidTransport)
	}
	config.SourceRoot = canonicalSource
	config.LineagePath = canonicalLineage
	if info, err := os.Lstat(config.SourceRoot); err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return BuildConfig{}, Lineage{}, fmt.Errorf("%w: source root is not a real directory", ErrInvalidTransport)
	}
	if _, err := os.Lstat(config.OutputRoot); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return BuildConfig{}, Lineage{}, ErrDestinationExists
		}
		return BuildConfig{}, Lineage{}, err
	}
	lineage, err := readJSON[Lineage](config.LineagePath)
	if err != nil {
		return BuildConfig{}, Lineage{}, err
	}
	if err := validLineage(lineage); err != nil {
		return BuildConfig{}, Lineage{}, err
	}
	resolvedOutput, err := resolveThroughExistingAncestor(config.OutputRoot)
	if err != nil {
		return BuildConfig{}, Lineage{}, err
	}
	if pathWithin(canonicalSource, resolvedOutput) || pathWithin(resolvedOutput, canonicalSource) {
		return BuildConfig{}, Lineage{}, fmt.Errorf("%w: source and output trees overlap", ErrInvalidTransport)
	}
	if err := validateSourceEntries(config.SourceRoot, lineage.Entries); err != nil {
		return BuildConfig{}, Lineage{}, err
	}
	return config, lineage, nil
}

func writeBundle(ctx context.Context, sourceRoot, staging string, lineage LineageEntry) (BundleEntry, error) {
	source := filepath.Join(sourceRoot, lineage.Bundle)
	files, total, err := inventoryTree(ctx, source)
	if err != nil {
		return BundleEntry{}, err
	}
	auditSource := filepath.Join(sourceRoot, lineage.Audit)
	var runs []RunBinding
	var auditHash string
	if lineage.Disposition == DispositionRejected {
		failureHash, rejectedErr := validateRejectedBundle(source, files)
		if rejectedErr != nil {
			return BundleEntry{}, rejectedErr
		}
		_, auditHash, err = validateRejectionAudit(auditSource, failureHash)
	} else {
		runs, err = bindRuns(source, files)
		if err == nil {
			_, auditHash, err = validateAudit(auditSource, len(runs))
		}
	}
	if err != nil {
		return BundleEntry{}, err
	}
	manifest := BundleManifest{
		SchemaVersion: SchemaVersion, Bundle: lineage.Bundle, Disposition: lineage.Disposition,
		Audit: lineage.Audit, AuditSHA256: auditHash,
		FileCount: len(files), TotalBytes: total, Files: files, Runs: runs,
		Archive: lineage.Bundle + ".tar.gz",
	}
	archivePath := filepath.Join(staging, manifest.Archive)
	if err := writeArchive(ctx, source, archivePath, files); err != nil {
		return BundleEntry{}, err
	}
	stableFiles, stableTotal, err := inventoryTree(ctx, source)
	if err != nil || stableTotal != total || !reflect.DeepEqual(stableFiles, files) {
		return BundleEntry{}, fmt.Errorf("%w: source bundle changed while it was archived", ErrInvalidTransport)
	}
	manifest.ArchiveSHA256, err = hashFile(archivePath, maxArchiveBytes)
	if err != nil {
		return BundleEntry{}, err
	}
	if err := validateManifest(manifest); err != nil {
		return BundleEntry{}, err
	}
	manifestName := lineage.Bundle + ".manifest.json"
	manifestPath := filepath.Join(staging, manifestName)
	if err := writeJSONExclusive(manifestPath, manifest); err != nil {
		return BundleEntry{}, err
	}
	manifestHash, err := hashFile(manifestPath, maxJSONBytes)
	if err != nil {
		return BundleEntry{}, err
	}
	if err := copyExclusive(auditSource, filepath.Join(staging, lineage.Audit)); err != nil {
		return BundleEntry{}, err
	}
	return BundleEntry{
		Bundle: lineage.Bundle, Archive: manifest.Archive, ArchiveSHA256: manifest.ArchiveSHA256,
		Manifest: manifestName, ManifestSHA256: manifestHash, Audit: lineage.Audit, AuditSHA256: auditHash,
	}, nil
}

func Verify(ctx context.Context, root string) (Index, error) {
	if err := ctx.Err(); err != nil {
		return Index{}, err
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return Index{}, err
	}
	info, err := os.Lstat(absolute)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return Index{}, fmt.Errorf("%w: transport root is not a real directory", ErrInvalidTransport)
	}
	index, err := readJSON[Index](filepath.Join(absolute, IndexFile))
	if err != nil {
		return Index{}, err
	}
	if err := validateIndex(index); err != nil {
		return Index{}, err
	}
	wantFiles := []string{IndexFile}
	for _, entry := range index.Bundles {
		wantFiles = append(wantFiles, entry.Archive, entry.Manifest, entry.Audit)
	}
	if err := validateExactFiles(absolute, wantFiles); err != nil {
		return Index{}, err
	}
	for position, entry := range index.Bundles {
		if err := ctx.Err(); err != nil {
			return Index{}, err
		}
		manifest, err := loadBoundManifest(absolute, entry)
		if err != nil {
			return Index{}, err
		}
		if manifest.Disposition != index.Lineage[position].Disposition {
			return Index{}, fmt.Errorf("%w: manifest disposition differs from lineage", ErrInvalidTransport)
		}
		if manifest.Disposition == DispositionRejected {
			failureHash, rejectedErr := manifestFailureHash(manifest)
			if rejectedErr != nil {
				return Index{}, rejectedErr
			}
			if _, _, err := validateRejectionAudit(filepath.Join(absolute, entry.Audit), failureHash); err != nil {
				return Index{}, err
			}
		} else if _, _, err := validateAudit(filepath.Join(absolute, entry.Audit), len(manifest.Runs)); err != nil {
			return Index{}, err
		}
		if err := verifyArchive(ctx, filepath.Join(absolute, entry.Archive), manifest); err != nil {
			return Index{}, err
		}
	}
	return index, nil
}

func Restore(ctx context.Context, transportRoot, destinationRoot string) error {
	index, err := Verify(ctx, transportRoot)
	if err != nil {
		return err
	}
	destination, err := filepath.Abs(destinationRoot)
	if err != nil || destinationRoot == "" {
		return fmt.Errorf("%w: destination is required", ErrInvalidTransport)
	}
	destination = filepath.Clean(destination)
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return ErrDestinationExists
		}
		return err
	}
	transportAbsolute, err := filepath.Abs(transportRoot)
	if err != nil {
		return err
	}
	canonicalTransport, err := filepath.EvalSymlinks(filepath.Clean(transportAbsolute))
	if err != nil {
		return err
	}
	resolvedDestination, err := resolveThroughExistingAncestor(destination)
	if err != nil {
		return err
	}
	if pathWithin(canonicalTransport, resolvedDestination) {
		return fmt.Errorf("%w: restoration destination is inside the transport", ErrInvalidTransport)
	}
	staging, err := newStagingDirectory(destination)
	if err != nil {
		return err
	}
	published := false
	defer func() {
		if !published {
			_ = os.RemoveAll(staging)
		}
	}()
	for _, entry := range index.Bundles {
		manifest, err := loadBoundManifest(transportAbsolute, entry)
		if err != nil {
			return err
		}
		bundleRoot := filepath.Join(staging, entry.Bundle)
		if err := os.Mkdir(bundleRoot, 0o750); err != nil {
			return err
		}
		if err := extractArchive(ctx, filepath.Join(transportAbsolute, entry.Archive), bundleRoot, manifest); err != nil {
			return err
		}
		if err := copyExclusive(filepath.Join(transportAbsolute, entry.Audit), filepath.Join(staging, entry.Audit)); err != nil {
			return err
		}
		if err := verifyRestoredBundle(ctx, bundleRoot, filepath.Join(staging, entry.Audit), manifest); err != nil {
			return err
		}
	}
	if err := os.Rename(staging, destination); err != nil {
		return err
	}
	published = true
	return nil
}

func loadBoundManifest(root string, entry BundleEntry) (BundleManifest, error) {
	if digest, err := hashFile(filepath.Join(root, entry.Manifest), maxJSONBytes); err != nil || digest != entry.ManifestSHA256 {
		return BundleManifest{}, fmt.Errorf("%w: manifest hash differs for %s", ErrInvalidTransport, entry.Bundle)
	}
	manifest, err := readJSON[BundleManifest](filepath.Join(root, entry.Manifest))
	if err != nil {
		return BundleManifest{}, err
	}
	if err := validateManifest(manifest); err != nil {
		return BundleManifest{}, err
	}
	archiveHash, err := hashFile(filepath.Join(root, entry.Archive), maxArchiveBytes)
	if err != nil || archiveHash != entry.ArchiveSHA256 || archiveHash != manifest.ArchiveSHA256 {
		return BundleManifest{}, fmt.Errorf("%w: archive hash differs for %s", ErrInvalidTransport, entry.Bundle)
	}
	auditHash, err := hashFile(filepath.Join(root, entry.Audit), maxJSONBytes)
	if err != nil || auditHash != entry.AuditSHA256 || auditHash != manifest.AuditSHA256 {
		return BundleManifest{}, fmt.Errorf("%w: audit hash differs for %s", ErrInvalidTransport, entry.Bundle)
	}
	if manifest.Bundle != entry.Bundle || manifest.Archive != entry.Archive || manifest.Audit != entry.Audit {
		return BundleManifest{}, fmt.Errorf("%w: bundle entry and manifest differ", ErrInvalidTransport)
	}
	return manifest, nil
}

func validateManifest(manifest BundleManifest) error {
	if manifest.SchemaVersion != SchemaVersion || !safeBaseName(manifest.Bundle) ||
		manifest.Archive != manifest.Bundle+".tar.gz" || !safeBaseName(manifest.Audit) ||
		!validSHA256(manifest.ArchiveSHA256) || !validSHA256(manifest.AuditSHA256) ||
		manifest.FileCount != len(manifest.Files) || manifest.FileCount == 0 ||
		manifest.Disposition != DispositionRejected && manifest.Disposition != DispositionSuperseded &&
			manifest.Disposition != DispositionAdmitted {
		return fmt.Errorf("%w: bundle manifest is incomplete", ErrInvalidTransport)
	}
	var total int64
	previous := ""
	byPath := make(map[string]Artifact, len(manifest.Files))
	for _, artifact := range manifest.Files {
		if !safeRelativePath(artifact.Path) || artifact.Path <= previous || artifact.Size < 0 ||
			artifact.Mode&^os.FileMode(0o777) != 0 || !validSHA256(artifact.SHA256) {
			return fmt.Errorf("%w: bundle artifact is invalid", ErrInvalidTransport)
		}
		previous = artifact.Path
		total += artifact.Size
		byPath[artifact.Path] = artifact
	}
	if total != manifest.TotalBytes || total < 0 {
		return fmt.Errorf("%w: bundle total differs", ErrInvalidTransport)
	}
	if manifest.Disposition == DispositionRejected {
		if len(manifest.Runs) != 0 {
			return fmt.Errorf("%w: rejected bundle unexpectedly contains admitted run bindings", ErrInvalidTransport)
		}
		_, err := manifestFailureHash(manifest)
		return err
	}
	if len(manifest.Runs) == 0 {
		return fmt.Errorf("%w: non-rejected bundle lacks run bindings", ErrInvalidTransport)
	}
	previous = ""
	for _, run := range manifest.Runs {
		if !safeBaseName(run.RunID) || run.RunID <= previous {
			return fmt.Errorf("%w: run binding order or identity is invalid", ErrInvalidTransport)
		}
		previous = run.RunID
		bindings := []struct{ path, digest string }{
			{run.RawInventoryPath, run.RawInventorySHA256}, {run.SummaryPath, run.SummarySHA256},
			{run.HistoryPath, run.HistorySHA256},
		}
		for _, binding := range bindings {
			artifact, ok := byPath[binding.path]
			if !ok || artifact.SHA256 != binding.digest || !validSHA256(binding.digest) {
				return fmt.Errorf("%w: run binding differs from manifest artifacts", ErrInvalidTransport)
			}
		}
	}
	return nil
}

func manifestFailureHash(manifest BundleManifest) (string, error) {
	for _, artifact := range manifest.Files {
		if artifact.Path == "failure.json" {
			return artifact.SHA256, nil
		}
	}
	return "", fmt.Errorf("%w: rejected manifest lacks failure.json", ErrInvalidTransport)
}

func validateIndex(index Index) error {
	if index.SchemaVersion != SchemaVersion || len(index.Bundles) == 0 || len(index.Lineage) != len(index.Bundles) {
		return fmt.Errorf("%w: transport index is incomplete", ErrInvalidTransport)
	}
	if err := validLineage(Lineage{SchemaVersion: LineageVersion, Entries: index.Lineage}); err != nil {
		return err
	}
	for position, entry := range index.Bundles {
		lineage := index.Lineage[position]
		if entry.Bundle != lineage.Bundle || entry.Audit != lineage.Audit ||
			entry.Archive != entry.Bundle+".tar.gz" || entry.Manifest != entry.Bundle+".manifest.json" ||
			!validSHA256(entry.ArchiveSHA256) || !validSHA256(entry.ManifestSHA256) || !validSHA256(entry.AuditSHA256) {
			return fmt.Errorf("%w: index bundle entry is invalid", ErrInvalidTransport)
		}
	}
	return nil
}

func validateExactFiles(root string, want []string) error {
	sort.Strings(want)
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	actual := make([]string, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() || entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: transport contains non-regular entry", ErrInvalidTransport)
		}
		actual = append(actual, entry.Name())
	}
	sort.Strings(actual)
	if !slices.Equal(actual, want) {
		return fmt.Errorf("%w: transport file set differs", ErrInvalidTransport)
	}
	return nil
}

func verifyRestoredBundle(ctx context.Context, root, auditPath string, manifest BundleManifest) error {
	files, total, err := inventoryTree(ctx, root)
	if err != nil || total != manifest.TotalBytes || !reflect.DeepEqual(files, manifest.Files) {
		return fmt.Errorf("%w: restored inventory differs", ErrInvalidTransport)
	}
	if manifest.Disposition == DispositionRejected {
		failureHash, rejectedErr := validateRejectedBundle(root, files)
		if rejectedErr != nil {
			return rejectedErr
		}
		_, auditHash, err := validateRejectionAudit(auditPath, failureHash)
		if err != nil || auditHash != manifest.AuditSHA256 {
			return fmt.Errorf("%w: restored rejection audit differs", ErrInvalidTransport)
		}
		return nil
	}
	runs, err := bindRuns(root, files)
	if err != nil || !reflect.DeepEqual(runs, manifest.Runs) {
		return fmt.Errorf("%w: restored run bindings differ", ErrInvalidTransport)
	}
	_, auditHash, err := validateAudit(auditPath, len(runs))
	if err != nil || auditHash != manifest.AuditSHA256 {
		return fmt.Errorf("%w: restored audit differs", ErrInvalidTransport)
	}
	return nil
}

func newStagingDirectory(destination string) (string, error) {
	parent := filepath.Dir(destination)
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return "", err
	}
	return os.MkdirTemp(parent, "."+filepath.Base(destination)+".staging-")
}

func pathWithin(root, candidate string) bool {
	relative, err := filepath.Rel(filepath.Clean(root), filepath.Clean(candidate))
	if err != nil {
		return false
	}
	return relative == "." || relative != ".." && !filepath.IsAbs(relative) &&
		!strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func resolveThroughExistingAncestor(path string) (string, error) {
	current := filepath.Clean(path)
	var missing []string
	for {
		_, err := os.Lstat(current)
		if err == nil {
			resolved, err := filepath.EvalSymlinks(current)
			if err != nil {
				return "", err
			}
			for index := len(missing) - 1; index >= 0; index-- {
				resolved = filepath.Join(resolved, missing[index])
			}
			return filepath.Clean(resolved), nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", err
		}
		missing = append(missing, filepath.Base(current))
		current = parent
	}
}
