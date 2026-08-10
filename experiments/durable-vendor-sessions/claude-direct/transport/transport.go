package transport

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
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
	index, err := writeTransport(ctx, resolved.SourceRoot, staging, lineage)
	if err != nil {
		return Index{}, err
	}
	if err := os.Rename(staging, resolved.OutputRoot); err != nil {
		return Index{}, err
	}
	published = true
	return index, nil
}

func prepareBuild(ctx context.Context, config BuildConfig) (BuildConfig, Lineage, error) {
	if err := checkContext(ctx); err != nil {
		return BuildConfig{}, Lineage{}, err
	}
	resolved, err := resolveBuildConfig(config)
	if err != nil {
		return BuildConfig{}, Lineage{}, err
	}
	lineage, err := readJSON[Lineage](resolved.LineagePath)
	if err != nil {
		return BuildConfig{}, Lineage{}, err
	}
	if err := validateLineage(lineage); err != nil {
		return BuildConfig{}, Lineage{}, err
	}
	if err := validateSourceBundles(resolved.SourceRoot, lineage.Entries); err != nil {
		return BuildConfig{}, Lineage{}, err
	}
	return resolved, lineage, nil
}

func writeTransport(ctx context.Context, sourceRoot, staging string, lineage Lineage) (Index, error) {
	index := Index{SchemaVersion: SchemaVersion, Lineage: slices.Clone(lineage.Entries)}
	for _, entry := range lineage.Entries {
		if err := checkContext(ctx); err != nil {
			return Index{}, err
		}
		bundle, err := writeBundleTransport(ctx, sourceRoot, staging, entry.Bundle)
		if err != nil {
			return Index{}, err
		}
		index.Bundles = append(index.Bundles, bundle)
	}
	if err := writeJSONExclusive(filepath.Join(staging, IndexFile), index); err != nil {
		return Index{}, err
	}
	if _, err := Verify(ctx, staging); err != nil {
		return Index{}, err
	}
	return index, nil
}

func writeBundleTransport(ctx context.Context, sourceRoot, staging, bundle string) (BundleEntry, error) {
	source := filepath.Join(sourceRoot, bundle)
	manifest, err := inspectBundle(ctx, source, bundle)
	if err != nil {
		return BundleEntry{}, err
	}
	manifest.Archive = bundle + ".tar.gz"
	archivePath := filepath.Join(staging, manifest.Archive)
	if err := writeArchive(ctx, source, archivePath, manifest); err != nil {
		return BundleEntry{}, err
	}
	manifest.ArchiveSHA256, err = hashRegularFile(archivePath, maxArchiveBytes)
	if err != nil {
		return BundleEntry{}, err
	}
	if err := validateBundleManifest(manifest); err != nil {
		return BundleEntry{}, err
	}
	manifestName := bundle + ".manifest.json"
	manifestPath := filepath.Join(staging, manifestName)
	if err := writeJSONExclusive(manifestPath, manifest); err != nil {
		return BundleEntry{}, err
	}
	manifestSHA, err := hashRegularFile(manifestPath, maxJSONBytes)
	if err != nil {
		return BundleEntry{}, err
	}
	return BundleEntry{Bundle: bundle, Archive: manifest.Archive, ArchiveSHA256: manifest.ArchiveSHA256,
		Manifest: manifestName, ManifestSHA256: manifestSHA}, nil
}

func Verify(ctx context.Context, root string) (Index, error) {
	if err := checkContext(ctx); err != nil {
		return Index{}, err
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return Index{}, err
	}
	if err := ensureDirectory(absolute); err != nil {
		return Index{}, err
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
		wantFiles = append(wantFiles, entry.Archive, entry.Manifest)
	}
	if err := validateTransportFiles(absolute, wantFiles); err != nil {
		return Index{}, err
	}
	for _, entry := range index.Bundles {
		if err := checkContext(ctx); err != nil {
			return Index{}, err
		}
		manifest, err := loadBoundManifest(absolute, entry)
		if err != nil {
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
	destination, err := resolvePlannedPath(destinationRoot)
	if err != nil {
		return err
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
	transportAbsolute, err := filepath.EvalSymlinks(transportRoot)
	if err != nil {
		return err
	}
	if err := extractBundles(ctx, transportAbsolute, staging, index); err != nil {
		return err
	}
	if err := verifyRestoredBundles(ctx, transportAbsolute, staging, index); err != nil {
		return err
	}
	if err := os.Rename(staging, destination); err != nil {
		return err
	}
	published = true
	return nil
}

func extractBundles(ctx context.Context, transportRoot, staging string, index Index) (returnErr error) {
	root, err := os.OpenRoot(staging)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, root.Close()) }()
	for _, entry := range index.Bundles {
		manifest, err := loadBoundManifest(transportRoot, entry)
		if err != nil {
			return err
		}
		if err := extractArchive(ctx, filepath.Join(transportRoot, entry.Archive), manifest, root); err != nil {
			return err
		}
	}
	return nil
}

func verifyRestoredBundles(ctx context.Context, transportRoot, staging string, index Index) error {
	for _, entry := range index.Bundles {
		manifest, err := loadBoundManifest(transportRoot, entry)
		if err != nil {
			return err
		}
		observed, err := inspectBundle(ctx, filepath.Join(staging, entry.Bundle), entry.Bundle)
		if err != nil {
			return err
		}
		if manifest.FileCount != observed.FileCount || manifest.TotalBytes != observed.TotalBytes ||
			!reflect.DeepEqual(manifest.Files, observed.Files) || !reflect.DeepEqual(manifest.Runs, observed.Runs) {
			return fmt.Errorf("%w: restored bundle differs for %s", ErrInvalidTransport, entry.Bundle)
		}
	}
	return nil
}

func newStagingDirectory(destination string) (string, error) {
	if _, err := os.Lstat(destination); !errors.Is(err, fs.ErrNotExist) {
		if err == nil {
			return "", ErrDestinationExists
		}
		return "", err
	}
	parent := filepath.Dir(destination)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return "", err
	}
	return os.MkdirTemp(parent, "."+filepath.Base(destination)+".staging-")
}

func resolveBuildConfig(config BuildConfig) (BuildConfig, error) {
	if config.SourceRoot == "" || config.LineagePath == "" || config.OutputRoot == "" {
		return BuildConfig{}, fmt.Errorf("%w: source, lineage, and output are required", ErrInvalidTransport)
	}
	var err error
	config.SourceRoot, err = filepath.Abs(config.SourceRoot)
	if err != nil {
		return BuildConfig{}, err
	}
	config.SourceRoot, err = filepath.EvalSymlinks(config.SourceRoot)
	if err != nil {
		return BuildConfig{}, err
	}
	config.LineagePath, err = filepath.Abs(config.LineagePath)
	if err != nil {
		return BuildConfig{}, err
	}
	config.LineagePath, err = filepath.EvalSymlinks(config.LineagePath)
	if err != nil {
		return BuildConfig{}, err
	}
	config.OutputRoot, err = resolvePlannedPath(config.OutputRoot)
	if err != nil {
		return BuildConfig{}, err
	}
	if err := ensureDirectory(config.SourceRoot); err != nil {
		return BuildConfig{}, err
	}
	if pathWithin(config.SourceRoot, config.OutputRoot) || pathWithin(config.OutputRoot, config.SourceRoot) {
		return BuildConfig{}, fmt.Errorf("%w: source and output trees overlap", ErrInvalidTransport)
	}
	return config, nil
}

func validateLineage(lineage Lineage) error {
	if lineage.SchemaVersion != SchemaVersion || len(lineage.Entries) == 0 || len(lineage.Entries) > maxBundles {
		return fmt.Errorf("%w: lineage identity", ErrInvalidTransport)
	}
	seen := make(map[string]bool, len(lineage.Entries))
	for index, entry := range lineage.Entries {
		if !safeBundleName(entry.Bundle) || seen[entry.Bundle] || strings.TrimSpace(entry.Reason) == "" {
			return fmt.Errorf("%w: lineage entry %d", ErrInvalidTransport, index)
		}
		seen[entry.Bundle] = true
		last := index == len(lineage.Entries)-1
		if last {
			if entry.Disposition != DispositionAdmitted || entry.SupersededBy != "" {
				return fmt.Errorf("%w: final lineage entry", ErrInvalidTransport)
			}
			continue
		}
		if entry.Disposition != DispositionRejected && entry.Disposition != DispositionSuperseded {
			return fmt.Errorf("%w: non-final lineage disposition", ErrInvalidTransport)
		}
		if entry.SupersededBy != lineage.Entries[index+1].Bundle {
			return fmt.Errorf("%w: broken correction lineage", ErrInvalidTransport)
		}
	}
	return nil
}

func validateSourceBundles(root string, lineage []LineageEntry) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	want := make([]string, len(lineage))
	for index, entry := range lineage {
		want[index] = entry.Bundle
	}
	sort.Strings(want)
	got := make([]string, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&fs.ModeSymlink != 0 {
			return fmt.Errorf("%w: source root entry %s", ErrInvalidTransport, entry.Name())
		}
		got = append(got, entry.Name())
	}
	sort.Strings(got)
	if !slices.Equal(got, want) {
		return fmt.Errorf("%w: source bundle set differs", ErrInvalidTransport)
	}
	return nil
}

func validateIndex(index Index) error {
	lineage := Lineage{SchemaVersion: index.SchemaVersion, Entries: index.Lineage}
	if err := validateLineage(lineage); err != nil || len(index.Bundles) != len(index.Lineage) {
		return fmt.Errorf("%w: index identity", ErrInvalidTransport)
	}
	for position, entry := range index.Bundles {
		bundle := index.Lineage[position].Bundle
		if entry.Bundle != bundle || entry.Archive != bundle+".tar.gz" || entry.Manifest != bundle+".manifest.json" ||
			!validSHA256(entry.ArchiveSHA256) || !validSHA256(entry.ManifestSHA256) {
			return fmt.Errorf("%w: index bundle %d", ErrInvalidTransport, position)
		}
	}
	return nil
}

func validateBundleManifest(manifest BundleManifest) error {
	if manifest.SchemaVersion != SchemaVersion || !safeBundleName(manifest.Bundle) || manifest.Archive != manifest.Bundle+".tar.gz" ||
		!validSHA256(manifest.ArchiveSHA256) || manifest.FileCount == 0 || manifest.FileCount > maxBundleFiles ||
		manifest.FileCount != len(manifest.Files) || manifest.TotalBytes < 0 || manifest.TotalBytes > maxBundleBytes || len(manifest.Runs) > maxBundleFiles {
		return fmt.Errorf("%w: bundle manifest identity", ErrInvalidTransport)
	}
	var total int64
	previous := ""
	byPath := make(map[string]Artifact, len(manifest.Files))
	for _, artifact := range manifest.Files {
		if !safeRelativePath(artifact.Path) || artifact.Path <= previous || artifact.Size < 0 || artifact.Size > maxArtifactBytes ||
			artifact.Mode != artifact.Mode.Perm() || !validSHA256(artifact.SHA256) {
			return fmt.Errorf("%w: bundle artifact %q", ErrInvalidTransport, artifact.Path)
		}
		if total > int64(^uint64(0)>>1)-artifact.Size {
			return fmt.Errorf("%w: bundle size overflow", ErrInvalidTransport)
		}
		total += artifact.Size
		previous = artifact.Path
		byPath[artifact.Path] = artifact
	}
	if total != manifest.TotalBytes || total > maxBundleBytes {
		return fmt.Errorf("%w: bundle byte count", ErrInvalidTransport)
	}
	previous = ""
	for _, run := range manifest.Runs {
		if run.RunID == "" || run.RunID <= previous || run.DeclaredRawInventorySHA256 != run.RawInventorySHA256 {
			return fmt.Errorf("%w: run binding order", ErrInvalidTransport)
		}
		for path, digest := range map[string]string{
			run.RawInventoryPath:   run.RawInventorySHA256,
			run.EffectiveInputPath: run.EffectiveInputSHA256,
			run.CommonManifestPath: run.CommonManifestSHA256,
			run.VerdictPath:        run.VerdictSHA256,
		} {
			artifact, ok := byPath[path]
			if !ok || artifact.SHA256 != digest || !validSHA256(digest) {
				return fmt.Errorf("%w: run binding for %s", ErrInvalidTransport, run.RunID)
			}
		}
		previous = run.RunID
	}
	return nil
}

func validateTransportFiles(root string, want []string) error {
	sort.Strings(want)
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	got := make([]string, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Mode()&fs.ModeSymlink != 0 {
			return fmt.Errorf("%w: transport entry %s", ErrInvalidTransport, entry.Name())
		}
		got = append(got, entry.Name())
	}
	sort.Strings(got)
	if !slices.Equal(got, want) {
		return fmt.Errorf("%w: transport file set differs", ErrInvalidTransport)
	}
	return nil
}

func safeBundleName(name string) bool {
	return safeRelativePath(name) && !strings.Contains(name, "/") && len(name) <= 200
}

func pathWithin(root, candidate string) bool {
	relative, err := filepath.Rel(root, candidate)
	return err == nil && relative != ".." && !filepath.IsAbs(relative) && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func resolvePlannedPath(value string) (string, error) {
	absolute, err := filepath.Abs(value)
	if err != nil {
		return "", err
	}
	parent := filepath.Dir(absolute)
	parts := []string{filepath.Base(absolute)}
	for {
		resolved, err := filepath.EvalSymlinks(parent)
		if err == nil {
			for index := len(parts) - 1; index >= 0; index-- {
				resolved = filepath.Join(resolved, parts[index])
			}
			return resolved, nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return "", err
		}
		next := filepath.Dir(parent)
		if next == parent {
			return "", err
		}
		parts = append(parts, filepath.Base(parent))
		parent = next
	}
}

func loadBoundManifest(root string, entry BundleEntry) (BundleManifest, error) {
	path := filepath.Join(root, entry.Manifest)
	data, err := readRegularFile(path, maxJSONBytes)
	if err != nil {
		return BundleManifest{}, err
	}
	if hashBytes(data) != entry.ManifestSHA256 {
		return BundleManifest{}, fmt.Errorf("%w: manifest hash for %s", ErrInvalidTransport, entry.Bundle)
	}
	manifest, err := decodeJSON[BundleManifest](entry.Manifest, data)
	if err != nil {
		return BundleManifest{}, err
	}
	if err := validateBundleManifest(manifest); err != nil {
		return BundleManifest{}, err
	}
	if manifest.Bundle != entry.Bundle || manifest.Archive != entry.Archive || manifest.ArchiveSHA256 != entry.ArchiveSHA256 {
		return BundleManifest{}, fmt.Errorf("%w: bundle index binding for %s", ErrInvalidTransport, entry.Bundle)
	}
	return manifest, nil
}
