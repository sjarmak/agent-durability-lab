package lab

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
)

const maxEvidenceFileBytes = 64 << 20

type auditBundle struct {
	files map[string][]byte
}

func loadAuditBundle(rootPath string) (auditBundle, Manifest, error) {
	absolute, err := filepath.Abs(rootPath)
	if err != nil {
		return auditBundle{}, Manifest{}, err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil || resolved != filepath.Clean(absolute) {
		return auditBundle{}, Manifest{}, fmt.Errorf("%w: evidence root traverses a symlink", ErrInvalidArtifact)
	}
	root, err := os.OpenRoot(resolved)
	if err != nil {
		return auditBundle{}, Manifest{}, err
	}
	defer func() { _ = root.Close() }()
	manifestData, err := readRootRegularOnce(root, "manifest.json", maxEvidenceJSONBytes)
	if err != nil {
		return auditBundle{}, Manifest{}, err
	}
	var manifest Manifest
	if err := decodeStrictJSON(manifestData, &manifest); err != nil {
		return auditBundle{}, Manifest{}, err
	}
	files := map[string][]byte{"manifest.json": manifestData}
	var directories []string
	if err := walkAuditRoot(root, ".", files, &directories); err != nil {
		return auditBundle{}, Manifest{}, err
	}
	actualDigests := make(map[string]string, len(files)-1)
	for name, data := range files {
		if name != "manifest.json" {
			actualDigests[name] = digestBytes(data)
		}
	}
	if !reflect.DeepEqual(actualDigests, manifest.Files) {
		return auditBundle{}, Manifest{}, fmt.Errorf("%w: evidence file inventory or digest differs", ErrArtifactConflict)
	}
	expectedDirectories := append([]string(nil), manifest.Directories...)
	sort.Strings(expectedDirectories)
	sort.Strings(directories)
	if !reflect.DeepEqual(directories, expectedDirectories) {
		return auditBundle{}, Manifest{}, fmt.Errorf("%w: evidence directory inventory differs", ErrArtifactConflict)
	}
	return auditBundle{files: files}, manifest, nil
}

func walkAuditRoot(root *os.Root, directory string, files map[string][]byte, directories *[]string) error {
	before, err := root.Lstat(directory)
	if err != nil || !before.IsDir() {
		return errors.New("evidence directory changed before open")
	}
	handle, err := root.Open(directory)
	if err != nil {
		return err
	}
	opened, statErr := handle.Stat()
	if statErr != nil || !opened.IsDir() || !os.SameFile(before, opened) {
		_ = handle.Close()
		return errors.New("evidence directory changed before enumeration")
	}
	entries, readErr := handle.ReadDir(-1)
	after, afterErr := handle.Stat()
	closeErr := handle.Close()
	if err := errors.Join(readErr, afterErr, closeErr); err != nil {
		return err
	}
	if !os.SameFile(opened, after) {
		return errors.New("evidence directory changed while enumerating")
	}
	for _, entry := range entries {
		name := entry.Name()
		if directory != "." {
			name = filepath.Join(directory, name)
		}
		name = filepath.ToSlash(name)
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: evidence symlink %q", ErrInvalidArtifact, name)
		}
		if entry.IsDir() {
			*directories = append(*directories, name)
			if err := walkAuditRoot(root, name, files, directories); err != nil {
				return err
			}
			continue
		}
		data, err := readRootRegularOnce(root, name, maxEvidenceFileBytes)
		if err != nil {
			return fmt.Errorf("read evidence %q: %w", name, err)
		}
		if prior, found := files[name]; found && !bytes.Equal(prior, data) {
			return fmt.Errorf("%w: evidence file %q changed between reads", ErrArtifactConflict, name)
		}
		files[name] = data
	}
	return nil
}

func readRootRegularOnce(root *os.Root, name string, limit int64) (data []byte, returnErr error) {
	before, err := root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() || before.Size() < 0 || before.Size() > limit {
		return nil, errors.New("not a bounded regular file")
	}
	file, err := root.Open(name)
	if err != nil {
		return nil, err
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(before, opened) {
		return nil, errors.New("evidence changed before open")
	}
	data, err = io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || int64(len(data)) > limit {
		return nil, errors.New("evidence exceeds read bound")
	}
	after, err := file.Stat()
	if err != nil || !os.SameFile(opened, after) || after.Size() != int64(len(data)) ||
		after.Size() != opened.Size() || !after.ModTime().Equal(opened.ModTime()) {
		return nil, errors.New("evidence changed while reading")
	}
	return data, nil
}

func (b auditBundle) decode(name string, target any) error {
	data, found := b.files[name]
	if !found {
		return fmt.Errorf("%w: evidence file %q is absent", ErrInvalidArtifact, name)
	}
	return decodeStrictJSON(data, target)
}

func (b auditBundle) snapshot(prefix string) StoreSnapshot {
	return StoreSnapshot{Blobs: b.entries(filepath.Join(prefix, blobsDirectory)),
		PendingReferences: b.entries(filepath.Join(prefix, pendingDirectory)),
		References:        b.entries(filepath.Join(prefix, referencesDirectory)),
		Acknowledgements:  b.entries(filepath.Join(prefix, acknowledgementsDirectory))}
}

func (b auditBundle) entries(prefix string) []StoredEntry {
	prefix = filepath.ToSlash(prefix) + "/"
	entries := make([]StoredEntry, 0)
	for name, data := range b.files {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		remainder := strings.TrimPrefix(name, prefix)
		if remainder == "" || strings.Contains(remainder, "/") {
			continue
		}
		entries = append(entries, StoredEntry{Name: remainder, Size: int64(len(data)), SHA256: digestBytes(data)})
	}
	sort.Slice(entries, func(left, right int) bool { return entries[left].Name < entries[right].Name })
	return entries
}

type bundleStorageDriver struct {
	mode  Mode
	files map[string][]byte
}

func (d bundleStorageDriver) Name() string { return "large-artifact-" + string(d.mode) }
func (d bundleStorageDriver) Type() string { return "content-addressed-file" }

func (bundleStorageDriver) Store(
	converter.StorageDriverStoreContext,
	[]*commonpb.Payload,
) ([]converter.StorageDriverClaim, error) {
	return nil, errors.New("audit replay cannot store external payloads")
}

func (d bundleStorageDriver) Retrieve(
	ctx converter.StorageDriverRetrieveContext,
	claims []converter.StorageDriverClaim,
) ([]*commonpb.Payload, error) {
	if ctx.Context == nil || len(claims) == 0 {
		return nil, fmt.Errorf("%w: audit Retrieve requires context and claims", ErrInvalidArtifact)
	}
	result := make([]*commonpb.Payload, 0, len(claims))
	for _, claim := range claims {
		if err := ctx.Context.Err(); err != nil {
			return nil, err
		}
		key, digest, size, err := validateExternalClaim(claim)
		if err != nil {
			return nil, err
		}
		data, found := d.files[filepath.ToSlash(filepath.Join("external-store", externalObjectsDirectory, key))]
		if !found || len(data) != size || digestBytes(data) != digest {
			return nil, fmt.Errorf("%w: external object differs from claim", ErrArtifactConflict)
		}
		payload := &commonpb.Payload{}
		if err := proto.Unmarshal(data, payload); err != nil {
			return nil, err
		}
		result = append(result, payload)
	}
	return result, nil
}

var _ converter.StorageDriver = bundleStorageDriver{}

func auditReplayStorage(mode Mode, files map[string][]byte) converter.ExternalStorage {
	return converter.ExternalStorage{
		Drivers:              []converter.StorageDriver{bundleStorageDriver{mode: mode, files: files}},
		PayloadSizeThreshold: ExternalStorageThreshold,
	}
}
