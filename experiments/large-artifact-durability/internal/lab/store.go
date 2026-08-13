package lab

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	blobsDirectory            = "blobs"
	pendingDirectory          = "pending-references"
	referencesDirectory       = "references"
	acknowledgementsDirectory = "acknowledgements"
	maxRecordBytes            = 64 << 10
)

type ArtifactStore struct {
	root string
}

func NewArtifactStore(root string) (*ArtifactStore, error) {
	if root == "" {
		return nil, fmt.Errorf("%w: store root is required", ErrInvalidArtifact)
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, fmt.Errorf("create artifact store: %w", err)
	}
	rootInfo, err := os.Lstat(root)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%w: artifact store root is not a real directory", ErrInvalidArtifact)
	}
	for _, name := range storeDirectories() {
		path := filepath.Join(root, name)
		if err := os.Mkdir(path, 0o750); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("create artifact store directory %q: %w", name, err)
		}
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("%w: artifact store directory %q is not real", ErrInvalidArtifact, name)
		}
	}
	return &ArtifactStore{root: root}, nil
}

func (s *ArtifactStore) Produce(ctx context.Context, request ProduceRequest, hook BoundaryHook) (ArtifactReference, error) {
	if err := validateProduceRequest(request); err != nil {
		return ArtifactReference{}, err
	}
	if err := ctx.Err(); err != nil {
		return ArtifactReference{}, err
	}

	digest := sha256.Sum256(request.Content)
	digestText := hex.EncodeToString(digest[:])
	blobName := digestText + ".blob"
	referenceName := request.LogicalID + ".json"
	if request.Mode == ModeUnsafe {
		suffix := "-attempt-" + strconv.Itoa(int(request.Attempt))
		blobName = request.LogicalID + suffix + "-" + digestText + ".blob"
		referenceName = request.LogicalID + suffix + ".json"
	}
	reference := ArtifactReference{
		LogicalID:     request.LogicalID,
		Digest:        digestText,
		BlobName:      blobName,
		ReferenceName: referenceName,
		Size:          int64(len(request.Content)),
	}
	referenceData, err := encodeRecord(reference)
	if err != nil {
		return ArtifactReference{}, err
	}
	if request.Mode == ModeProtected {
		if err := s.rejectReferenceConflict(referenceName, reference); err != nil {
			return ArtifactReference{}, err
		}
	}
	if _, err := writeExclusiveOrValidate(s.path(blobsDirectory, blobName), request.Content); err != nil {
		return ArtifactReference{}, fmt.Errorf("publish artifact blob: %w", err)
	}
	if err := s.callHook(ctx, hook, BoundaryBlobPublished); err != nil {
		return ArtifactReference{}, err
	}

	pendingName := request.LogicalID + "-attempt-" + strconv.Itoa(int(request.Attempt)) + ".pending.json"
	pendingPath := s.path(pendingDirectory, pendingName)
	if _, err := writeExclusiveOrValidate(pendingPath, referenceData); err != nil {
		return ArtifactReference{}, fmt.Errorf("create pending artifact reference: %w", err)
	}
	if err := s.callHook(ctx, hook, BoundaryReferenceCreated); err != nil {
		return ArtifactReference{}, err
	}

	if _, err := writeExclusiveOrValidate(s.path(referencesDirectory, referenceName), referenceData); err != nil {
		if request.Mode == ModeProtected {
			return ArtifactReference{}, fmt.Errorf("%w: publish durable reference: %v", ErrArtifactConflict, err)
		}
		return ArtifactReference{}, fmt.Errorf("publish durable reference: %w", err)
	}
	if err := removeAndSync(pendingPath); err != nil {
		return ArtifactReference{}, fmt.Errorf("remove published pending reference: %w", err)
	}
	if err := s.callHook(ctx, hook, BoundaryReferencePublished); err != nil {
		return ArtifactReference{}, err
	}
	return reference, nil
}

func (s *ArtifactStore) Acknowledge(ctx context.Context, request AcknowledgeRequest, hook BoundaryHook) (Acknowledgement, error) {
	if !request.Mode.valid() || request.Attempt < 1 || !safeComponent(request.ConsumerID) {
		return Acknowledgement{}, fmt.Errorf("%w: valid mode, attempt, and consumer are required", ErrInvalidArtifact)
	}
	if _, err := s.Read(ctx, request.Reference); err != nil {
		return Acknowledgement{}, fmt.Errorf("verify artifact before acknowledgement: %w", err)
	}
	acknowledgement := Acknowledgement{
		LogicalID:     request.Reference.LogicalID,
		Digest:        request.Reference.Digest,
		ConsumerID:    request.ConsumerID,
		ReferenceName: request.Reference.ReferenceName,
	}
	data, err := encodeRecord(acknowledgement)
	if err != nil {
		return Acknowledgement{}, err
	}
	name := request.Reference.LogicalID + "--" + request.ConsumerID + ".json"
	if request.Mode == ModeUnsafe {
		name = request.Reference.LogicalID + "--" + request.ConsumerID + "--attempt-" + strconv.Itoa(int(request.Attempt)) + ".json"
	}
	if _, err := writeExclusiveOrValidate(s.path(acknowledgementsDirectory, name), data); err != nil {
		if request.Mode == ModeProtected {
			return Acknowledgement{}, fmt.Errorf("%w: publish acknowledgement: %v", ErrArtifactConflict, err)
		}
		return Acknowledgement{}, fmt.Errorf("publish acknowledgement: %w", err)
	}
	if err := s.callHook(ctx, hook, BoundaryAcknowledgementPublished); err != nil {
		return Acknowledgement{}, err
	}
	return acknowledgement, nil
}

func (s *ArtifactStore) Read(ctx context.Context, reference ArtifactReference) ([]byte, error) {
	if err := validateReference(reference); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var durable ArtifactReference
	if err := readRecord(s.path(referencesDirectory, reference.ReferenceName), &durable); err != nil {
		return nil, fmt.Errorf("read durable artifact reference: %w", err)
	}
	if durable != reference {
		return nil, fmt.Errorf("%w: supplied reference differs from durable reference", ErrArtifactConflict)
	}
	content, err := readBoundedRegular(s.path(blobsDirectory, reference.BlobName), MaxArtifactBytes)
	if err != nil {
		return nil, fmt.Errorf("read artifact blob: %w", err)
	}
	if len(content) > MaxArtifactBytes || int64(len(content)) != reference.Size || digestBytes(content) != reference.Digest {
		return nil, fmt.Errorf("%w: artifact blob differs from durable reference", ErrArtifactConflict)
	}
	return content, nil
}

func (s *ArtifactStore) Snapshot(ctx context.Context) (StoreSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return StoreSnapshot{}, err
	}
	groups := make([][]StoredEntry, 0, 4)
	for _, directory := range storeDirectories() {
		entries, err := snapshotDirectory(s.path(directory))
		if err != nil {
			return StoreSnapshot{}, fmt.Errorf("snapshot %s: %w", directory, err)
		}
		groups = append(groups, entries)
	}
	return StoreSnapshot{
		Blobs: groups[0], PendingReferences: groups[1], References: groups[2], Acknowledgements: groups[3],
	}, nil
}

func (s *ArtifactStore) Reconcile(ctx context.Context) (ReconcileReport, error) {
	if err := ctx.Err(); err != nil {
		return ReconcileReport{}, err
	}
	reachable := make(map[string]struct{})
	referenceEntries, err := os.ReadDir(s.path(referencesDirectory))
	if err != nil {
		return ReconcileReport{}, fmt.Errorf("read durable references: %w", err)
	}
	for _, entry := range referenceEntries {
		if !entry.Type().IsRegular() || !strings.HasSuffix(entry.Name(), ".json") {
			return ReconcileReport{}, fmt.Errorf("%w: unexpected durable reference entry %q", ErrInvalidArtifact, entry.Name())
		}
		var reference ArtifactReference
		if err := readRecord(s.path(referencesDirectory, entry.Name()), &reference); err != nil {
			return ReconcileReport{}, err
		}
		if err := validateReference(reference); err != nil || reference.ReferenceName != entry.Name() {
			return ReconcileReport{}, fmt.Errorf("%w: invalid durable reference %q", ErrInvalidArtifact, entry.Name())
		}
		reachable[reference.BlobName] = struct{}{}
	}
	report := ReconcileReport{ReachableBlobs: len(reachable)}
	pending, err := os.ReadDir(s.path(pendingDirectory))
	if err != nil {
		return ReconcileReport{}, fmt.Errorf("read pending references: %w", err)
	}
	for _, entry := range pending {
		if !entry.Type().IsRegular() {
			return ReconcileReport{}, fmt.Errorf("%w: unexpected pending reference entry %q", ErrInvalidArtifact, entry.Name())
		}
		if err := removeAndSync(s.path(pendingDirectory, entry.Name())); err != nil {
			return ReconcileReport{}, err
		}
		report.RemovedPendingReferences = append(report.RemovedPendingReferences, entry.Name())
	}
	blobs, err := os.ReadDir(s.path(blobsDirectory))
	if err != nil {
		return ReconcileReport{}, fmt.Errorf("read blobs: %w", err)
	}
	for _, entry := range blobs {
		if !entry.Type().IsRegular() || !strings.HasSuffix(entry.Name(), ".blob") {
			return ReconcileReport{}, fmt.Errorf("%w: unexpected blob entry %q", ErrInvalidArtifact, entry.Name())
		}
		if _, found := reachable[entry.Name()]; found {
			continue
		}
		if err := removeAndSync(s.path(blobsDirectory, entry.Name())); err != nil {
			return ReconcileReport{}, err
		}
		report.RemovedBlobs = append(report.RemovedBlobs, entry.Name())
	}
	sort.Strings(report.RemovedBlobs)
	sort.Strings(report.RemovedPendingReferences)
	return report, nil
}

func (s *ArtifactStore) rejectReferenceConflict(name string, expected ArtifactReference) error {
	var existing ArtifactReference
	err := readRecord(s.path(referencesDirectory, name), &existing)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read existing durable reference: %w", err)
	}
	if existing != expected {
		return fmt.Errorf("%w: logical artifact %q already names a different blob", ErrArtifactConflict, expected.LogicalID)
	}
	return nil
}

func (s *ArtifactStore) callHook(ctx context.Context, hook BoundaryHook, boundary Boundary) error {
	if hook == nil {
		return nil
	}
	snapshot, err := s.Snapshot(ctx)
	if err != nil {
		return err
	}
	return hook(ctx, boundary, snapshot)
}

func (s *ArtifactStore) path(parts ...string) string {
	return filepath.Join(append([]string{s.root}, parts...)...)
}

func validateProduceRequest(request ProduceRequest) error {
	if !request.Mode.valid() || !safeComponent(request.LogicalID) || request.Attempt < 1 ||
		len(request.Content) == 0 || len(request.Content) > MaxArtifactBytes {
		return fmt.Errorf("%w: valid mode, logical ID, attempt, and bounded nonempty content are required", ErrInvalidArtifact)
	}
	return nil
}

func validateReference(reference ArtifactReference) error {
	digest, err := hex.DecodeString(reference.Digest)
	if !safeComponent(reference.LogicalID) || !safeComponent(reference.BlobName) ||
		!safeComponent(reference.ReferenceName) || err != nil || len(digest) != sha256.Size ||
		hex.EncodeToString(digest) != reference.Digest || reference.Size < 1 || reference.Size > MaxArtifactBytes {
		return fmt.Errorf("%w: invalid artifact reference", ErrInvalidArtifact)
	}
	return nil
}

func safeComponent(value string) bool {
	if value == "" || len(value) > 255 || value == "." || value == ".." || filepath.Base(value) != value || strings.Contains(value, "\\") {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || character == '-' || character == '_' || character == '.' {
			continue
		}
		return false
	}
	return true
}

func storeDirectories() []string {
	return []string{blobsDirectory, pendingDirectory, referencesDirectory, acknowledgementsDirectory}
}

func encodeRecord(value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode artifact record: %w", err)
	}
	return append(data, '\n'), nil
}

func readRecord(path string, destination any) error {
	data, err := readBoundedRegular(path, maxRecordBytes)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return fmt.Errorf("decode %s: trailing data", path)
	}
	return nil
}

func snapshotDirectory(path string) ([]StoredEntry, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	result := make([]StoredEntry, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("%w: non-regular entry %q", ErrInvalidArtifact, entry.Name())
		}
		data, err := os.ReadFile(filepath.Join(path, entry.Name()))
		if err != nil {
			return nil, err
		}
		result = append(result, StoredEntry{Name: entry.Name(), Size: info.Size(), SHA256: digestBytes(data)})
	}
	sort.Slice(result, func(left, right int) bool { return result[left].Name < result[right].Name })
	return result, nil
}

func digestBytes(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func writeExclusiveOrValidate(path string, content []byte) (bool, error) {
	file, err := os.CreateTemp(filepath.Dir(path), ".artifact-*")
	if err != nil {
		return false, err
	}
	temporary := file.Name()
	defer func() { _ = os.Remove(temporary) }()
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return false, err
	}
	if _, err := file.Write(content); err != nil {
		_ = file.Close()
		return false, err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return false, err
	}
	if err := file.Close(); err != nil {
		return false, err
	}
	if err := os.Link(temporary, path); errors.Is(err, os.ErrExist) {
		existing, readErr := readBoundedRegular(path, len(content))
		if readErr != nil {
			return false, readErr
		}
		if !bytes.Equal(existing, content) {
			return false, fmt.Errorf("existing content conflicts at %s", path)
		}
		return false, nil
	} else if err != nil {
		return false, err
	}
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		return false, err
	}
	return true, nil
}

func removeAndSync(path string) error {
	if err := os.Remove(path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return err
	}
	return directory.Close()
}
