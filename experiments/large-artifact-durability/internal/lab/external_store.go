package lab

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
)

const (
	externalObjectsDirectory = "objects"
	maxExternalStoredBytes   = MaxArtifactBytes + maxRecordBytes
	ExternalStorageThreshold = 256 << 10
)

type FileStorageDriver struct {
	root string
	mode Mode
	hook BoundaryHook
}

func NewFileStorageDriver(root string, mode Mode, hook BoundaryHook) (*FileStorageDriver, error) {
	if root == "" || !mode.valid() {
		return nil, fmt.Errorf("%w: external storage requires root and mode", ErrInvalidArtifact)
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, fmt.Errorf("create external storage: %w", err)
	}
	rootInfo, err := os.Lstat(root)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%w: external storage root is not a real directory", ErrInvalidArtifact)
	}
	objectsPath := filepath.Join(root, externalObjectsDirectory)
	if err := os.Mkdir(objectsPath, 0o750); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("create external object directory: %w", err)
	}
	objectsInfo, err := os.Lstat(objectsPath)
	if err != nil || !objectsInfo.IsDir() || objectsInfo.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%w: external object directory is not real", ErrInvalidArtifact)
	}
	return &FileStorageDriver{root: root, mode: mode, hook: hook}, nil
}

func (d *FileStorageDriver) Name() string {
	return "large-artifact-" + string(d.mode)
}

func (d *FileStorageDriver) Type() string {
	return "content-addressed-file"
}

func (d *FileStorageDriver) Store(
	ctx converter.StorageDriverStoreContext,
	payloads []*commonpb.Payload,
) ([]converter.StorageDriverClaim, error) {
	if ctx.Context == nil || len(payloads) == 0 {
		return nil, fmt.Errorf("%w: external Store requires context and payloads", ErrInvalidArtifact)
	}
	claims := make([]converter.StorageDriverClaim, 0, len(payloads))
	for _, payload := range payloads {
		if err := ctx.Context.Err(); err != nil {
			return nil, err
		}
		if payload == nil {
			return nil, fmt.Errorf("%w: nil external payload", ErrInvalidArtifact)
		}
		encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("encode external payload: %w", err)
		}
		if len(encoded) < 1 || len(encoded) > maxExternalStoredBytes {
			return nil, fmt.Errorf("%w: external payload exceeds bound", ErrInvalidArtifact)
		}
		digest := digestBytes(encoded)
		key, err := d.publishObject(digest, encoded)
		if err != nil {
			return nil, err
		}
		claims = append(claims, converter.StorageDriverClaim{ClaimData: map[string]string{
			"key": key, "sha256": digest, "size": strconv.Itoa(len(encoded)),
		}})
	}
	if d.hook != nil {
		snapshot, err := d.Snapshot(ctx.Context)
		if err != nil {
			return nil, err
		}
		if err := d.hook(ctx.Context, BoundaryExternalStorageStored, snapshot); err != nil {
			return nil, err
		}
	}
	return claims, nil
}

func (d *FileStorageDriver) Retrieve(
	ctx converter.StorageDriverRetrieveContext,
	claims []converter.StorageDriverClaim,
) ([]*commonpb.Payload, error) {
	if ctx.Context == nil || len(claims) == 0 {
		return nil, fmt.Errorf("%w: external Retrieve requires context and claims", ErrInvalidArtifact)
	}
	payloads := make([]*commonpb.Payload, 0, len(claims))
	for _, claim := range claims {
		if err := ctx.Context.Err(); err != nil {
			return nil, err
		}
		key, digest, size, err := validateExternalClaim(claim)
		if err != nil {
			return nil, err
		}
		data, err := readBoundedRegular(filepath.Join(d.root, externalObjectsDirectory, key), maxExternalStoredBytes)
		if err != nil {
			return nil, fmt.Errorf("retrieve external object: %w", err)
		}
		if len(data) != size || digestBytes(data) != digest {
			return nil, fmt.Errorf("%w: external object differs from claim", ErrArtifactConflict)
		}
		payload := &commonpb.Payload{}
		if err := proto.Unmarshal(data, payload); err != nil {
			return nil, fmt.Errorf("decode external payload: %w", err)
		}
		payloads = append(payloads, payload)
	}
	return payloads, nil
}

func (d *FileStorageDriver) Snapshot(ctx context.Context) (StoreSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return StoreSnapshot{}, err
	}
	entries, err := snapshotDirectory(filepath.Join(d.root, externalObjectsDirectory))
	if err != nil {
		return StoreSnapshot{}, err
	}
	return StoreSnapshot{Blobs: entries}, nil
}

func (d *FileStorageDriver) publishObject(digest string, data []byte) (string, error) {
	directory := filepath.Join(d.root, externalObjectsDirectory)
	if d.mode == ModeProtected {
		key := digest + ".payload"
		if _, err := writeExclusiveOrValidate(filepath.Join(directory, key), data); err != nil {
			return "", fmt.Errorf("publish protected external object: %w", err)
		}
		return key, nil
	}
	file, err := os.CreateTemp(directory, "unsafe-"+digest+"-*.payload")
	if err != nil {
		return "", fmt.Errorf("create unsafe external object: %w", err)
	}
	key := filepath.Base(file.Name())
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return "", err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return "", err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return "", err
	}
	if err := file.Close(); err != nil {
		return "", err
	}
	if err := syncDirectory(directory); err != nil {
		return "", err
	}
	return key, nil
}

func validateExternalClaim(claim converter.StorageDriverClaim) (string, string, int, error) {
	if len(claim.ClaimData) != 3 {
		return "", "", 0, fmt.Errorf("%w: external claim has unexpected fields", ErrInvalidArtifact)
	}
	key := claim.ClaimData["key"]
	digest := claim.ClaimData["sha256"]
	size, err := strconv.Atoi(claim.ClaimData["size"])
	decoded, digestErr := hex.DecodeString(digest)
	if !safeComponent(key) || err != nil || size < 1 || size > maxExternalStoredBytes ||
		digestErr != nil || len(decoded) != 32 || hex.EncodeToString(decoded) != digest {
		return "", "", 0, fmt.Errorf("%w: malformed external claim", ErrInvalidArtifact)
	}
	return key, digest, size, nil
}

func readBoundedRegular(path string, limit int) ([]byte, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() || before.Size() < 1 || before.Size() > int64(limit) {
		return nil, fmt.Errorf("%w: object is not a bounded regular file", ErrInvalidArtifact)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(before, opened) {
		return nil, fmt.Errorf("%w: object changed before open", ErrInvalidArtifact)
	}
	data, err := io.ReadAll(io.LimitReader(file, int64(limit)+1))
	if err != nil {
		return nil, err
	}
	after, statErr := file.Stat()
	if len(data) > limit || statErr != nil || !os.SameFile(opened, after) || after.Size() != int64(len(data)) ||
		after.Size() != opened.Size() || !after.ModTime().Equal(opened.ModTime()) {
		return nil, fmt.Errorf("%w: object grew while reading", ErrInvalidArtifact)
	}
	return data, nil
}
