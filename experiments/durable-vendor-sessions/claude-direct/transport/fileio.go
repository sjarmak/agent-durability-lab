package transport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

const (
	maxJSONBytes     = int64(64 << 20)
	maxArchiveBytes  = int64(1 << 30)
	maxArtifactBytes = int64(512 << 20)
	maxBundleBytes   = int64(8 << 30)
	maxBundleFiles   = 100_000
	maxBundles       = 1_000
)

func readJSON[T any](path string) (T, error) {
	data, err := readRegularFile(path, maxJSONBytes)
	if err != nil {
		var value T
		return value, err
	}
	return decodeJSON[T](filepath.Base(path), data)
}

func decodeJSON[T any](name string, data []byte) (T, error) {
	var value T
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return value, fmt.Errorf("%w: decode %s: %v", ErrInvalidTransport, name, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return value, fmt.Errorf("%w: trailing JSON in %s", ErrInvalidTransport, name)
	}
	return value, nil
}

func writeJSONExclusive(path string, value any) (returnErr error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	encoder := json.NewEncoder(file)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		return err
	}
	return file.Sync()
}

func readRegularFile(path string, limit int64) (data []byte, returnErr error) {
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() || before.Size() < 0 || before.Size() > limit {
		return nil, fmt.Errorf("%w: %s is not a bounded regular file", ErrInvalidTransport, path)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(before, opened) {
		return nil, fmt.Errorf("%w: %s changed before open", ErrInvalidTransport, path)
	}
	data, err = io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	after, err := file.Stat()
	if err != nil || int64(len(data)) > limit || !os.SameFile(opened, after) ||
		after.Size() != int64(len(data)) || after.Size() != opened.Size() ||
		!after.ModTime().Equal(opened.ModTime()) {
		return nil, fmt.Errorf("%w: %s changed while reading", ErrInvalidTransport, path)
	}
	return data, nil
}

func hashRegularFile(path string, limit int64) (digest string, returnErr error) {
	before, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !before.Mode().IsRegular() || before.Size() < 0 || before.Size() > limit {
		return "", fmt.Errorf("%w: %s is not a bounded regular file", ErrInvalidTransport, path)
	}
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(before, opened) {
		return "", fmt.Errorf("%w: %s changed before hash", ErrInvalidTransport, path)
	}
	hasher := sha256.New()
	written, err := io.Copy(hasher, io.LimitReader(file, limit+1))
	if err != nil {
		return "", err
	}
	after, err := file.Stat()
	if err != nil || written > limit || !os.SameFile(opened, after) || written != after.Size() ||
		after.Size() != opened.Size() || !after.ModTime().Equal(opened.ModTime()) {
		return "", fmt.Errorf("%w: %s changed while hashing", ErrInvalidTransport, path)
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func hashBytes(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func checkContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func ensureDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&fs.ModeSymlink != 0 {
		return fmt.Errorf("%w: %s is not a directory", ErrInvalidTransport, path)
	}
	return nil
}
