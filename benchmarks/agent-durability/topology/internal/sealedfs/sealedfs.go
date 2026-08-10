// Package sealedfs implements the shared append-only filesystem boundary for
// run- and pair-level topology evidence.
package sealedfs

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
)

func ConfinedDirectory(root, directory string) (string, error) {
	if root == "" || directory == "" {
		return "", fmt.Errorf("%w: artifact root and directory", protocol.ErrInvalidEvidence)
	}
	rootAbsolute, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	resolvedRoot, err := filepath.EvalSymlinks(rootAbsolute)
	if err != nil {
		return "", err
	}
	candidate := directory
	if !filepath.IsAbs(candidate) {
		candidate = filepath.Join(resolvedRoot, candidate)
	}
	resolved, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return "", fmt.Errorf("%w: resolve artifact directory: %v", protocol.ErrInvalidEvidence, err)
	}
	relative, err := filepath.Rel(resolvedRoot, resolved)
	if err != nil || relative == "." || filepath.IsAbs(relative) || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: artifact directory outside root", protocol.ErrInvalidEvidence)
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() {
		return "", fmt.Errorf("%w: artifact directory", protocol.ErrInvalidEvidence)
	}
	return resolved, nil
}

func WriteJSONExclusive(path string, value any) (returnErr error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
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

func WriteJSONLinesExclusive[T any](path string, values []T) (returnErr error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	buffer := bufio.NewWriter(file)
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	for _, value := range values {
		if err := encoder.Encode(value); err != nil {
			return err
		}
	}
	if err := buffer.Flush(); err != nil {
		return err
	}
	return file.Sync()
}

func DecodeJSON(name string, data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("%w: decode %s: %v", protocol.ErrInvalidEvidence, name, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("%w: trailing JSON in %s", protocol.ErrInvalidEvidence, name)
	}
	return nil
}

func DecodeJSONLines[T any](name string, data []byte, target *[]T) error {
	decoder := json.NewDecoder(bufio.NewReader(bytes.NewReader(data)))
	decoder.DisallowUnknownFields()
	for {
		var value T
		if err := decoder.Decode(&value); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return fmt.Errorf("%w: decode %s: %v", protocol.ErrInvalidEvidence, name, err)
		}
		*target = append(*target, value)
	}
	return nil
}

func ReadRegularFileOnce(root *os.Root, name string, limit int64) (data []byte, returnErr error) {
	before, err := root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() || before.Size() < 0 || before.Size() > limit {
		return nil, fmt.Errorf("not a bounded regular file")
	}
	file, err := root.Open(name)
	if err != nil {
		return nil, err
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(before, opened) {
		return nil, fmt.Errorf("artifact changed before open")
	}
	data, err = io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("artifact exceeds size limit")
	}
	after, err := file.Stat()
	if err != nil || !os.SameFile(opened, after) || after.Size() != int64(len(data)) ||
		after.Size() != opened.Size() || !after.ModTime().Equal(opened.ModTime()) {
		return nil, fmt.Errorf("artifact changed while reading")
	}
	return data, nil
}

func ValidateArtifactSet(root *os.Root, required []string) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	entries, readErr := directory.ReadDir(-1)
	closeErr := directory.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return err
	}
	want := make(map[string]bool, len(required))
	for _, name := range required {
		want[name] = true
	}
	if len(entries) != len(want) {
		return fmt.Errorf("%w: artifact set", protocol.ErrInvalidEvidence)
	}
	for _, entry := range entries {
		if !want[entry.Name()] {
			return fmt.Errorf("%w: unexpected artifact", protocol.ErrInvalidEvidence)
		}
	}
	return nil
}

func HashBytes(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func HashRegularFile(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("not a regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func SyncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
