package transport

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
)

const (
	maxJSONBytes     = 64 << 20
	maxArtifactBytes = 1 << 30
	maxArchiveBytes  = 4 << 30
)

func readJSON[T any](path string) (value T, returnErr error) {
	return readJSONWithPolicy[T](path, true)
}

func readJSONProjection[T any](path string) (value T, returnErr error) {
	return readJSONWithPolicy[T](path, false)
}

func readJSONWithPolicy[T any](path string, rejectUnknown bool) (value T, returnErr error) {
	info, err := os.Lstat(path)
	if err != nil {
		return value, err
	}
	if !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > maxJSONBytes {
		return value, fmt.Errorf("%w: JSON artifact %q is not a bounded regular file", ErrInvalidTransport, path)
	}
	file, err := os.Open(path)
	if err != nil {
		return value, err
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	decoder := json.NewDecoder(io.LimitReader(file, maxJSONBytes+1))
	if rejectUnknown {
		decoder.DisallowUnknownFields()
	}
	if err := decoder.Decode(&value); err != nil {
		return value, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return value, fmt.Errorf("%w: trailing JSON data in %q", ErrInvalidTransport, path)
	}
	return value, nil
}

func writeJSONExclusive(path string, value any) (returnErr error) {
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

func copyExclusive(source, destination string) (returnErr error) {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, input.Close()) }()
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, output.Close()) }()
	if _, err := io.Copy(output, io.LimitReader(input, maxJSONBytes+1)); err != nil {
		return err
	}
	return output.Sync()
}

func hashFile(path string, maximum int64) (string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > maximum {
		return "", fmt.Errorf("%w: artifact %q is not a bounded regular file", ErrInvalidTransport, path)
	}
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	written, err := io.Copy(hash, io.LimitReader(file, info.Size()+1))
	if err != nil || written != info.Size() {
		return "", fmt.Errorf("hash artifact %q: %w", path, err)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func safeBaseName(name string) bool {
	if name == "" || name == "." || name == ".." || filepath.Base(name) != name || strings.ContainsAny(name, `/\\`) {
		return false
	}
	for _, character := range name {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || strings.ContainsRune("._-", character) {
			continue
		}
		return false
	}
	return true
}

func safeRelativePath(name string) bool {
	if name == "" || strings.ContainsAny(name, "\\\x00") || strings.HasPrefix(name, "/") || path.Clean(name) != name {
		return false
	}
	parts := strings.Split(name, "/")
	if len(parts) == 0 || parts[0] == "." || parts[0] == ".." ||
		len(parts[0]) == 2 && parts[0][1] == ':' && isASCIILetter(parts[0][0]) {
		return false
	}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return false
		}
		for _, character := range part {
			if character < 0x20 || character == 0x7f {
				return false
			}
		}
	}
	return true
}

func isASCIILetter(character byte) bool {
	return character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z'
}

func validSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}
