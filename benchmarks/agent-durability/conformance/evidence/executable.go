package evidence

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	legacyprotocol "github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

const maximumExecutableBytes = 128 << 20

func PreserveExecutable(ctx context.Context, root, sourcePath string, pin Pin) (string, error) {
	if root == "" || sourcePath == "" || pin.Path != ExecutableArtifactPath {
		return "", fmt.Errorf("%w: executable source and fixed artifact pin are required", legacyprotocol.ErrInvalidEvidence)
	}
	data, err := readRegularFile(sourcePath)
	if err != nil {
		return "", fmt.Errorf("read executable: %w", err)
	}
	if digestBytes(data) != pin.SHA256 {
		return "", fmt.Errorf("%w: executable differs from its pin", legacyprotocol.ErrInvalidEvidence)
	}
	for _, path := range []string{root, filepath.Join(root, "inputs"), filepath.Join(root, "inputs", "executable")} {
		if err := ensureDirectory(path); err != nil {
			return "", err
		}
	}
	destination := filepath.Join(root, filepath.FromSlash(ExecutableArtifactPath))
	if err := writeExclusiveFile(ctx, destination, data); err != nil {
		return destination, err
	}
	if err := syncDirectory(filepath.Dir(destination)); err != nil {
		return destination, err
	}
	return destination, nil
}

func readRegularFile(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%w: %s is not a regular non-symlink file", legacyprotocol.ErrInvalidEvidence, path)
	}
	if info.Size() > maximumExecutableBytes {
		return nil, fmt.Errorf("%w: executable exceeds %d bytes", legacyprotocol.ErrInvalidEvidence, maximumExecutableBytes)
	}
	return os.ReadFile(path)
}

func VerifyPreservedExecutable(root string, pin Pin) error {
	if root == "" || pin.Path != ExecutableArtifactPath {
		return fmt.Errorf("%w: fixed executable artifact pin is required", legacyprotocol.ErrInvalidEvidence)
	}
	path := filepath.Join(root, filepath.FromSlash(pin.Path))
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect preserved executable: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: preserved executable is not a regular file", legacyprotocol.ErrInvalidEvidence)
	}
	confined, err := confinedPath(root, filepath.FromSlash(pin.Path))
	if err != nil {
		return err
	}
	digest, err := fileSHA256(confined)
	if err != nil {
		return err
	}
	if digest != pin.SHA256 {
		return fmt.Errorf("%w: preserved executable differs from its pin", legacyprotocol.ErrInvalidEvidence)
	}
	return nil
}

func ensureDirectory(path string) error {
	if err := os.Mkdir(path, 0o750); err != nil && !os.IsExist(err) {
		return fmt.Errorf("create directory %s: %w", path, err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect directory %s: %w", path, err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: %s is not a real directory", legacyprotocol.ErrInvalidEvidence, path)
	}
	return nil
}
