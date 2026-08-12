package lab

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func writeJSONAtomicExclusive(path string, value any) (returnErr error) {
	directory := filepath.Dir(path)
	file, err := os.CreateTemp(directory, ".json-*.tmp")
	if err != nil {
		return fmt.Errorf("create atomic JSON staging file: %w", err)
	}
	temporary := file.Name()
	defer func() {
		if temporary != "" {
			returnErr = errors.Join(returnErr, os.Remove(temporary))
		}
	}()
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return err
	}
	encoder := json.NewEncoder(file)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		_ = file.Close()
		return fmt.Errorf("encode atomic JSON file: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync atomic JSON file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close atomic JSON file: %w", err)
	}
	if err := unix.Renameat2(unix.AT_FDCWD, temporary, unix.AT_FDCWD, path, unix.RENAME_NOREPLACE); err != nil {
		return fmt.Errorf("publish atomic JSON file: %w", err)
	}
	temporary = ""
	return syncDirectory(directory)
}
