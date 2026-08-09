package lab

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func writeJSONExclusive(path string, value any) error {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode %s: %w", filepath.Base(path), err)
	}
	return writeBytesExclusive(path, append(encoded, '\n'))
}

func writeBytesExclusive(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("publish %s: already exists", filepath.Base(path))
		}
		return fmt.Errorf("create %s: %w", filepath.Base(path), err)
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync %s: %w", filepath.Base(path), err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close %s: %w", filepath.Base(path), err)
	}
	return nil
}
