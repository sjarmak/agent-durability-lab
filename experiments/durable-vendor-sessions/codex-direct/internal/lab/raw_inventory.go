package lab

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
	"sort"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

const (
	rawInventoryFile    = "raw-inventory.json"
	rawInventoryVersion = "codex-direct-raw-v1"
)

type rawArtifact struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type rawInventory struct {
	Version string        `json:"version"`
	Files   []rawArtifact `json:"files"`
}

func writeRawInventory(root string) (string, error) {
	files, err := listRawArtifacts(root)
	if err != nil {
		return "", err
	}
	path := filepath.Join(root, rawInventoryFile)
	if err := writeJSONExclusive(path, rawInventory{Version: rawInventoryVersion, Files: files}); err != nil {
		return "", err
	}
	hash, err := protocol.FileSHA256(path)
	if err != nil {
		return "", fmt.Errorf("hash raw evidence inventory: %w", err)
	}
	return hash, nil
}

func verifyRawInventory(root, wantHash string) (rawInventory, error) {
	path := filepath.Join(root, rawInventoryFile)
	actualHash, err := protocol.FileSHA256(path)
	if err != nil {
		return rawInventory{}, fmt.Errorf("hash preserved raw inventory: %w", err)
	}
	if actualHash != wantHash {
		return rawInventory{}, fmt.Errorf("raw inventory hash = %s, want %s", actualHash, wantHash)
	}
	inventory, err := readStrictJSON[rawInventory](path)
	if err != nil {
		return rawInventory{}, err
	}
	actualFiles, err := listRawArtifacts(root)
	if err != nil {
		return rawInventory{}, err
	}
	if inventory.Version != rawInventoryVersion || !slices.Equal(inventory.Files, actualFiles) {
		return rawInventory{}, fmt.Errorf("raw evidence inventory does not match preserved artifacts")
	}
	return inventory, nil
}

func listRawArtifacts(root string) ([]rawArtifact, error) {
	var artifacts []rawArtifact
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if filepath.ToSlash(relative) == rawInventoryFile {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("inspect raw artifact %q: %w", relative, err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("raw artifact %q is not a regular file", relative)
		}
		hash, err := protocol.FileSHA256(path)
		if err != nil {
			return err
		}
		artifacts = append(artifacts, rawArtifact{
			Path: filepath.ToSlash(relative), Size: info.Size(), SHA256: hash,
		})
		return nil
	})
	sort.Slice(artifacts, func(left, right int) bool { return artifacts[left].Path < artifacts[right].Path })
	return artifacts, err
}
