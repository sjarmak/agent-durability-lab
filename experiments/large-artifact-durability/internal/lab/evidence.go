package lab

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func preserveEvidence(runDirectory string, evidence Evidence, verdict Verdict, manifest Manifest) error {
	if err := writeJSONExclusive(filepath.Join(runDirectory, "evidence.json"), evidence); err != nil {
		return err
	}
	if err := writeJSONExclusive(filepath.Join(runDirectory, "verdict.json"), verdict); err != nil {
		return err
	}
	manifest.Files = make(map[string]string)
	files, directories, err := evidenceInventory(runDirectory)
	if err != nil {
		return err
	}
	manifest.Files = files
	manifest.Directories = directories
	return writeJSONExclusive(filepath.Join(runDirectory, "manifest.json"), manifest)
}

func writeJSONExclusive(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode %s: %w", path, err)
	}
	return writeFileAtomically(path, append(data, '\n'))
}

func evidenceInventory(root string) (map[string]string, []string, error) {
	digests := make(map[string]string)
	var directories []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if entry.IsDir() {
			directories = append(directories, relative)
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 || !entry.Type().IsRegular() {
			return fmt.Errorf("%w: evidence entry %q is not regular", ErrInvalidArtifact, path)
		}
		info, err := entry.Info()
		if err != nil || info.Size() < 0 || info.Size() > maxEvidenceFileBytes {
			return fmt.Errorf("%w: evidence entry %q exceeds the file bound", ErrInvalidArtifact, path)
		}
		if relative == "manifest.json" {
			return nil
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		hash := sha256.New()
		_, copyErr := io.Copy(hash, file)
		closeErr := file.Close()
		if copyErr != nil || closeErr != nil {
			return fmt.Errorf("hash evidence %q: %v %v", relative, copyErr, closeErr)
		}
		digests[relative] = hex.EncodeToString(hash.Sum(nil))
		return nil
	})
	return digests, directories, err
}
