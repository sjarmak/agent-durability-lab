package lab

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
)

func CaptureSourcePins() (map[string]string, error) {
	repositoryRoot, err := sourceRepositoryRoot()
	if err != nil {
		return nil, err
	}
	paths := []string{"go.mod", "go.sum", runtimePreregistrationPath}
	experimentRoot := filepath.Join(repositoryRoot, "experiments", "large-artifact-durability")
	err = filepath.WalkDir(experimentRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == "evidence" {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: source pin traverses symlink %q", ErrInvalidArtifact, path)
		}
		if strings.HasSuffix(entry.Name(), ".go") {
			relative, err := filepath.Rel(repositoryRoot, path)
			if err != nil {
				return err
			}
			paths = append(paths, filepath.ToSlash(relative))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	pins := make(map[string]string, len(paths))
	for _, relative := range paths {
		data, err := readBoundedRegular(filepath.Join(repositoryRoot, filepath.FromSlash(relative)), maxEvidenceFileBytes)
		if err != nil {
			return nil, fmt.Errorf("hash source %q: %w", relative, err)
		}
		pins[relative] = digestBytes(data)
	}
	return pins, nil
}

func ValidateCurrentSourcePins(expected map[string]string) error {
	if len(expected) == 0 {
		return fmt.Errorf("%w: source pins are absent", ErrInvalidArtifact)
	}
	actual, err := CaptureSourcePins()
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(actual, expected) {
		return fmt.Errorf("%w: current source differs from evidence pins", ErrArtifactConflict)
	}
	return nil
}

func sourceRepositoryRoot() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("%w: locate source repository", ErrInvalidArtifact)
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", ".."))
	info, err := os.Lstat(filepath.Join(root, "go.mod"))
	if err != nil || !info.Mode().IsRegular() {
		return "", fmt.Errorf("%w: source repository lacks go.mod", ErrInvalidArtifact)
	}
	return root, nil
}
