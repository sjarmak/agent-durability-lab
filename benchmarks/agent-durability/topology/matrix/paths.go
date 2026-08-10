package matrix

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

func ValidateDisjointPaths(first, second string) error {
	firstResolved, err := resolveProspectivePath(first)
	if err != nil {
		return err
	}
	secondResolved, err := resolveProspectivePath(second)
	if err != nil {
		return err
	}
	if containsPath(firstResolved, secondResolved) || containsPath(secondResolved, firstResolved) {
		return errors.New("evidence-root and work-root must be disjoint")
	}
	return nil
}

func resolveProspectivePath(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	absolute = filepath.Clean(absolute)
	cursor := absolute
	missing := make([]string, 0)
	for {
		if _, err := os.Lstat(cursor); err == nil {
			resolved, err := filepath.EvalSymlinks(cursor)
			if err != nil {
				return "", err
			}
			for index := len(missing) - 1; index >= 0; index-- {
				resolved = filepath.Join(resolved, missing[index])
			}
			return filepath.Clean(resolved), nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(cursor)
		if parent == cursor {
			return "", os.ErrNotExist
		}
		missing = append(missing, filepath.Base(cursor))
		cursor = parent
	}
}

func containsPath(root, candidate string) bool {
	relative, err := filepath.Rel(root, candidate)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}
