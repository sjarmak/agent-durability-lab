package matrix

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/internal/sealedfs"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
)

type SourceArtifact struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}

type SourceSet struct {
	SHA256    string           `json:"sha256"`
	Artifacts []SourceArtifact `json:"artifacts"`
}

func collectSourceSet(root string, members []string) (SourceSet, error) {
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return SourceSet{}, err
	}
	resolvedRoot, err = filepath.Abs(resolvedRoot)
	if err != nil {
		return SourceSet{}, err
	}
	byPath := make(map[string]SourceArtifact)
	for _, member := range members {
		if member == "" || filepath.IsAbs(member) || member == ".." || strings.HasPrefix(filepath.Clean(member), ".."+string(filepath.Separator)) {
			return SourceSet{}, fmt.Errorf("%w: source member", protocol.ErrInvalidEvidence)
		}
		path := filepath.Join(resolvedRoot, filepath.Clean(member))
		if !containsPath(resolvedRoot, path) {
			return SourceSet{}, fmt.Errorf("%w: source member path", protocol.ErrInvalidEvidence)
		}
		if err := filepath.WalkDir(path, func(candidate string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.Type()&os.ModeSymlink != 0 {
				return fmt.Errorf("%w: symlink in source set", protocol.ErrInvalidEvidence)
			}
			if entry.IsDir() {
				return nil
			}
			if !entry.Type().IsRegular() {
				return fmt.Errorf("%w: non-regular source artifact", protocol.ErrInvalidEvidence)
			}
			if !sourceArtifactIncluded(candidate) {
				return nil
			}
			data, readErr := os.ReadFile(candidate)
			if readErr != nil || len(data) > 8<<20 {
				return fmt.Errorf("%w: source artifact read", protocol.ErrInvalidEvidence)
			}
			relative, relativeErr := filepath.Rel(resolvedRoot, candidate)
			if relativeErr != nil || filepath.IsAbs(relative) || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
				return fmt.Errorf("%w: source artifact path", protocol.ErrInvalidEvidence)
			}
			relative = filepath.ToSlash(relative)
			byPath[relative] = SourceArtifact{Path: relative, SHA256: sealedfs.HashBytes(data), Bytes: int64(len(data))}
			return nil
		}); err != nil {
			return SourceSet{}, err
		}
	}
	artifacts := make([]SourceArtifact, 0, len(byPath))
	for _, artifact := range byPath {
		artifacts = append(artifacts, artifact)
	}
	slices.SortFunc(artifacts, func(first, second SourceArtifact) int { return strings.Compare(first.Path, second.Path) })
	set := SourceSet{Artifacts: artifacts}
	set.SHA256, err = sourceSetDigest(artifacts)
	if err != nil {
		return SourceSet{}, err
	}
	if err := validateSourceSet(set); err != nil {
		return SourceSet{}, err
	}
	return set, nil
}

func sourceArtifactIncluded(path string) bool {
	if filepath.Ext(path) == ".go" {
		return true
	}
	switch filepath.Base(path) {
	case "go.mod", "go.sum", "go.work", "go.work.sum", "Makefile":
		return true
	default:
		return false
	}
}

func validateSourceSet(set SourceSet) error {
	if len(set.Artifacts) == 0 || !validDigest(set.SHA256) {
		return fmt.Errorf("%w: source set identity", protocol.ErrInvalidEvidence)
	}
	previous := ""
	for _, artifact := range set.Artifacts {
		if artifact.Path == "" || artifact.Path <= previous || filepath.IsAbs(artifact.Path) ||
			artifact.Path == ".." || strings.HasPrefix(artifact.Path, "../") || !validDigest(artifact.SHA256) || artifact.Bytes < 0 {
			return fmt.Errorf("%w: source artifact", protocol.ErrInvalidEvidence)
		}
		previous = artifact.Path
	}
	digest, err := sourceSetDigest(set.Artifacts)
	if err != nil || digest != set.SHA256 {
		return fmt.Errorf("%w: source set digest", protocol.ErrInvalidEvidence)
	}
	return nil
}

func sourceSetDigest(artifacts []SourceArtifact) (string, error) {
	data, err := json.Marshal(artifacts)
	if err != nil {
		return "", err
	}
	return sealedfs.HashBytes(data), nil
}
