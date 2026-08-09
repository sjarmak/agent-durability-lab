package lab

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
)

func prepareFixture(ctx context.Context, root string) error {
	if err := os.MkdirAll(root, 0o750); err != nil {
		return fmt.Errorf("create fixture repository: %w", err)
	}
	readme := []byte("# Claude direct durability fixture\n\nThe controlled effect appends to effects.jsonl.\n")
	if err := os.WriteFile(filepath.Join(root, "README.md"), readme, 0o600); err != nil {
		return fmt.Errorf("write fixture README: %w", err)
	}
	commands := [][]string{
		{"init", "--quiet"},
		{"config", "user.name", "Agent Durability Lab"},
		{"config", "user.email", "agent-durability@example.invalid"},
		{"add", "README.md"},
		{"commit", "--quiet", "-m", "fixture baseline"},
	}
	for _, args := range commands {
		command := exec.CommandContext(ctx, "git", append([]string{"-C", root}, args...)...)
		if output, err := command.CombinedOutput(); err != nil {
			return fmt.Errorf("prepare fixture git repository (%v): %w: %s", args, err, output)
		}
	}
	return nil
}

func hashWorkspace(root string) (string, error) {
	paths := make([]string, 0)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if relative == ".git" && entry.IsDir() {
			return filepath.SkipDir
		}
		if !entry.IsDir() && relative != "." {
			paths = append(paths, relative)
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("walk fixture workspace: %w", err)
	}
	sort.Strings(paths)
	hash := sha256.New()
	for _, relative := range paths {
		data, err := os.ReadFile(filepath.Join(root, relative))
		if err != nil {
			return "", fmt.Errorf("read fixture file %s: %w", relative, err)
		}
		_, _ = hash.Write([]byte(relative))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write(data)
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func workspaceStatus(ctx context.Context, root string) ([]byte, error) {
	command := exec.CommandContext(ctx, "git", "-C", root, "status", "--porcelain=v2", "--untracked-files=all")
	output, err := command.Output()
	if err != nil {
		return nil, fmt.Errorf("read fixture git status: %w", err)
	}
	if len(output) == 0 {
		return nil, errors.New("fixture workspace has no observed effect")
	}
	return output, nil
}
