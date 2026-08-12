package lab

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
)

func prepareFixture(ctx context.Context, root string) error {
	if err := os.MkdirAll(root, 0o750); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(root, "README.md"),
		[]byte("# Codex durability fixture\n\nThe controlled effect appends to effects.jsonl.\n"), 0o600); err != nil {
		return err
	}
	commands := [][]string{
		{"init", "--quiet"}, {"config", "user.name", "Agent Durability Lab"},
		{"config", "user.email", "agent-durability@example.invalid"}, {"add", "README.md"},
		{"commit", "--quiet", "-m", "fixture baseline"},
	}
	for _, arguments := range commands {
		command := exec.CommandContext(ctx, "git", append([]string{"-C", root}, arguments...)...)
		if output, err := command.CombinedOutput(); err != nil {
			return fmt.Errorf("prepare fixture (%v): %w: %s", arguments, err, output)
		}
	}
	return nil
}

func hashWorkspace(root string) (string, error) {
	var paths []string
	if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
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
	}); err != nil {
		return "", err
	}
	sort.Strings(paths)
	hash := sha256.New()
	for _, relative := range paths {
		data, err := os.ReadFile(filepath.Join(root, relative))
		if err != nil {
			return "", err
		}
		_, _ = hash.Write([]byte(relative))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write(data)
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
