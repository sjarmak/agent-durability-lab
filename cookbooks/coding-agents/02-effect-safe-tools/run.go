package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

type commandRunner func(context.Context, string, string, ...string) error

type runOptions struct {
	RepositoryRoot string
	Destination    Destination
	OutputRoot     string
	RunID          string
	Trials         int
	TemporalPath   string
}

func runRecipe(ctx context.Context, runner commandRunner, options runOptions) error {
	if _, found := recipeFor(options.Destination); !found {
		return fmt.Errorf("unknown destination %q", options.Destination)
	}
	if strings.TrimSpace(options.RunID) == "" {
		return errors.New("run ID is required")
	}
	if options.Trials < 1 {
		return errors.New("trials must be positive")
	}
	if strings.TrimSpace(options.OutputRoot) == "" {
		return errors.New("output root is required")
	}
	if err := rejectPreservedEvidenceTarget(options.RepositoryRoot, options.OutputRoot); err != nil {
		return err
	}
	temporaryDirectory, err := os.MkdirTemp("", "effect-safe-tools-")
	if err != nil {
		return fmt.Errorf("create temporary build directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(temporaryDirectory) }()
	workerPath := filepath.Join(temporaryDirectory, "external-effect-worker")
	if err := runner(ctx, options.RepositoryRoot, "go", "build", "-o", workerPath, "./experiments/external-effects/cmd/worker"); err != nil {
		return fmt.Errorf("build external-effect Worker: %w", err)
	}
	arguments := []string{
		"run", "./experiments/external-effects/cmd/experiment",
		"--destination", string(options.Destination),
		"--mode", "all",
		"--trials", strconv.Itoa(options.Trials),
		"--run-id", options.RunID,
		"--output", options.OutputRoot,
		"--worker", workerPath,
	}
	if options.TemporalPath != "" {
		arguments = append(arguments, "--temporal", options.TemporalPath)
	}
	if err := runner(ctx, options.RepositoryRoot, "go", arguments...); err != nil {
		return fmt.Errorf("run external-effect experiment: %w", err)
	}
	return nil
}

func rejectPreservedEvidenceTarget(repositoryRoot, outputRoot string) error {
	preserved, err := canonicalPath(filepath.Join(repositoryRoot, "experiments", "external-effects", "evidence"))
	if err != nil {
		return fmt.Errorf("resolve preserved evidence root: %w", err)
	}
	output, err := canonicalPath(outputRoot)
	if err != nil {
		return fmt.Errorf("resolve output root: %w", err)
	}
	relative, err := filepath.Rel(preserved, output)
	if err != nil {
		return fmt.Errorf("compare evidence roots: %w", err)
	}
	if relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))) {
		return errors.New("refusing to write into the preserved final-evidence tree; choose a separate output root")
	}
	return nil
}

func canonicalPath(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	existing := absolute
	var missing []string
	for {
		if _, err := os.Lstat(existing); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(existing)
		if parent == existing {
			return "", fmt.Errorf("no existing ancestor for %s", path)
		}
		missing = append(missing, filepath.Base(existing))
		existing = parent
	}
	resolved, err := filepath.EvalSymlinks(existing)
	if err != nil {
		return "", err
	}
	for index := len(missing) - 1; index >= 0; index-- {
		resolved = filepath.Join(resolved, missing[index])
	}
	return filepath.Clean(resolved), nil
}

func executeCommand(ctx context.Context, directory, name string, arguments ...string) error {
	command := exec.CommandContext(ctx, name, arguments...)
	command.Dir = directory
	command.Stdin = os.Stdin
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	return command.Run()
}
