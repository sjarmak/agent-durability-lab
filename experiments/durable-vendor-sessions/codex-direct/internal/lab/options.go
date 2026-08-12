package lab

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type ExperimentOptions struct {
	EvidenceRoot, TemporalPath, WorkerBinary, EffectBinary, LauncherBinary string
	CodexBinary, CodexWrapper, CodexHome, OutputSchema                     string
	Trials                                                                 int
	Timeout                                                                time.Duration
	Model, ReasoningEffort                                                 string
	RecoveryMode                                                           RecoveryMode
	Hermetic                                                               bool
}

type ExperimentResult struct {
	EvidenceRoot   string   `json:"evidence_root"`
	RunDirectories []string `json:"run_directories"`
}

func normalizeExperimentOptions(options ExperimentOptions) (ExperimentOptions, error) {
	paths := []*string{
		&options.EvidenceRoot, &options.TemporalPath, &options.WorkerBinary, &options.EffectBinary,
		&options.LauncherBinary, &options.CodexBinary, &options.CodexWrapper, &options.CodexHome,
		&options.OutputSchema,
	}
	for _, path := range paths {
		if *path == "" {
			continue
		}
		absolute, err := filepath.Abs(*path)
		if err != nil {
			return ExperimentOptions{}, err
		}
		*path = filepath.Clean(absolute)
	}
	return options, nil
}

func validateExperimentOptions(options ExperimentOptions) error {
	if options.EvidenceRoot == "" || options.TemporalPath == "" || options.WorkerBinary == "" ||
		options.EffectBinary == "" || options.LauncherBinary == "" || options.CodexBinary == "" ||
		options.CodexWrapper == "" || options.CodexHome == "" || options.OutputSchema == "" ||
		options.Trials < 1 || options.Timeout <= 0 || options.Model == "" || options.ReasoningEffort == "" ||
		!options.RecoveryMode.valid() {
		return errors.New("experiment requires new evidence root and pinned Temporal, Codex, model, and harness inputs")
	}
	if _, err := os.Stat(options.EvidenceRoot); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return errors.New("evidence root already exists")
		}
		return err
	}
	for _, path := range []string{
		options.TemporalPath, options.WorkerBinary, options.EffectBinary,
		options.LauncherBinary, options.CodexBinary, options.CodexWrapper,
	} {
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
			return fmt.Errorf("path %q is not an executable regular file: %w", path, err)
		}
	}
	if info, err := os.Stat(options.CodexHome); err != nil || !info.IsDir() {
		return fmt.Errorf("codex home is not a directory: %w", err)
	}
	if info, err := os.Stat(options.OutputSchema); err != nil || !info.Mode().IsRegular() {
		return fmt.Errorf("output schema is not a regular file: %w", err)
	}
	return nil
}
