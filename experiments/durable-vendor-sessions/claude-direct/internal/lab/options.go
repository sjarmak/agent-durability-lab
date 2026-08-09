package lab

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"
)

type ExperimentOptions struct {
	EvidenceRoot   string
	TemporalPath   string
	WorkerBinary   string
	EffectBinary   string
	LauncherBinary string
	ClaudeBinary   string
	Trials         int
	Timeout        time.Duration
	Model          string
	MaxBudgetUSD   string
	MaxTurns       int
}

type ExperimentResult struct {
	EvidenceRoot   string   `json:"evidence_root"`
	RunDirectories []string `json:"run_directories"`
}

func validateExperimentOptions(options ExperimentOptions) error {
	if options.EvidenceRoot == "" || options.TemporalPath == "" || options.WorkerBinary == "" ||
		options.EffectBinary == "" || options.LauncherBinary == "" || options.ClaudeBinary == "" || options.Trials < 1 ||
		options.Timeout <= 0 || options.Model == "" || options.MaxTurns < 1 {
		return errors.New("experiment requires evidence root, pinned binaries, trials, timeout, model, budget, and turn limit")
	}
	budget, err := strconv.ParseFloat(options.MaxBudgetUSD, 64)
	if err != nil || budget <= 0 || math.IsInf(budget, 0) || math.IsNaN(budget) {
		return errors.New("experiment budget must be finite and positive")
	}
	if _, err := os.Stat(options.EvidenceRoot); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return errors.New("evidence root already exists")
		}
		return fmt.Errorf("inspect evidence root: %w", err)
	}
	for _, path := range []string{
		options.TemporalPath, options.WorkerBinary, options.EffectBinary, options.LauncherBinary, options.ClaudeBinary,
	} {
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("inspect executable %q: %w", path, err)
		}
		if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
			return fmt.Errorf("path %q is not an executable regular file", path)
		}
	}
	return nil
}
