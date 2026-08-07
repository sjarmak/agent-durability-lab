package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/temporalio-labs/agent-durability-lab/experiments/activity-completion-identity/internal/lab"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "activity completion identity experiment: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var rawArm string
	var temporalPath string
	var outputRoot string
	var runID string
	var trials int
	flag.StringVar(&rawArm, "arm", "all", "stale-task-token, stale-by-id, fenced-by-id, or all")
	flag.StringVar(&temporalPath, "temporal", "", "Temporal CLI path; defaults to PATH lookup")
	flag.StringVar(
		&outputRoot,
		"output",
		filepath.FromSlash("experiments/activity-completion-identity/evidence"),
		"append-only evidence root",
	)
	flag.StringVar(&runID, "run-id", "completion-identity", "run ID prefix")
	flag.IntVar(&trials, "trials", 3, "independent trials per arm")
	flag.Parse()

	if trials <= 0 {
		return errors.New("trials must be positive")
	}
	if temporalPath == "" {
		resolved, err := exec.LookPath("temporal")
		if err != nil {
			return errors.New("temporal CLI not found; pass --temporal")
		}
		temporalPath = resolved
	}
	arms, err := parseArms(rawArm)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	for _, arm := range arms {
		for trial := 1; trial <= trials; trial++ {
			trialRunID := fmt.Sprintf("%s-%s-trial-%d", runID, arm, trial)
			result, err := lab.Run(ctx, lab.Options{
				Arm: arm, TemporalPath: temporalPath, OutputRoot: outputRoot, RunID: trialRunID,
			})
			if err != nil {
				return fmt.Errorf("run %s trial %d: %w", arm, trial, err)
			}
			if err := encoder.Encode(result); err != nil {
				return fmt.Errorf("print %s trial %d result: %w", arm, trial, err)
			}
		}
	}
	return nil
}

func parseArms(raw string) ([]lab.Arm, error) {
	normalized := lab.Arm(strings.ToLower(raw))
	if normalized == "all" {
		return []lab.Arm{lab.ArmStaleTaskToken, lab.ArmStaleByID, lab.ArmFencedByID}, nil
	}
	if !normalized.Valid() {
		return nil, fmt.Errorf("invalid arm %q", raw)
	}
	return []lab.Arm{normalized}, nil
}
