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
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/temporalio-labs/agent-durability-lab/experiments/worker-death/internal/lab"
	"github.com/temporalio-labs/agent-durability-lab/internal/workstore"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "worker-death experiment: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var scenario string
	var rawMode string
	var rawArm string
	var temporalPath string
	var workerBinary string
	var agentBinary string
	var outputRoot string
	var runID string
	var trials int
	flag.StringVar(&scenario, "scenario", "surviving-agent", "surviving-agent, launch-gap, or post-exec-gap")
	flag.StringVar(&rawMode, "mode", "all", "surviving-agent arm: unsafe, reattach, fenced, or all")
	flag.StringVar(&rawArm, "arm", "all", "scenario-specific arm or all")
	flag.StringVar(&temporalPath, "temporal", "", "Temporal CLI path; defaults to PATH lookup")
	flag.StringVar(&workerBinary, "worker", filepath.FromSlash("bin/lab-worker"), "Worker binary path")
	flag.StringVar(&agentBinary, "agent", filepath.FromSlash("bin/agent-simulator"), "agent simulator binary path")
	flag.StringVar(&outputRoot, "output", filepath.FromSlash("experiments/worker-death/evidence"), "append-only evidence root")
	flag.StringVar(&runID, "run-id", time.Now().UTC().Format("20060102T150405Z"), "run ID or prefix")
	flag.IntVar(&trials, "trials", 1, "fresh trials per selected arm")
	flag.Parse()
	if trials < 1 {
		return errors.New("trials must be positive")
	}

	if temporalPath == "" {
		resolved, err := exec.LookPath("temporal")
		if err != nil {
			return errors.New("temporal CLI not found; pass --temporal")
		}
		temporalPath = resolved
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	options := commandOptions{
		temporalPath: temporalPath, workerBinary: workerBinary, agentBinary: agentBinary,
		outputRoot: outputRoot, runID: runID, trials: trials,
	}
	switch strings.ToLower(scenario) {
	case "surviving-agent":
		return runSurvivingAgent(ctx, encoder, options, rawMode)
	case "launch-gap":
		return runLaunchGap(ctx, encoder, options, rawArm)
	case "post-exec-gap":
		return runPostExecGap(ctx, encoder, options, rawArm)
	default:
		return fmt.Errorf("invalid scenario %q", scenario)
	}
}

func runPostExecGap(ctx context.Context, encoder *json.Encoder, options commandOptions, rawArm string) error {
	arms, err := parsePostExecGapArms(rawArm)
	if err != nil {
		return err
	}
	for _, arm := range arms {
		for trial := 1; trial <= options.trials; trial++ {
			armRunID := variantRunID(options.runID, string(arm), len(arms), trial, options.trials)
			result, err := lab.RunPostExecGap(ctx, lab.PostExecGapOptions{
				Arm: arm,
				Options: lab.Options{
					Mode: workstore.ModeFenced, TemporalPath: options.temporalPath, WorkerBinary: options.workerBinary,
					AgentBinary: options.agentBinary, OutputRoot: options.outputRoot, RunID: armRunID,
				},
			})
			if err != nil {
				return fmt.Errorf("run post-exec gap %s arm trial %d: %w", arm, trial, err)
			}
			if err := encoder.Encode(result); err != nil {
				return fmt.Errorf("print post-exec gap %s trial %d result: %w", arm, trial, err)
			}
		}
	}
	return nil
}

type commandOptions struct {
	temporalPath string
	workerBinary string
	agentBinary  string
	outputRoot   string
	runID        string
	trials       int
}

func runSurvivingAgent(ctx context.Context, encoder *json.Encoder, options commandOptions, rawMode string) error {
	modes, err := parseModes(rawMode)
	if err != nil {
		return err
	}
	for _, mode := range modes {
		for trial := 1; trial <= options.trials; trial++ {
			modeRunID := variantRunID(options.runID, string(mode), len(modes), trial, options.trials)
			result, err := lab.Run(ctx, lab.Options{
				Mode: mode, TemporalPath: options.temporalPath, WorkerBinary: options.workerBinary,
				AgentBinary: options.agentBinary, OutputRoot: options.outputRoot, RunID: modeRunID,
			})
			if err != nil {
				return fmt.Errorf("run %s arm trial %d: %w", mode, trial, err)
			}
			if err := encoder.Encode(result); err != nil {
				return fmt.Errorf("print %s trial %d result: %w", mode, trial, err)
			}
		}
	}
	return nil
}

func runLaunchGap(ctx context.Context, encoder *json.Encoder, options commandOptions, rawArm string) error {
	arms, err := parseLaunchGapArms(rawArm)
	if err != nil {
		return err
	}
	for _, arm := range arms {
		for trial := 1; trial <= options.trials; trial++ {
			armRunID := variantRunID(options.runID, string(arm), len(arms), trial, options.trials)
			result, err := lab.RunLaunchGap(ctx, lab.LaunchGapOptions{
				Arm: arm,
				Options: lab.Options{
					Mode: workstore.ModeFenced, TemporalPath: options.temporalPath, WorkerBinary: options.workerBinary,
					AgentBinary: options.agentBinary, OutputRoot: options.outputRoot, RunID: armRunID,
				},
			})
			if err != nil {
				return fmt.Errorf("run launch-gap %s arm trial %d: %w", arm, trial, err)
			}
			if err := encoder.Encode(result); err != nil {
				return fmt.Errorf("print launch-gap %s trial %d result: %w", arm, trial, err)
			}
		}
	}
	return nil
}

func parseModes(raw string) ([]workstore.Mode, error) {
	normalized := strings.ToLower(raw)
	if normalized == "all" {
		return []workstore.Mode{workstore.ModeUnsafe, workstore.ModeReattach, workstore.ModeFenced}, nil
	}
	mode := workstore.Mode(normalized)
	if !mode.Valid() {
		return nil, fmt.Errorf("invalid mode %q", raw)
	}
	return []workstore.Mode{mode}, nil
}

func parseLaunchGapArms(raw string) ([]lab.LaunchGapArm, error) {
	normalized := strings.ToLower(raw)
	if normalized == "all" {
		return []lab.LaunchGapArm{lab.LaunchGapControl, lab.LaunchGapFencedRecovery}, nil
	}
	arm := lab.LaunchGapArm(normalized)
	if !arm.Valid() {
		return nil, fmt.Errorf("invalid launch-gap arm %q", raw)
	}
	return []lab.LaunchGapArm{arm}, nil
}

func parsePostExecGapArms(raw string) ([]lab.PostExecGapArm, error) {
	normalized := strings.ToLower(raw)
	if normalized == "all" {
		return []lab.PostExecGapArm{lab.PostExecAttachControl, lab.PostExecFencedReplacement}, nil
	}
	arm := lab.PostExecGapArm(normalized)
	if !arm.Valid() {
		return nil, fmt.Errorf("invalid post-exec gap arm %q", raw)
	}
	return []lab.PostExecGapArm{arm}, nil
}

func variantRunID(base, variant string, variantCount, trial, trials int) string {
	runID := base
	if variantCount > 1 {
		runID += "-" + variant
	}
	if trials > 1 {
		runID += "-trial-" + strconv.Itoa(trial)
	}
	return runID
}
