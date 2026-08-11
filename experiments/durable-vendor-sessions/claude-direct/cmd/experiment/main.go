package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sjarmak/temporal_projects/experiments/durable-vendor-sessions/claude-direct/internal/lab"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	options, err := parseOptions(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	result, err := lab.RunExperiment(ctx, options)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func parseOptions(args []string) (lab.ExperimentOptions, error) {
	flags := flag.NewFlagSet("claude-direct-experiment", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var options lab.ExperimentOptions
	var recoveryMode string
	flags.StringVar(&options.EvidenceRoot, "evidence-root", "", "new append-only evidence directory")
	flags.StringVar(&options.TemporalPath, "temporal-binary", "", "pinned Temporal CLI binary")
	flags.StringVar(&options.WorkerBinary, "worker-binary", "", "claude-direct Worker binary")
	flags.StringVar(&options.EffectBinary, "effect-binary", "", "controlled effect binary")
	flags.StringVar(&options.LauncherBinary, "launcher-binary", "", "pre-registration Claude launcher binary")
	flags.StringVar(&options.ClaudeBinary, "claude-binary", "", "pinned Claude CLI binary")
	flags.IntVar(&options.Trials, "trials", 3, "independent trials per probe")
	flags.DurationVar(&options.Timeout, "timeout", 20*time.Minute, "whole-suite timeout")
	flags.StringVar(&options.Model, "model", "haiku", "pinned model alias or ID")
	flags.StringVar(&options.MaxBudgetUSD, "max-budget-usd", "0.25", "per-attempt spend ceiling")
	flags.IntVar(&options.MaxTurns, "max-turns", 2, "per-attempt agentic turn ceiling")
	flags.StringVar(&recoveryMode, "recovery-mode", string(lab.RecoveryModeUnsafeFresh),
		"Claude delivery strategy: unsafe-fresh, resume-only, or fenced-start-or-attach")
	if err := flags.Parse(args); err != nil {
		return lab.ExperimentOptions{}, fmt.Errorf("parse experiment flags: %w", err)
	}
	mode, err := lab.ParseRecoveryMode(recoveryMode)
	if err != nil {
		return lab.ExperimentOptions{}, err
	}
	options.RecoveryMode = mode
	if flags.NArg() != 0 || options.EvidenceRoot == "" || options.TemporalPath == "" ||
		options.WorkerBinary == "" || options.EffectBinary == "" || options.LauncherBinary == "" ||
		options.ClaudeBinary == "" {
		return lab.ExperimentOptions{}, errors.New("evidence root and all binary paths are required; positional arguments are not accepted")
	}
	return options, nil
}
