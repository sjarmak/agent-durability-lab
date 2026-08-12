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

	"github.com/sjarmak/temporal_projects/experiments/durable-vendor-sessions/codex-direct/internal/lab"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	options, err := parseOptions(os.Args[1:])
	if err == nil {
		var result lab.ExperimentResult
		result, err = lab.RunExperiment(ctx, options)
		if err == nil {
			encoder := json.NewEncoder(os.Stdout)
			encoder.SetIndent("", "  ")
			err = encoder.Encode(result)
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func parseOptions(args []string) (lab.ExperimentOptions, error) {
	flags := flag.NewFlagSet("codex-direct-experiment", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var options lab.ExperimentOptions
	var mode string
	flags.StringVar(&options.EvidenceRoot, "evidence-root", "", "new append-only evidence root")
	flags.StringVar(&options.TemporalPath, "temporal-binary", "", "pinned Temporal CLI")
	flags.StringVar(&options.WorkerBinary, "worker-binary", "", "Codex Worker binary")
	flags.StringVar(&options.EffectBinary, "effect-binary", "", "controlled effect binary")
	flags.StringVar(&options.LauncherBinary, "launcher-binary", "", "pre-thread launcher binary")
	flags.StringVar(&options.CodexBinary, "codex-binary", "", "pinned underlying Codex CLI")
	flags.StringVar(&options.CodexWrapper, "codex-wrapper", "", "fixed authenticated codex-2 wrapper")
	flags.StringVar(&options.CodexHome, "codex-home", "", "fixed authenticated Codex profile")
	flags.StringVar(&options.OutputSchema, "output-schema", "", "structured result schema")
	flags.IntVar(&options.Trials, "trials", 3, "trials per exact boundary")
	flags.DurationVar(&options.Timeout, "timeout", 30*time.Minute, "whole suite timeout")
	flags.StringVar(&options.Model, "model", "gpt-5.6-sol", "pinned supported model")
	flags.StringVar(&options.ReasoningEffort, "reasoning-effort", "low", "pinned reasoning effort")
	flags.StringVar(&mode, "recovery-mode", string(lab.RecoveryModeUnsafeFresh), "recovery strategy")
	flags.BoolVar(&options.Hermetic, "hermetic", false, "enable hermetic Codex exact gate")
	if err := flags.Parse(args); err != nil {
		return options, err
	}
	recoveryMode, err := lab.ParseRecoveryMode(mode)
	if err != nil {
		return options, err
	}
	options.RecoveryMode = recoveryMode
	if flags.NArg() != 0 || options.EvidenceRoot == "" || options.TemporalPath == "" ||
		options.WorkerBinary == "" || options.EffectBinary == "" || options.LauncherBinary == "" ||
		options.CodexBinary == "" || options.CodexWrapper == "" || options.CodexHome == "" ||
		options.OutputSchema == "" {
		return options, errors.New("evidence and all pinned binary/profile/schema paths are required")
	}
	return options, nil
}
