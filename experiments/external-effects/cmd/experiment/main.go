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

	"github.com/sjarmak/temporal_projects/experiments/external-effects/internal/lab"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "external-effect experiment: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var rawDestination string
	var rawMode string
	var temporalPath string
	var workerBinary string
	var outputRoot string
	var runPrefix string
	var trials int
	flag.StringVar(&rawDestination, "destination", "all", "destination or all")
	flag.StringVar(&rawMode, "mode", "all", "unsafe, protected, or all")
	flag.StringVar(&temporalPath, "temporal", "", "Temporal CLI path; defaults to PATH lookup")
	flag.StringVar(&workerBinary, "worker", filepath.FromSlash("bin/external-effect-worker"), "experiment Worker binary")
	flag.StringVar(&outputRoot, "output", filepath.FromSlash("experiments/external-effects/evidence"), "append-only evidence root")
	flag.StringVar(&runPrefix, "run-id", time.Now().UTC().Format("20060102T150405Z"), "run ID prefix")
	flag.IntVar(&trials, "trials", 3, "fresh trials per destination and mode")
	flag.Parse()
	if trials < 1 {
		return errors.New("trials must be positive")
	}
	destinations, err := parseDestinations(rawDestination)
	if err != nil {
		return err
	}
	modes, err := parseModes(rawMode)
	if err != nil {
		return err
	}
	if temporalPath == "" {
		temporalPath, err = exec.LookPath("temporal")
		if err != nil {
			return errors.New("temporal CLI not found; pass --temporal")
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	for _, destination := range destinations {
		for _, mode := range modes {
			for trial := 1; trial <= trials; trial++ {
				runID := strings.Join([]string{
					runPrefix, string(destination), string(mode), "trial-" + strconv.Itoa(trial),
				}, "-")
				result, err := lab.Run(ctx, lab.Options{
					Destination: destination, Mode: mode, TemporalPath: temporalPath,
					WorkerBinary: workerBinary, OutputRoot: outputRoot, RunID: runID,
				})
				if err != nil {
					return fmt.Errorf("run %s/%s trial %d: %w", destination, mode, trial, err)
				}
				if err := encoder.Encode(result); err != nil {
					return fmt.Errorf("print %s/%s trial %d result: %w", destination, mode, trial, err)
				}
			}
		}
	}
	return nil
}

func parseDestinations(raw string) ([]lab.Destination, error) {
	normalized := strings.ToLower(raw)
	if normalized == "all" {
		return lab.AllDestinations(), nil
	}
	destination := lab.Destination(normalized)
	if !destination.Valid() {
		return nil, fmt.Errorf("invalid destination %q", raw)
	}
	return []lab.Destination{destination}, nil
}

func parseModes(raw string) ([]lab.Mode, error) {
	normalized := strings.ToLower(raw)
	if normalized == "all" {
		return []lab.Mode{lab.ModeUnsafe, lab.ModeProtected}, nil
	}
	mode := lab.Mode(normalized)
	if !mode.Valid() {
		return nil, fmt.Errorf("invalid mode %q", raw)
	}
	return []lab.Mode{mode}, nil
}
