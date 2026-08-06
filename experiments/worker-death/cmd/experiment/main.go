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
	var rawMode string
	var temporalPath string
	var workerBinary string
	var agentBinary string
	var outputRoot string
	var runID string
	flag.StringVar(&rawMode, "mode", "all", "unsafe, reattach, fenced, or all")
	flag.StringVar(&temporalPath, "temporal", "", "Temporal CLI path; defaults to PATH lookup")
	flag.StringVar(&workerBinary, "worker", filepath.FromSlash("bin/lab-worker"), "Worker binary path")
	flag.StringVar(&agentBinary, "agent", filepath.FromSlash("bin/agent-simulator"), "agent simulator binary path")
	flag.StringVar(&outputRoot, "output", filepath.FromSlash("experiments/worker-death/evidence"), "append-only evidence root")
	flag.StringVar(&runID, "run-id", time.Now().UTC().Format("20060102T150405Z"), "run ID or prefix")
	flag.Parse()

	if temporalPath == "" {
		resolved, err := exec.LookPath("temporal")
		if err != nil {
			return errors.New("temporal CLI not found; pass --temporal")
		}
		temporalPath = resolved
	}
	modes, err := parseModes(rawMode)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	for _, mode := range modes {
		modeRunID := runID
		if len(modes) > 1 {
			modeRunID += "-" + string(mode)
		}
		result, err := lab.Run(ctx, lab.Options{
			Mode: mode, TemporalPath: temporalPath, WorkerBinary: workerBinary,
			AgentBinary: agentBinary, OutputRoot: outputRoot, RunID: modeRunID,
		})
		if err != nil {
			return fmt.Errorf("run %s arm: %w", mode, err)
		}
		if err := encoder.Encode(result); err != nil {
			return fmt.Errorf("print %s result: %w", mode, err)
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
