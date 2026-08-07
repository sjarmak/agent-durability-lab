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

	"github.com/sjarmak/temporal_projects/experiments/cancellation/internal/lab"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "cancellation experiment: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var rawScenario, rawWait, temporalPath, workerBinary, agentBinary, outputRoot, runID string
	var trials int
	flag.StringVar(&rawScenario, "scenario", "all", "temporal-control, healthy-safe, worker-death-safe, frozen-safe, or all")
	flag.StringVar(&rawWait, "wait-policy", "both", "false, true, or both")
	flag.StringVar(&temporalPath, "temporal", "", "Temporal CLI path; defaults to PATH lookup")
	flag.StringVar(&workerBinary, "worker", filepath.FromSlash("bin/lab-worker"), "Worker binary path")
	flag.StringVar(&agentBinary, "agent", filepath.FromSlash("bin/agent-simulator"), "agent simulator binary path")
	flag.StringVar(&outputRoot, "output", filepath.FromSlash("experiments/cancellation/evidence"), "append-only evidence root")
	flag.StringVar(&runID, "run-id", time.Now().UTC().Format("20060102T150405Z"), "run ID prefix")
	flag.IntVar(&trials, "trials", 3, "fresh trials per selected arm")
	flag.Parse()
	if trials < 1 {
		return errors.New("trials must be positive")
	}
	scenarios, err := parseScenarios(rawScenario)
	if err != nil {
		return err
	}
	waitPolicies, err := parseWaitPolicies(rawWait)
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
	for _, scenario := range scenarios {
		for _, wait := range waitPolicies {
			for trial := 1; trial <= trials; trial++ {
				variant := fmt.Sprintf("%s-wait-%t-trial-%d", scenario, wait, trial)
				result, err := lab.Run(ctx, lab.Options{
					Scenario: scenario, WaitForCancellation: wait, TemporalPath: temporalPath,
					WorkerBinary: workerBinary, AgentBinary: agentBinary, OutputRoot: outputRoot,
					RunID: runID + "-" + variant,
				})
				if err != nil {
					return fmt.Errorf("run %s: %w", variant, err)
				}
				if err := encoder.Encode(result); err != nil {
					return fmt.Errorf("print %s result: %w", variant, err)
				}
			}
		}
	}
	return nil
}

func parseScenarios(raw string) ([]lab.Scenario, error) {
	if strings.EqualFold(raw, "all") {
		return []lab.Scenario{
			lab.ScenarioTemporalControl, lab.ScenarioHealthySafe,
			lab.ScenarioWorkerDeathSafe, lab.ScenarioFrozenSafe,
		}, nil
	}
	scenario := lab.Scenario(strings.ToLower(raw))
	if !scenario.Valid() {
		return nil, fmt.Errorf("invalid scenario %q", raw)
	}
	return []lab.Scenario{scenario}, nil
}

func parseWaitPolicies(raw string) ([]bool, error) {
	if strings.EqualFold(raw, "both") {
		return []bool{false, true}, nil
	}
	wait, err := strconv.ParseBool(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid wait policy %q", raw)
	}
	return []bool{wait}, nil
}
