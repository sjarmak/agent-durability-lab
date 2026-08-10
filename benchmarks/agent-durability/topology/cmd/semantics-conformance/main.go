package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/semantics"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "topology conformance: %v\n", err)
		os.Exit(1)
	}
}

func run() (returnErr error) {
	evidenceRoot := flag.String("evidence-root", "", "new append-only evidence root")
	workRoot := flag.String("work-root", "", "Temporal service and process work root outside the evidence root")
	temporalPath := flag.String("temporal-path", "", "Temporal CLI executable")
	agentBinary := flag.String("agent-binary", "", "hermetic agent-simulator executable")
	fanout := flag.Int("fanout", 32, "fixed mechanism conformance fan-out: 8, 32, or 128")
	trials := flag.Int("trials", 1, "independent development trials per case/boundary/probe")
	suite := flag.String("suite", "semantics", "mechanism suite: semantics or recovery")
	deadline := flag.Duration("deadline", 20*time.Minute, "whole-suite liveness deadline")
	flag.Parse()
	if *evidenceRoot == "" || *workRoot == "" || *temporalPath == "" || *agentBinary == "" || *deadline <= 0 ||
		(*suite != "semantics" && *suite != "recovery") {
		return errors.New("--evidence-root, --work-root, --temporal-path, --agent-binary, and a positive --deadline are required")
	}
	if err := requireDisjointRoots(*evidenceRoot, *workRoot); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *deadline)
	defer cancel()
	executor, err := semantics.OpenTemporalExecutor(ctx, semantics.ExecutorConfig{
		TemporalPath: *temporalPath, WorkRoot: *workRoot, AgentBinary: *agentBinary,
	})
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, executor.Close()) }()
	var summary semantics.ConformanceSummary
	var runErr error
	if *suite == "recovery" {
		summary, runErr = semantics.RunRecoveryConformance(ctx, executor, *evidenceRoot, *fanout, *trials)
	} else {
		summary, runErr = semantics.RunConformance(ctx, executor, *evidenceRoot, *fanout, *trials)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(summary); err != nil {
		return errors.Join(runErr, err)
	}
	return runErr
}

func requireDisjointRoots(evidenceRoot, workRoot string) error {
	for _, root := range []string{evidenceRoot, workRoot} {
		if err := os.MkdirAll(root, 0o750); err != nil {
			return err
		}
	}
	evidenceResolved, err := filepath.EvalSymlinks(evidenceRoot)
	if err != nil {
		return err
	}
	workResolved, err := filepath.EvalSymlinks(workRoot)
	if err != nil {
		return err
	}
	evidenceResolved, err = filepath.Abs(evidenceResolved)
	if err != nil {
		return err
	}
	workResolved, err = filepath.Abs(workResolved)
	if err != nil {
		return err
	}
	if pathContains(evidenceResolved, workResolved) || pathContains(workResolved, evidenceResolved) {
		return errors.New("evidence-root and work-root must be disjoint")
	}
	return nil
}

func pathContains(root, candidate string) bool {
	relative, err := filepath.Rel(root, candidate)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}
