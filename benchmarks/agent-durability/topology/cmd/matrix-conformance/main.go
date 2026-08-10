package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/matrix"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "topology matrix conformance: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	evidenceRoot := flag.String("evidence-root", "", "new append-only integrated matrix evidence root")
	workRoot := flag.String("work-root", "", "Temporal service and process work root outside the evidence root")
	preregistration := flag.String("preregistration", "", "frozen topology preregistration JSON")
	temporalPath := flag.String("temporal-path", "", "Temporal CLI executable")
	agentBinary := flag.String("agent-binary", "", "hermetic agent-simulator executable")
	deadline := flag.Duration("deadline", 30*time.Minute, "whole-suite liveness deadline")
	auditOnly := flag.Bool("audit-only", false, "audit an existing evidence root without executing")
	flag.Parse()
	if *evidenceRoot == "" || *deadline <= 0 {
		return errors.New("--evidence-root and a positive --deadline are required")
	}
	if *auditOnly {
		report, err := matrix.Audit(*evidenceRoot)
		if err != nil {
			return err
		}
		return encodeReport(report)
	}
	if *workRoot == "" || *preregistration == "" || *temporalPath == "" || *agentBinary == "" {
		return errors.New("--work-root, --preregistration, --temporal-path, and --agent-binary are required")
	}
	if err := matrix.ValidateDisjointPaths(*evidenceRoot, *workRoot); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *deadline)
	defer cancel()
	report, err := matrix.RunConformance(ctx, matrix.Config{
		Root: *evidenceRoot, PreregistrationPath: *preregistration,
		TemporalPath: *temporalPath, WorkRoot: *workRoot, AgentBinary: *agentBinary,
	})
	if err != nil {
		return err
	}
	return encodeReport(report)
}

func encodeReport(report matrix.Report) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}
