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

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/matrix"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintf(os.Stderr, "topology pilot: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("topology-pilot", flag.ContinueOnError)
	flags.SetOutput(stdout)
	evidenceRoot := flags.String("evidence-root", "", "new append-only pilot evidence root")
	workRoot := flags.String("work-root", "", "Temporal service and process work root outside evidence")
	preregistration := flags.String("preregistration", "", "frozen topology preregistration JSON")
	contract := flags.String("contract", "", "frozen topology contract JSON")
	temporalPath := flags.String("temporal-path", "", "Temporal CLI executable")
	agentBinary := flags.String("agent-binary", "", "hermetic agent-simulator executable")
	sourceRoot := flags.String("source-root", "", "repository source root to freeze")
	deadline := flags.Duration("deadline", 8*time.Hour, "whole-pilot liveness deadline")
	auditOnly := flags.Bool("audit-only", false, "audit an existing pilot without execution")
	verifyFreeze := flags.Bool("verify-freeze", false, "compare a qualified freeze with current sources, binaries, and host")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *evidenceRoot == "" || *deadline <= 0 {
		return errors.New("--evidence-root and a positive --deadline are required")
	}
	ctx, cancel := context.WithTimeout(ctx, *deadline)
	defer cancel()
	if *auditOnly {
		report, err := matrix.AuditPilot(*evidenceRoot)
		if err != nil {
			return err
		}
		if *verifyFreeze {
			if *sourceRoot == "" || *temporalPath == "" || *agentBinary == "" {
				return errors.New("--source-root, --temporal-path, and --agent-binary are required to verify the freeze")
			}
			executable, err := os.Executable()
			if err != nil {
				return err
			}
			if err := matrix.VerifyPilotFreeze(ctx, *evidenceRoot, matrix.PilotFreezeVerificationConfig{
				SourceRoot: *sourceRoot, RunnerBinary: executable, AgentBinary: *agentBinary, TemporalBinary: *temporalPath,
			}); err != nil {
				return err
			}
		}
		if err := encode(stdout, report); err != nil {
			return err
		}
		if !report.Qualified {
			return errors.New("pilot did not qualify")
		}
		return nil
	}
	if *workRoot == "" || *preregistration == "" || *contract == "" || *temporalPath == "" || *agentBinary == "" || *sourceRoot == "" {
		return errors.New("--work-root, --preregistration, --contract, --temporal-path, --agent-binary, and --source-root are required")
	}
	report, err := matrix.RunPilot(ctx, matrix.PilotConfig{
		Root: *evidenceRoot, PreregistrationPath: *preregistration, ContractPath: *contract,
		TemporalPath: *temporalPath, WorkRoot: *workRoot, AgentBinary: *agentBinary, SourceRoot: *sourceRoot,
	})
	if err != nil {
		return err
	}
	if err := encode(stdout, report); err != nil {
		return err
	}
	if !report.Qualified {
		return errors.New("pilot did not qualify")
	}
	return nil
}

func encode(writer io.Writer, report matrix.PilotReport) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}
