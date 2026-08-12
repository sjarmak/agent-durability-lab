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

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/conformance/profile"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	os.Exit(runMain(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func runMain(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if err := run(ctx, args, stdout, stderr); err != nil {
		fmt.Fprintf(stderr, "coding-agent conformance failed: %v\n", err)
		return 1
	}
	return 0
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("coding-agent-conformance", flag.ContinueOnError)
	flags.SetOutput(stderr)
	evidenceRoot := flags.String("evidence-root", "", "new directory in which to preserve conformance evidence")
	sourceRoot := flags.String("source-root", "", "repository root containing the pinned calibration sources")
	schemaRoot := flags.String("schema-root", "", "directory containing protocol v1 schemas and their manifest")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	if *evidenceRoot == "" || *sourceRoot == "" || *schemaRoot == "" {
		return errors.New("--evidence-root, --source-root, and --schema-root are required")
	}
	executable, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve running executable: %w", err)
	}
	report, runErr := profile.RunCalibration(ctx, profile.Config{
		EvidenceRoot: *evidenceRoot, SourceRoot: *sourceRoot, SchemaRoot: *schemaRoot, ExecutablePath: executable,
	})
	if report.ContractVersion != "" {
		encoder := json.NewEncoder(stdout)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(report); err != nil {
			return errors.Join(runErr, fmt.Errorf("write result: %w", err))
		}
	}
	return runErr
}
