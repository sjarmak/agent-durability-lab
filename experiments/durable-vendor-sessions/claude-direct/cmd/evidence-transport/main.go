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

	"github.com/sjarmak/temporal_projects/experiments/durable-vendor-sessions/claude-direct/transport"
)

type commandOperations struct {
	build   func(context.Context, transport.BuildConfig) (transport.Index, error)
	verify  func(context.Context, string) (transport.Index, error)
	restore func(context.Context, string, string) error
}

var defaultOperations = commandOperations{
	build: transport.Build, verify: transport.Verify, restore: transport.Restore,
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr, defaultOperations); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer, operations commandOperations) error {
	if len(args) == 0 {
		return errors.New("usage: evidence-transport package|verify|restore [flags]")
	}
	switch args[0] {
	case "package":
		return runPackage(ctx, args[1:], stdout, stderr, operations.build)
	case "verify":
		return runVerify(ctx, args[1:], stdout, stderr, operations.verify)
	case "restore":
		return runRestore(ctx, args[1:], stdout, stderr, operations.restore)
	default:
		return fmt.Errorf("unknown evidence-transport command %q", args[0])
	}
}

func runPackage(ctx context.Context, args []string, stdout, stderr io.Writer, build func(context.Context, transport.BuildConfig) (transport.Index, error)) error {
	flags := flag.NewFlagSet("package", flag.ContinueOnError)
	flags.SetOutput(stderr)
	source := flags.String("source", "", "raw evidence collection root")
	lineage := flags.String("lineage", "", "correction-lineage JSON")
	output := flags.String("output", "", "new transport directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *source == "" || *lineage == "" || *output == "" || flags.NArg() != 0 {
		return errors.New("package requires --source, --lineage, and --output")
	}
	index, err := build(ctx, transport.BuildConfig{SourceRoot: *source, LineagePath: *lineage, OutputRoot: *output})
	if err != nil {
		return fmt.Errorf("package evidence: %w", err)
	}
	return writeResult(stdout, index)
}

func runVerify(ctx context.Context, args []string, stdout, stderr io.Writer, verify func(context.Context, string) (transport.Index, error)) error {
	flags := flag.NewFlagSet("verify", flag.ContinueOnError)
	flags.SetOutput(stderr)
	root := flags.String("transport", "", "transport directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *root == "" || flags.NArg() != 0 {
		return errors.New("verify requires --transport")
	}
	index, err := verify(ctx, *root)
	if err != nil {
		return fmt.Errorf("verify evidence transport: %w", err)
	}
	return writeResult(stdout, index)
}

func runRestore(ctx context.Context, args []string, stdout, stderr io.Writer, restore func(context.Context, string, string) error) error {
	flags := flag.NewFlagSet("restore", flag.ContinueOnError)
	flags.SetOutput(stderr)
	root := flags.String("transport", "", "transport directory")
	output := flags.String("output", "", "new restored evidence directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *root == "" || *output == "" || flags.NArg() != 0 {
		return errors.New("restore requires --transport and --output")
	}
	if err := restore(ctx, *root, *output); err != nil {
		return fmt.Errorf("restore evidence transport: %w", err)
	}
	return writeResult(stdout, map[string]string{"restored_to": *output})
}

func writeResult(writer io.Writer, value any) error {
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}
