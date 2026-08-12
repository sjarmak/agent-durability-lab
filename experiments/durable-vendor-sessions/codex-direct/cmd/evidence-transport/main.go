package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/sjarmak/temporal_projects/experiments/durable-vendor-sessions/codex-direct/transport"
)

type operations struct {
	build   func(context.Context, transport.BuildConfig) (transport.Index, error)
	verify  func(context.Context, string) (transport.Index, error)
	restore func(context.Context, string, string) error
}

var defaultOperations = operations{build: transport.Build, verify: transport.Verify, restore: transport.Restore}

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, defaultOperations); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout io.Writer, operation operations) error {
	if len(args) == 0 {
		return errors.New("usage: evidence-transport package|verify|restore [flags]")
	}
	switch args[0] {
	case "package":
		flags := flag.NewFlagSet("package", flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		source := flags.String("source", "", "source root containing evidence bundles and audits")
		lineage := flags.String("lineage", "", "lineage JSON")
		output := flags.String("output", "", "new transport directory")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 || *source == "" || *lineage == "" || *output == "" {
			return errors.New("package requires --source, --lineage, and --output")
		}
		index, err := operation.build(ctx, transport.BuildConfig{SourceRoot: *source, LineagePath: *lineage, OutputRoot: *output})
		if err != nil {
			return err
		}
		return json.NewEncoder(stdout).Encode(index)
	case "verify":
		flags := flag.NewFlagSet("verify", flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		root := flags.String("transport", "", "transport directory")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 || *root == "" {
			return errors.New("verify requires --transport")
		}
		index, err := operation.verify(ctx, *root)
		if err != nil {
			return err
		}
		return json.NewEncoder(stdout).Encode(index)
	case "restore":
		flags := flag.NewFlagSet("restore", flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		root := flags.String("transport", "", "transport directory")
		output := flags.String("output", "", "new restoration directory")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 || *root == "" || *output == "" {
			return errors.New("restore requires --transport and --output")
		}
		return operation.restore(ctx, *root, *output)
	default:
		return fmt.Errorf("unknown evidence-transport command %q", args[0])
	}
}
