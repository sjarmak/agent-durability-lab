package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/sjarmak/temporal_projects/experiments/durable-vendor-sessions/sandbox-harness/internal/lab"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, output io.Writer) error {
	flags := flag.NewFlagSet("sandbox-harness-evidence", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	evidenceRoot := flags.String("evidence-root", "", "new append-only evidence directory")
	temporalPath := flags.String("temporal-path", "", "Temporal CLI path")
	trials := flags.Int("trials", 3, "trials per probe and operation")
	timeout := flags.Duration("timeout", 3*time.Minute, "suite timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *evidenceRoot == "" || *temporalPath == "" || flags.NArg() != 0 {
		return errors.New("--evidence-root and --temporal-path are required")
	}
	result, err := lab.Run(ctx, lab.Options{
		EvidenceRoot: *evidenceRoot, TemporalPath: *temporalPath, Trials: *trials, Timeout: *timeout,
	})
	if err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(result)
}
