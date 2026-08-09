package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/abalive"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/oracle"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "agent durability v2 live ABA: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	root := flag.String("evidence-root", "", "append-only evidence root")
	client := flag.String("client-binary", "", "absolute path to the ABA client binary")
	adapterVersion := flag.String("adapter-version", "", "source-sha256:<64 hex characters>")
	trials := flag.Int("trials", 3, "independent trials per unsafe/protected arm")
	flag.Parse()
	if *root == "" || *client == "" || *adapterVersion == "" || *trials < 1 {
		return errors.New("--evidence-root, --client-binary, --adapter-version, and positive --trials are required")
	}
	ctx := context.Background()
	var verdicts []protocol.Verdict
	for _, probe := range []protocol.Probe{protocol.ProbeUnsafe, protocol.ProbeProtected} {
		for trial := 1; trial <= *trials; trial++ {
			runDir, err := abalive.Run(ctx, abalive.Config{
				Root: *root, Probe: probe, Trial: trial, ClientBinary: *client, AdapterVersion: *adapterVersion,
			})
			if err != nil {
				return err
			}
			verdict, err := oracle.EvaluateAndWrite(ctx, runDir)
			if err != nil {
				return err
			}
			verdicts = append(verdicts, verdict)
		}
	}
	return json.NewEncoder(os.Stdout).Encode(verdicts)
}
