package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/calibration"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/oracle"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "agent durability v2 calibration: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	root := flag.String("evidence-root", "", "append-only evidence root")
	trials := flag.Int("trials", 3, "independent development trials per case and probe")
	flag.Parse()
	if *root == "" || *trials < 1 {
		return errors.New("--evidence-root and positive --trials are required")
	}
	ctx := context.Background()
	var verdicts []protocol.Verdict
	for _, benchmarkCase := range protocol.Cases() {
		for _, probe := range []protocol.Probe{protocol.ProbeUnfaulted, protocol.ProbeUnsafe, protocol.ProbeProtected} {
			for trial := 1; trial <= *trials; trial++ {
				runDir, err := calibration.Run(ctx, calibration.Config{Root: *root, Case: benchmarkCase, Probe: probe, Trial: trial})
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
	}
	return json.NewEncoder(os.Stdout).Encode(verdicts)
}
