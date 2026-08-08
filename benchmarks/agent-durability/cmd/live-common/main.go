package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/livecommon"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/oracle"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

type summary struct {
	ValidPass int `json:"valid_pass"`
	ValidFail int `json:"valid_fail"`
	Invalid   int `json:"invalid"`
}

func main() {
	evidenceDir := flag.String("evidence-dir", "", "new directory in which to preserve live common evidence")
	agentBinary := flag.String("agent-binary", "", "path to the built agent-simulator binary")
	adapterVersion := flag.String("adapter-version", "", "immutable adapter commit or source hash")
	flag.Parse()
	if *evidenceDir == "" || *agentBinary == "" || *adapterVersion == "" {
		fmt.Fprintln(os.Stderr, "-evidence-dir, -agent-binary, and -adapter-version are required")
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	result, err := run(ctx, *evidenceDir, *agentBinary, *adapterVersion)
	if err != nil {
		fmt.Fprintf(os.Stderr, "live common suite failed: %v\n", err)
		os.Exit(1)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		fmt.Fprintf(os.Stderr, "write summary: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, evidenceDir, agentBinary, adapterVersion string) (summary, error) {
	var result summary
	probes := []protocol.Probe{protocol.ProbeUnfaulted, protocol.ProbeUnsafe, protocol.ProbeProtected}
	for _, benchmarkCase := range protocol.Cases() {
		for _, probe := range probes {
			for trial := 1; trial <= 3; trial++ {
				runDir, err := livecommon.Run(ctx, livecommon.Config{
					Root: evidenceDir, Case: benchmarkCase, Probe: probe,
					Trial: trial, AgentBinary: agentBinary, AdapterVersion: adapterVersion,
				})
				if err != nil {
					return result, fmt.Errorf("run %s/%s trial %d: %w", benchmarkCase, probe, trial, err)
				}
				verdict, err := oracle.EvaluateAndWrite(ctx, runDir)
				if err != nil {
					return result, fmt.Errorf("evaluate %s/%s trial %d: %w", benchmarkCase, probe, trial, err)
				}
				if err := recordVerdict(&result, probe, verdict); err != nil {
					return result, fmt.Errorf("check %s/%s trial %d: %w", benchmarkCase, probe, trial, err)
				}
			}
		}
	}
	return result, nil
}

func recordVerdict(result *summary, probe protocol.Probe, verdict protocol.Verdict) error {
	want := protocol.VerdictValidPass
	if probe == protocol.ProbeUnsafe {
		want = protocol.VerdictValidFail
	}
	if verdict.Class != want {
		return fmt.Errorf("verdict is %s, want %s: %v", verdict.Class, want, verdict.ReasonCodes)
	}
	switch verdict.Class {
	case protocol.VerdictValidPass:
		result.ValidPass++
	case protocol.VerdictValidFail:
		result.ValidFail++
	case protocol.VerdictInvalid:
		result.Invalid++
	default:
		return fmt.Errorf("unsupported verdict class %q", verdict.Class)
	}
	return nil
}
