package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/evidence"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/oracle"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
	"github.com/sjarmak/temporal_projects/experiments/durable-vendor-sessions/temporal-native/evidenceadapter"
)

type summary struct {
	RunDirectory string                `json:"run_directory"`
	Class        protocol.VerdictClass `json:"class"`
	ReasonCodes  []string              `json:"reason_codes"`
}

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, output io.Writer) error {
	flags := flag.NewFlagSet("temporal-native-evidence", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	capturePath := flags.String("capture", "", "path to one native capture JSON file")
	evidenceRoot := flags.String("evidence-root", "", "append-only evidence root")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *capturePath == "" || *evidenceRoot == "" || flags.NArg() != 0 {
		return errors.New("--capture and --evidence-root are required")
	}
	capture, err := readCapture(*capturePath)
	if err != nil {
		return err
	}
	bundle, err := evidenceadapter.BuildBundle(capture)
	if err != nil {
		return err
	}
	runDir, err := evidence.WriteRun(ctx, *evidenceRoot, bundle)
	if err != nil {
		return err
	}
	verdict, err := oracle.EvaluateAndWrite(ctx, runDir)
	if err != nil {
		return err
	}
	want := protocol.VerdictValidPass
	if capture.Probe == protocol.ProbeUnsafe {
		want = protocol.VerdictValidFail
	}
	if verdict.Class != want {
		return fmt.Errorf("unexpected independent verdict %q, want %q: %v", verdict.Class, want, verdict.ReasonCodes)
	}
	return json.NewEncoder(output).Encode(summary{RunDirectory: runDir, Class: verdict.Class, ReasonCodes: verdict.ReasonCodes})
}

func readCapture(path string) (capture evidenceadapter.Capture, returnErr error) {
	file, err := os.Open(path)
	if err != nil {
		return evidenceadapter.Capture{}, fmt.Errorf("open capture: %w", err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, file.Close())
	}()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&capture); err != nil {
		return evidenceadapter.Capture{}, fmt.Errorf("decode capture: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return evidenceadapter.Capture{}, errors.New("capture contains trailing JSON")
	}
	return capture, nil
}
