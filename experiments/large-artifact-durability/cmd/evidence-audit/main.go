package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/sjarmak/temporal_projects/experiments/large-artifact-durability/internal/lab"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, lab.AuditRun, lab.AuditPopulation); err != nil {
		fmt.Fprintf(os.Stderr, "large-artifact evidence audit: %v\n", err)
		os.Exit(1)
	}
}

func run(
	args []string,
	output io.Writer,
	auditRun func(string) (lab.Verdict, error),
	auditPopulation func(string) (lab.PopulationIndex, error),
) error {
	if len(args) == 2 && args[0] == "--population" && args[1] != "" {
		index, err := auditPopulation(args[1])
		if err != nil {
			return err
		}
		return writeJSON(output, index)
	}
	if len(args) != 1 || args[0] == "" {
		return errors.New("usage: evidence-audit [--population] EVIDENCE_DIRECTORY")
	}
	verdict, err := auditRun(args[0])
	if err != nil {
		return err
	}
	if !verdict.RunValid || !verdict.ExpectedObservation {
		return errors.New("evidence verdict is not an expected valid run")
	}
	return writeJSON(output, verdict)
}

func writeJSON(output io.Writer, value any) error {
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}
