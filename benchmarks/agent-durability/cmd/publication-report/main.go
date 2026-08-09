package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/publication/report"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "agent durability v2 publication report: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	root := flag.String("root", "", "completed append-only population evidence root")
	preregistration := flag.String("preregistration", "benchmarks/agent-durability/publication-preregistration-v2.json", "frozen preregistration")
	output := flag.String("output", "", "new append-only analysis JSON path outside the evidence root")
	flag.Parse()
	if *root == "" || *preregistration == "" || *output == "" {
		return errors.New("root, preregistration, and output are required")
	}
	analysis, err := report.AnalyzePopulationFiles(*root, *preregistration)
	if err != nil {
		return err
	}
	return report.WriteAnalysis(*output, analysis)
}
