package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/sjarmak/temporal_projects/experiments/worker-versioning-compat/internal/lab"
)

func main() {
	flag.Parse()
	if flag.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: evidence-audit <evidence-root>")
		os.Exit(2)
	}
	result, err := lab.AuditEvidence(flag.Arg(0))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(encoded))
}
