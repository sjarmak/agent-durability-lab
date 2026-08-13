package main

import (
	"fmt"
	"io"
	"os"

	"github.com/sjarmak/temporal_projects/cookbooks/coding-agents/quickstart"
)

func main() {
	os.Exit(exitCode(os.Stdin, os.Stdout, os.Stderr))
}

func exitCode(receipts io.Reader, output, errorOutput io.Writer) int {
	if err := run(receipts, output); err != nil {
		fmt.Fprintf(errorOutput, "coding-agent quickstart: %v\n", err)
		return 1
	}
	return 0
}

func run(receipts io.Reader, output io.Writer) error {
	if err := quickstart.VerifyTestReceipts(receipts); err != nil {
		return err
	}
	catalog, err := quickstart.LoadCatalog()
	if err != nil {
		return fmt.Errorf("load presentation catalog: %w", err)
	}
	return quickstart.WriteSummary(output, catalog)
}
