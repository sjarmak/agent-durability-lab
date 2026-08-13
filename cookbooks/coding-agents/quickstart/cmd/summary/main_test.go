package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/sjarmak/temporal_projects/cookbooks/coding-agents/quickstart"
)

func TestRunRequiresReceiptsBeforeRendering(t *testing.T) {
	t.Parallel()
	var receipts strings.Builder
	for _, name := range quickstart.RequiredTestReceipts() {
		fmt.Fprintf(&receipts, `{"Action":"pass","Test":%q}`+"\n", name)
	}
	var output bytes.Buffer
	if err := run(strings.NewReader(receipts.String()), &output); err != nil {
		t.Fatalf("run(valid): %v", err)
	}
	if !strings.Contains(output.String(), "FIRST TRUSTWORTHY RECOVERY") {
		t.Fatalf("output = %q", output.String())
	}

	output.Reset()
	if err := run(strings.NewReader(""), &output); err == nil {
		t.Fatal("run() accepted missing test receipts")
	}
	if output.Len() != 0 {
		t.Fatalf("failed run wrote presentation: %q", output.String())
	}
}

func TestExitCodeReportsReceiptFailure(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	var errorOutput bytes.Buffer
	if code := exitCode(strings.NewReader(""), &output, &errorOutput); code != 1 {
		t.Fatalf("exitCode() = %d, want 1", code)
	}
	if output.Len() != 0 || !strings.Contains(errorOutput.String(), "missing pass receipt") {
		t.Fatalf("stdout=%q stderr=%q", output.String(), errorOutput.String())
	}
}
