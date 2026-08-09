package main

import (
	"testing"
	"time"
)

func TestParseOptionsRequiresExplicitBinariesAndEvidenceRoot(t *testing.T) {
	t.Parallel()

	options, err := parseOptions([]string{
		"--evidence-root", "/tmp/evidence",
		"--temporal-binary", "/opt/temporal",
		"--worker-binary", "/opt/worker",
		"--effect-binary", "/opt/effect",
		"--launcher-binary", "/opt/launcher",
		"--claude-binary", "/opt/claude",
		"--trials", "3",
		"--timeout", "20m",
		"--model", "haiku",
		"--max-budget-usd", "0.25",
		"--max-turns", "2",
	})
	if err != nil {
		t.Fatalf("parse options: %v", err)
	}
	if options.EvidenceRoot != "/tmp/evidence" || options.Trials != 3 || options.Timeout != 20*time.Minute ||
		options.LauncherBinary != "/opt/launcher" || options.ClaudeBinary != "/opt/claude" || options.MaxTurns != 2 {
		t.Fatalf("options = %+v", options)
	}
	if _, err := parseOptions(nil); err == nil {
		t.Fatal("empty options returned nil error")
	}
}
