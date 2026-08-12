package main

import "testing"

func TestParseOptionsPinsModelAndExplicitRecoveryMode(t *testing.T) {
	options, err := parseOptions([]string{
		"--evidence-root", "/tmp/evidence", "--temporal-binary", "/opt/temporal",
		"--worker-binary", "/opt/worker", "--effect-binary", "/opt/effect",
		"--launcher-binary", "/opt/launcher", "--codex-binary", "/opt/codex",
		"--codex-wrapper", "/opt/codex-2", "--codex-home", "/tmp/codex-home",
		"--output-schema", "/tmp/schema.json", "--recovery-mode", "explicit-thread-resume",
		"--model", "gpt-5.6-sol", "--reasoning-effort", "low", "--hermetic",
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if options.Model != "gpt-5.6-sol" || options.RecoveryMode != "explicit-thread-resume" || !options.Hermetic {
		t.Fatalf("options = %+v", options)
	}
}
