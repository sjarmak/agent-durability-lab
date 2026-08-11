package main

import "testing"

func TestParseOptionsRequiresRootAndNewOutput(t *testing.T) {
	options, err := parseOptions([]string{
		"--mode", "fenced", "--root", "/evidence/v2", "--output", "/evidence/v2-audit.json",
	})
	if err != nil {
		t.Fatalf("parse options: %v", err)
	}
	if options.mode != "fenced" || options.root != "/evidence/v2" || options.output != "/evidence/v2-audit.json" {
		t.Fatalf("options = %+v", options)
	}
	if options, err := parseOptions([]string{
		"--mode", "resume-only", "--root", "/evidence/control", "--output", "/evidence/control-audit.json",
	}); err != nil || options.mode != "resume-only" {
		t.Fatalf("parse resume-only options = %+v, %v", options, err)
	}
	if options, err := parseOptions([]string{
		"--mode", "direct", "--root", "/evidence/direct", "--output", "/evidence/direct-audit.json",
	}); err != nil || options.mode != "direct" {
		t.Fatalf("parse direct options = %+v, %v", options, err)
	}
	for _, args := range [][]string{
		nil,
		{"--mode", "fenced", "--root", "/evidence/v2"},
		{"--mode", "fenced", "--output", "/evidence/audit.json"},
		{"--root", "/evidence/v2", "--output", "/evidence/audit.json"},
		{"--mode", "unknown", "--root", "/evidence/v2", "--output", "/evidence/audit.json"},
		{"--mode", "fenced", "--root", "/evidence/v2", "--output", "/evidence/audit.json", "extra"},
	} {
		if _, err := parseOptions(args); err == nil {
			t.Fatalf("args %q unexpectedly accepted", args)
		}
	}
}
