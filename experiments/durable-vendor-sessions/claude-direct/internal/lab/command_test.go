package lab

import (
	"slices"
	"strings"
	"testing"
)

func TestUnsafeClaudeInvocationIsStructuredAndHasNoRecoveryControls(t *testing.T) {
	t.Parallel()

	invocation, err := (ClaudeCommand{
		Binary:       "/opt/claude",
		WorkDir:      "/work/fixture",
		Model:        "haiku",
		MaxBudgetUSD: "0.25",
		MaxTurns:     2,
		AllowedTool:  "Bash(/work/fixture/controlled-effect --request /work/fixture/effect.json)",
	}).Invocation("Call the controlled effect exactly once.")
	if err != nil {
		t.Fatalf("build invocation: %v", err)
	}
	wantArgs := []string{
		"-p",
		"--output-format", "stream-json",
		"--verbose",
		"--json-schema", `{"type":"object","properties":{"status":{"type":"string","enum":["EFFECT_COMPLETE"]}},"required":["status"],"additionalProperties":false}`,
		"--safe-mode",
		"--permission-mode", "dontAsk",
		"--tools", "Bash",
		"--allowedTools", "Bash(/work/fixture/controlled-effect --request /work/fixture/effect.json)",
		"--model", "haiku",
		"--max-turns", "2",
		"--max-budget-usd", "0.25",
	}
	if invocation.Binary != "/opt/claude" || invocation.WorkDir != "/work/fixture" {
		t.Fatalf("invocation identity = %+v", invocation)
	}
	if !slices.Equal(invocation.Args, wantArgs) {
		t.Fatalf("args = %q, want %q", invocation.Args, wantArgs)
	}
	if invocation.Stdin != "Call the controlled effect exactly once.\n" {
		t.Fatalf("stdin = %q", invocation.Stdin)
	}

	joined := strings.Join(invocation.Args, " ")
	for _, forbidden := range []string{"--session-id", "--resume", "--continue", "--fork-session", "--background", "--bg"} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("unsafe direct invocation unexpectedly contains %q: %s", forbidden, joined)
		}
	}
}

func TestUnsafeClaudeInvocationRejectsIncompleteOrUnboundedInput(t *testing.T) {
	t.Parallel()

	tests := []ClaudeCommand{
		{},
		{Binary: "/opt/claude", WorkDir: "/work", Model: "haiku", MaxBudgetUSD: "0.25", MaxTurns: 0, AllowedTool: "Bash(command)"},
		{Binary: "/opt/claude", WorkDir: "/work", Model: "haiku", MaxBudgetUSD: "unbounded", MaxTurns: 2, AllowedTool: "Bash(command)"},
		{Binary: "/opt/claude", WorkDir: "/work", Model: "haiku", MaxBudgetUSD: "0.25", MaxTurns: 2},
	}
	for index, command := range tests {
		if _, err := command.Invocation("prompt"); err == nil {
			t.Errorf("case %d returned nil error", index)
		}
	}
	if _, err := (ClaudeCommand{
		Binary: "/opt/claude", WorkDir: "/work", Model: "haiku", MaxBudgetUSD: "0.25", MaxTurns: 2,
		AllowedTool: "Bash(command)",
	}).Invocation(""); err == nil {
		t.Fatal("empty prompt returned nil error")
	}
}
