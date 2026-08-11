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

func TestResumeOnlyClaudeInvocationUsesCallerSelectedSessionIdentity(t *testing.T) {
	t.Parallel()

	command := ClaudeCommand{
		Binary:       "/opt/claude",
		WorkDir:      "/work/fixture",
		Model:        "haiku",
		MaxBudgetUSD: "0.25",
		MaxTurns:     2,
		AllowedTool:  "Bash(/work/fixture/controlled-effect --request /work/fixture/effect.json)",
	}
	const sessionID = "01890f3e-7b5a-4c2d-8e1f-0123456789ab"

	started, err := command.SessionInvocation("prompt", sessionID, 1)
	if err != nil {
		t.Fatalf("build selected-session invocation: %v", err)
	}
	resumed, err := command.SessionInvocation("prompt", sessionID, 2)
	if err != nil {
		t.Fatalf("build resumed invocation: %v", err)
	}
	startArgs := strings.Join(started.Args, " ")
	resumeArgs := strings.Join(resumed.Args, " ")
	if !strings.Contains(startArgs, "--session-id "+sessionID) || strings.Contains(startArgs, "--resume") {
		t.Fatalf("selected-session args = %q", startArgs)
	}
	if !strings.Contains(resumeArgs, "--resume "+sessionID) || strings.Contains(resumeArgs, "--session-id") {
		t.Fatalf("resume args = %q", resumeArgs)
	}
	for _, joined := range []string{startArgs, resumeArgs} {
		if strings.Contains(joined, "--fork-session") || strings.Contains(joined, "--continue") {
			t.Fatalf("resume-only invocation contains lineage-changing control: %q", joined)
		}
	}
}

func TestResumeOnlyClaudeInvocationRejectsInvalidSessionOrAttempt(t *testing.T) {
	t.Parallel()

	command := ClaudeCommand{
		Binary: "/opt/claude", WorkDir: "/work", Model: "haiku", MaxBudgetUSD: "0.25", MaxTurns: 2,
		AllowedTool: "Bash(command)",
	}
	for _, test := range []struct {
		sessionID string
		attempt   int32
	}{
		{sessionID: "not-a-uuid", attempt: 1},
		{sessionID: "01890f3e-7b5a-4c2d-8e1f-0123456789ab", attempt: 0},
	} {
		if _, err := command.SessionInvocation("prompt", test.sessionID, test.attempt); err == nil {
			t.Fatalf("SessionInvocation(%q, %d) returned nil error", test.sessionID, test.attempt)
		}
	}
}
