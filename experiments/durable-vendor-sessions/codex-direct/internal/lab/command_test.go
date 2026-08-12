package lab

import (
	"reflect"
	"strings"
	"testing"
)

func TestCodexCommandBuildsPinnedInitialAndExplicitResumeInvocations(t *testing.T) {
	command := CodexCommand{
		Binary: "/opt/codex/bin/codex", WorkDir: "/fixture", CodexHome: "/profiles/codex-2",
		Model: "gpt-5.6-sol", ReasoningEffort: "low", OutputSchema: "/fixture/result.schema.json",
		Sandbox: "workspace-write",
	}
	initial, err := command.InitialInvocation("run the controlled effect")
	if err != nil {
		t.Fatalf("initial invocation: %v", err)
	}
	wantInitial := Invocation{
		Binary: "/opt/codex/bin/codex",
		Args: []string{
			"--cd", "/fixture", "exec", "--json", "--ignore-user-config", "--ignore-rules",
			"--model", "gpt-5.6-sol", "-c", `model_reasoning_effort="low"`,
			"--sandbox", "workspace-write", "--output-schema", "/fixture/result.schema.json", "-",
		},
		Env: []string{"CODEX_HOME=/profiles/codex-2"}, WorkDir: "/fixture",
		Stdin: "run the controlled effect\n",
	}
	if !reflect.DeepEqual(initial, wantInitial) {
		t.Fatalf("initial invocation = %#v, want %#v", initial, wantInitial)
	}

	threadID := "0199a213-81c0-7800-8aa1-bbab2a035a53"
	resumed, err := command.ResumeInvocation("finish the same turn", threadID)
	if err != nil {
		t.Fatalf("resume invocation: %v", err)
	}
	wantResume := Invocation{
		Binary: "/opt/codex/bin/codex",
		Args: []string{
			"--cd", "/fixture", "exec", "--sandbox", "workspace-write", "resume",
			"--json", "--ignore-user-config", "--ignore-rules",
			"--model", "gpt-5.6-sol", "-c", `model_reasoning_effort="low"`,
			"--output-schema", "/fixture/result.schema.json", threadID, "-",
		},
		Env: []string{"CODEX_HOME=/profiles/codex-2"}, WorkDir: "/fixture",
		Stdin: "finish the same turn\n",
	}
	if !reflect.DeepEqual(resumed, wantResume) {
		t.Fatalf("resume invocation = %#v, want %#v", resumed, wantResume)
	}
	for _, invocation := range []Invocation{initial, resumed} {
		joined := strings.Join(invocation.Args, " ")
		if !strings.Contains(joined, "--sandbox workspace-write") {
			t.Fatalf("invocation does not pin workspace-write sandbox: %q", joined)
		}
		if strings.Contains(joined, "--last") || strings.Contains(joined, "--all") ||
			strings.Contains(joined, "--ephemeral") || strings.Contains(joined, "danger-full-access") {
			t.Fatalf("invocation contains unsafe or ambiguous control: %q", joined)
		}
	}
}

func TestCodexCommandRejectsIncompleteOrAmbiguousIdentity(t *testing.T) {
	valid := CodexCommand{
		Binary: "/opt/codex/bin/codex", WorkDir: "/fixture", CodexHome: "/profiles/codex-2",
		Model: "gpt-5.6-sol", ReasoningEffort: "low", OutputSchema: "/fixture/result.schema.json",
		Sandbox: "workspace-write",
	}
	tests := []struct {
		name string
		edit func(*CodexCommand)
	}{
		{"binary", func(command *CodexCommand) { command.Binary = "" }},
		{"workdir", func(command *CodexCommand) { command.WorkDir = "" }},
		{"home", func(command *CodexCommand) { command.CodexHome = "" }},
		{"model", func(command *CodexCommand) { command.Model = "" }},
		{"reasoning", func(command *CodexCommand) { command.ReasoningEffort = "maximum" }},
		{"schema", func(command *CodexCommand) { command.OutputSchema = "" }},
		{"sandbox", func(command *CodexCommand) { command.Sandbox = "danger-full-access" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command := valid
			test.edit(&command)
			if _, err := command.InitialInvocation("effect"); err == nil {
				t.Fatal("initial invocation unexpectedly succeeded")
			}
		})
	}
	if _, err := valid.InitialInvocation(" \n"); err == nil {
		t.Fatal("blank prompt unexpectedly succeeded")
	}
	for _, threadID := range []string{"", "--last", "0199A213-81C0-7800-8AA1-BBAB2A035A53", "not-a-uuid"} {
		if _, err := valid.ResumeInvocation("effect", threadID); err == nil {
			t.Fatalf("resume with thread ID %q unexpectedly succeeded", threadID)
		}
	}
}
