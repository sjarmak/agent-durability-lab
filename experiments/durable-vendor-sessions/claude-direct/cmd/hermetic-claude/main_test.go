package main

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestRunHermeticClaudeExecutesExactControlledEffect(t *testing.T) {
	const sessionID = "11111111-2222-4333-8444-555555555555"
	var output strings.Builder
	var executable string
	var arguments []string
	err := run(context.Background(), []string{"--model", "fake", "--session-id", sessionID},
		strings.NewReader("Use the Bash tool exactly once to run this exact command and no other command:\n"+
			"/opt/controlled-effect --request /tmp/request.json\n"+
			"After it succeeds, reply with EFFECT_COMPLETE.\n"), &output,
		func(_ context.Context, command string, args ...string) error {
			executable = command
			arguments = append([]string(nil), args...)
			return nil
		})
	if err != nil {
		t.Fatalf("run hermetic Claude: %v", err)
	}
	if executable != "/opt/controlled-effect" || strings.Join(arguments, " ") != "--request /tmp/request.json" {
		t.Fatalf("controlled effect = %q %q", executable, arguments)
	}
	text := output.String()
	if !strings.Contains(text, `"type":"system"`) || !strings.Contains(text, `"session_id":"`+sessionID+`"`) ||
		!strings.Contains(text, `"type":"result"`) || !strings.Contains(text, `"status":"EFFECT_COMPLETE"`) {
		t.Fatalf("stream output = %q", text)
	}
}

func TestRunHermeticClaudeSupportsResumeAndVersion(t *testing.T) {
	const sessionID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
	var version strings.Builder
	if err := run(context.Background(), []string{"--version"}, strings.NewReader(""), &version,
		func(context.Context, string, ...string) error { return errors.New("must not execute") }); err != nil {
		t.Fatalf("version: %v", err)
	}
	if got := strings.TrimSpace(version.String()); got != "hermetic-claude 1.0" {
		t.Fatalf("version = %q", got)
	}

	var output strings.Builder
	if err := run(context.Background(), []string{"--resume", sessionID},
		strings.NewReader("Use the Bash tool exactly once to run this exact command and no other command:\n"+
			"/opt/effect --request /tmp/request\nAfter it succeeds, reply with EFFECT_COMPLETE.\n"), &output,
		func(context.Context, string, ...string) error { return nil }); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if !strings.Contains(output.String(), sessionID) {
		t.Fatalf("resume output = %q", output.String())
	}
}

func TestRunHermeticClaudeRejectsCommandsOutsideProtocol(t *testing.T) {
	tests := []string{
		"/opt/effect --request /tmp/request;touch /tmp/unexpected",
		"/opt/effect --other /tmp/request",
		"effect --request /tmp/request",
		"/opt/effect --request request",
	}
	for _, controlledCommand := range tests {
		t.Run(controlledCommand, func(t *testing.T) {
			called := false
			err := run(context.Background(), []string{"--session-id", "11111111-2222-4333-8444-555555555555"},
				strings.NewReader("Use the Bash tool exactly once to run this exact command and no other command:\n"+
					controlledCommand+"\nAfter it succeeds, reply with EFFECT_COMPLETE.\n"), &strings.Builder{},
				func(context.Context, string, ...string) error { called = true; return nil })
			if err == nil || called {
				t.Fatalf("run error = %v, executed = %t", err, called)
			}
		})
	}
}

func TestRunHermeticClaudeRejectsMissingOrAmbiguousSession(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"--session-id", "one", "--resume", "two"},
		{"--session-id"},
		{"--session-id", "AAAAAAAA-BBBB-4CCC-8DDD-EEEEEEEEEEEE"},
	} {
		err := run(context.Background(), args,
			strings.NewReader("Use the Bash tool exactly once to run this exact command and no other command:\n"+
				"/opt/effect --request /tmp/request\nAfter it succeeds, reply with EFFECT_COMPLETE.\n"), &strings.Builder{},
			func(context.Context, string, ...string) error { return nil })
		if err == nil {
			t.Fatalf("args %q unexpectedly accepted", args)
		}
	}
}

func TestRunHermeticClaudeRejectsPromptOutsideExactProtocol(t *testing.T) {
	tests := []string{
		"wrong instruction\n/opt/effect --request /tmp/request\nAfter it succeeds, reply with EFFECT_COMPLETE.\n",
		"Use the Bash tool exactly once to run this exact command and no other command:\n/opt/effect --request /tmp/request\nwrong completion\n",
		"Use the Bash tool exactly once to run this exact command and no other command:\n/opt/effect --request /tmp/request\nAfter it succeeds, reply with EFFECT_COMPLETE.\nextra\n",
	}
	for _, prompt := range tests {
		called := false
		err := run(context.Background(), []string{"--session-id", "11111111-2222-4333-8444-555555555555"},
			strings.NewReader(prompt), &strings.Builder{},
			func(context.Context, string, ...string) error { called = true; return nil })
		if err == nil || called {
			t.Fatalf("prompt %q: error = %v, executed = %t", prompt, err, called)
		}
	}
}
