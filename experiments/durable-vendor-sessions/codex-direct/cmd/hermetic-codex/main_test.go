package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/sjarmak/temporal_projects/experiments/durable-vendor-sessions/codex-direct/internal/lab"
)

const hermeticThreadID = "019ff302-7730-7f21-90ed-73c37fb4e8fa"

func TestRunInitialAndResumeEmitCodexJSONLAndExecuteOnce(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
	}{
		{
			name: "initial",
			args: []string{"--cd", "/fixture", "exec", "--json", "--ignore-user-config", "--ignore-rules",
				"--model", "gpt-5.6-sol", "-c", `model_reasoning_effort="low"`, "--sandbox", "workspace-write",
				"--output-schema", "/fixture/result.schema.json", "-"},
		},
		{
			name: "resume",
			args: []string{"--cd", "/fixture", "exec", "--sandbox", "workspace-write", "resume", "--json", "--ignore-user-config", "--ignore-rules",
				"--model", "gpt-5.6-sol", "-c", `model_reasoning_effort="low"`,
				"--output-schema", "/fixture/result.schema.json", hermeticThreadID, "-"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var stdout bytes.Buffer
			var calls [][]string
			prompt := controlledPrompt("/fixture/effect", "/fixture/request.json")
			err := run(context.Background(), test.args, strings.NewReader(prompt), &stdout,
				hermeticThreadID, nil, func(_ context.Context, command string, args ...string) error {
					calls = append(calls, append([]string{command}, args...))
					return nil
				})
			if err != nil {
				t.Fatalf("run: %v", err)
			}
			wantCall := [][]string{{"/fixture/effect", "--request", "/fixture/request.json"}}
			if !reflect.DeepEqual(calls, wantCall) {
				t.Fatalf("calls = %v, want %v", calls, wantCall)
			}
			lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
			if len(lines) != 6 {
				t.Fatalf("JSONL lines = %d, want 6\n%s", len(lines), stdout.String())
			}
			if lines[0] != `{"thread_id":"`+hermeticThreadID+`","type":"thread.started"}` ||
				!strings.Contains(lines[2], `"type":"command_execution"`) ||
				!strings.Contains(lines[4], `"text":"{\"status\":\"EFFECT_COMPLETE\"}"`) ||
				!strings.Contains(lines[5], `"type":"turn.completed"`) {
				t.Fatalf("unexpected JSONL:\n%s", stdout.String())
			}
		})
	}
}

func TestRunReportsControlledEffectFailure(t *testing.T) {
	sentinel := errors.New("effect failed")
	var stdout bytes.Buffer
	err := run(context.Background(), []string{
		"--cd", "/fixture", "exec", "--json", "--model", "gpt-5.6-sol",
		"--sandbox", "workspace-write", "--output-schema", "/fixture/result.schema.json", "-",
	}, strings.NewReader(controlledPrompt("/fixture/effect", "/fixture/request.json")), &stdout,
		hermeticThreadID, nil, func(context.Context, string, ...string) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("run error = %v, want sentinel", err)
	}
	if !strings.Contains(stdout.String(), `"type":"turn.failed"`) || strings.Contains(stdout.String(), `"type":"turn.completed"`) {
		t.Fatalf("failure JSONL = %s", stdout.String())
	}
}

func TestRunRejectsAmbiguousInvocationAndPrompt(t *testing.T) {
	validArgs := []string{
		"--cd", "/fixture", "exec", "--json", "--model", "gpt-5.6-sol",
		"--sandbox", "workspace-write", "--output-schema", "/fixture/result.schema.json", "-",
	}
	tests := []struct {
		name   string
		args   []string
		prompt string
		thread string
	}{
		{"last", []string{"exec", "resume", "--last", "effect"}, controlledPrompt("/fixture/effect", "/fixture/request.json"), hermeticThreadID},
		{"wrong resume", []string{"exec", "resume", "019ff302-7730-7f21-90ed-73c37fb4e8fb", "effect"}, controlledPrompt("/fixture/effect", "/fixture/request.json"), hermeticThreadID},
		{"missing json", []string{"exec", "--output-schema", "/fixture/result.schema.json", "-"}, controlledPrompt("/fixture/effect", "/fixture/request.json"), hermeticThreadID},
		{"bad thread", validArgs, controlledPrompt("/fixture/effect", "/fixture/request.json"), "not-a-uuid"},
		{"extra command", validArgs, controlledPrompt("/fixture/effect", "/fixture/request.json") + "extra\n", hermeticThreadID},
		{"unsafe path", validArgs, controlledPrompt("/fixture/effect;bad", "/fixture/request.json"), hermeticThreadID},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := run(context.Background(), test.args, strings.NewReader(test.prompt), &bytes.Buffer{},
				test.thread, nil, func(context.Context, string, ...string) error { return nil }); err == nil {
				t.Fatal("invalid invocation unexpectedly succeeded")
			}
		})
	}
}

func TestRunRejectsResumeIdentityDriftWithOptionsBeforeSubcommand(t *testing.T) {
	base := []string{
		"--cd", "/fixture", "exec", "--sandbox", "workspace-write", "resume",
		"--json", "--ignore-user-config", "--ignore-rules", "--model", "gpt-5.6-sol",
		"-c", `model_reasoning_effort="low"`, "--output-schema", "/fixture/result.schema.json",
	}
	for _, test := range []struct {
		name    string
		session []string
	}{
		{name: "missing"},
		{name: "wrong", session: []string{"019ff302-7730-7f21-90ed-73c37fb4e8fb"}},
		{name: "multiple", session: []string{hermeticThreadID, "019ff302-7730-7f21-90ed-73c37fb4e8fb"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			args := append(append([]string(nil), base...), test.session...)
			args = append(args, "-")
			if err := run(context.Background(), args,
				strings.NewReader(controlledPrompt("/fixture/effect", "/fixture/request.json")),
				&bytes.Buffer{}, hermeticThreadID, nil,
				func(context.Context, string, ...string) error { return nil }); err == nil {
				t.Fatal("resume identity drift unexpectedly executed")
			}
		})
	}
}

func TestRunWaitsForThreadRegistrationBeforeEffect(t *testing.T) {
	reader, writer := io.Pipe()
	defer reader.Close()
	var stdout bytes.Buffer
	threadWritten := make(chan struct{})
	output := io.MultiWriter(&stdout, &threadEventWriter{written: threadWritten})
	effectStarted := make(chan struct{}, 1)
	finished := make(chan error, 1)
	go func() {
		finished <- run(context.Background(), []string{
			"--cd", "/fixture", "exec", "--json", "--model", "gpt-5.6-sol",
			"--sandbox", "workspace-write", "--output-schema", "/fixture/result.schema.json", "-",
		}, strings.NewReader(controlledPrompt("/fixture/effect", "/fixture/request.json")), output,
			hermeticThreadID, reader, func(context.Context, string, ...string) error {
				effectStarted <- struct{}{}
				return nil
			})
	}()

	<-threadWritten
	select {
	case <-effectStarted:
		t.Fatal("effect started before durable thread registration release")
	default:
	}
	if _, err := writer.Write([]byte{1}); err != nil {
		t.Fatalf("release thread gate: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close thread gate: %v", err)
	}
	if err := <-finished; err != nil {
		t.Fatalf("run: %v", err)
	}
	select {
	case <-effectStarted:
	default:
		t.Fatal("effect did not start after durable thread registration release")
	}
}

type threadEventWriter struct {
	once    sync.Once
	written chan struct{}
}

func (w *threadEventWriter) Write(value []byte) (int, error) {
	if bytes.Contains(value, []byte(`"type":"thread.started"`)) {
		w.once.Do(func() { close(w.written) })
	}
	return len(value), nil
}

func controlledPrompt(command, request string) string {
	return lab.ControlledEffectPrompt(command+" --request "+request) + "\n"
}
