package lab

import (
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

const testThreadID = "0199a213-81c0-7800-8aa1-bbab2a035a53"

func TestParseCodexStreamReturnsThreadStructuredResultAndUsage(t *testing.T) {
	expectedCommand := "/fixture/effect --request /fixture/request.json"
	stream := strings.Join([]string{
		`{"type":"thread.started","thread_id":"` + testThreadID + `"}`,
		`{"type":"turn.started"}`,
		`{"type":"item.started","item":{"id":"item_1","type":"command_execution","command":"/fixture/effect --request /fixture/request.json","exit_code":null,"status":"in_progress"}}`,
		`{"type":"item.completed","item":{"id":"item_1","type":"command_execution","command":"/fixture/effect --request /fixture/request.json","exit_code":0,"status":"completed"}}`,
		`{"type":"item.completed","item":{"id":"item_2","type":"agent_message","text":"{\"status\":\"EFFECT_COMPLETE\"}"}}`,
		`{"type":"turn.completed","usage":{"input_tokens":120,"cached_input_tokens":80,"output_tokens":12,"reasoning_output_tokens":4}}`,
	}, "\n") + "\n"
	var observed []string
	result, err := ParseCodexStream(strings.NewReader(stream), StreamHooks{
		ExpectedCommand: expectedCommand,
		ThreadStarted: func(threadID string) error {
			observed = append(observed, threadID)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("parse stream: %v", err)
	}
	if result.ThreadID != testThreadID || result.Result != "EFFECT_COMPLETE" || len(result.Events) != 6 {
		t.Fatalf("result = %+v", result)
	}
	if !reflect.DeepEqual(observed, []string{testThreadID}) {
		t.Fatalf("observed thread IDs = %v", observed)
	}
	wantUsage := TokenUsage{Input: 120, CachedInput: 80, Output: 12, ReasoningOutput: 4}
	if result.Usage != wantUsage {
		t.Fatalf("usage = %+v, want %+v", result.Usage, wantUsage)
	}
}

func TestParseCodexStreamStopsAtExactThreadHook(t *testing.T) {
	sentinel := errors.New("held at thread registration")
	stream := `{"type":"thread.started","thread_id":"` + testThreadID + `"}` + "\n" +
		`{"type":"turn.completed","usage":{}}` + "\n"
	result, err := ParseCodexStream(strings.NewReader(stream), StreamHooks{
		ExpectedCommand: "/fixture/effect --request /fixture/request.json",
		ThreadStarted:   func(string) error { return sentinel },
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("parse error = %v, want sentinel", err)
	}
	if len(result.Events) != 1 {
		t.Fatalf("events consumed after held thread boundary = %d, want 1", len(result.Events))
	}
}

func TestParseCodexStreamSelectsOnlyPostCommandAgentMessage(t *testing.T) {
	expectedCommand := "/fixture/effect --request /fixture/request.json"
	stream := strings.Join([]string{
		`{"type":"thread.started","thread_id":"` + testThreadID + `"}`,
		`{"type":"turn.started"}`,
		`{"type":"item.completed","item":{"id":"item_0","type":"agent_message","text":"{\"status\":\"EFFECT_COMPLETE\"}"}}`,
		`{"type":"item.started","item":{"id":"item_1","type":"command_execution","command":"/bin/bash -lc '/fixture/effect --request /fixture/request.json'","exit_code":null,"status":"in_progress"}}`,
		`{"type":"item.completed","item":{"id":"item_1","type":"command_execution","command":"/bin/bash -lc '/fixture/effect --request /fixture/request.json'","exit_code":0,"status":"completed"}}`,
		`{"type":"item.completed","item":{"id":"item_2","type":"agent_message","text":"{\"status\":\"EFFECT_COMPLETE\"}"}}`,
		`{"type":"turn.completed","usage":{}}`,
	}, "\n") + "\n"
	result, err := ParseCodexStream(strings.NewReader(stream), StreamHooks{ExpectedCommand: expectedCommand})
	if err != nil {
		t.Fatal(err)
	}
	if result.Result != "EFFECT_COMPLETE" || result.AgentMessageCount != 2 {
		t.Fatalf("last-message result = %+v", result)
	}
}

func TestParseCodexStreamFailsClosed(t *testing.T) {
	expectedCommand := "/fixture/effect --request /fixture/request.json"
	validThread := `{"type":"thread.started","thread_id":"` + testThreadID + `"}`
	validStart := `{"type":"turn.started"}`
	validCommandStart := `{"type":"item.started","item":{"id":"item_1","type":"command_execution","command":"/bin/bash -c '/fixture/effect --request /fixture/request.json'","exit_code":null,"status":"in_progress"}}`
	validCommandDone := `{"type":"item.completed","item":{"id":"item_1","type":"command_execution","command":"/bin/bash -c '/fixture/effect --request /fixture/request.json'","exit_code":0,"status":"completed"}}`
	validMessage := `{"type":"item.completed","item":{"id":"item_2","type":"agent_message","text":"{\"status\":\"EFFECT_COMPLETE\"}"}}`
	validDone := `{"type":"turn.completed","usage":{}}`
	validStream := []string{validThread, validStart, validCommandStart, validCommandDone, validMessage, validDone}
	tests := []struct {
		name   string
		stream string
	}{
		{"empty", ""},
		{"invalid JSON", "{\n"},
		{"missing thread", strings.Join(validStream[1:], "\n")},
		{"changed thread", strings.Join(append([]string{validThread, `{"type":"thread.started","thread_id":"0199a213-81c0-7800-8aa1-bbab2a035a54"}`}, validStream[1:]...), "\n")},
		{"missing turn start", strings.Join([]string{validThread, validMessage, validDone}, "\n")},
		{"failed turn", strings.Join([]string{validThread, validStart, `{"type":"turn.failed","error":{"message":"provider failed"}}`}, "\n")},
		{"error event", strings.Join([]string{validThread, validStart, `{"type":"error","message":"stream failed"}`}, "\n")},
		{"missing message", strings.Join([]string{validThread, validStart, validDone}, "\n")},
		{"result without command", strings.Join([]string{validThread, validStart, validMessage, validDone}, "\n")},
		{"result before command only", strings.Join([]string{validThread, validStart, validMessage, validCommandStart, validCommandDone, validDone}, "\n")},
		{"wrong command", strings.Join([]string{validThread, validStart, strings.ReplaceAll(validCommandStart, expectedCommand, "/fixture/other --request /fixture/request.json"), validCommandDone, validMessage, validDone}, "\n")},
		{"completion without start", strings.Join([]string{validThread, validStart, validCommandDone, validMessage, validDone}, "\n")},
		{"changed command on completion", strings.Join([]string{validThread, validStart, validCommandStart, strings.ReplaceAll(validCommandDone, expectedCommand, "/fixture/other --request /fixture/request.json"), validMessage, validDone}, "\n")},
		{"changed item on completion", strings.Join([]string{validThread, validStart, validCommandStart, strings.Replace(validCommandDone, `"id":"item_1"`, `"id":"other"`, 1), validMessage, validDone}, "\n")},
		{"failed command", strings.Join([]string{validThread, validStart, validCommandStart, strings.Replace(strings.Replace(validCommandDone, `"exit_code":0`, `"exit_code":1`, 1), `"status":"completed"`, `"status":"failed"`, 1), validMessage, validDone}, "\n")},
		{"missing exit code", strings.Join([]string{validThread, validStart, validCommandStart, strings.Replace(validCommandDone, `"exit_code":0,`, "", 1), validMessage, validDone}, "\n")},
		{"duplicate command", strings.Join([]string{validThread, validStart, validCommandStart, validCommandDone, validCommandStart, validCommandDone, validMessage, validDone}, "\n")},
		{"wrong result", strings.Join([]string{validThread, validStart, validCommandStart, validCommandDone, `{"type":"item.completed","item":{"id":"item_2","type":"agent_message","text":"{\"status\":\"WRONG\"}"}}`, validDone}, "\n")},
		{"duplicate completion", strings.Join(append(append([]string(nil), validStream...), validDone), "\n")},
		{"event after completion", strings.Join(append(append([]string(nil), validStream...), `{"type":"item.completed","item":{"id":"late","type":"reasoning","text":"late"}}`), "\n")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ParseCodexStream(strings.NewReader(test.stream), StreamHooks{ExpectedCommand: expectedCommand}); err == nil {
				t.Fatal("invalid stream unexpectedly succeeded")
			}
		})
	}
}

func TestParseCodexStreamDistinguishesInterruptedPrefixFromInvalidCommand(t *testing.T) {
	expectedCommand := "/fixture/effect --request /fixture/request.json"
	prefix := strings.Join([]string{
		`{"type":"thread.started","thread_id":"` + testThreadID + `"}`,
		`{"type":"turn.started"}`,
		`{"type":"item.started","item":{"id":"item_1","type":"command_execution","command":"/bin/bash -c '/fixture/effect --request /fixture/request.json'","exit_code":null,"status":"in_progress"}}`,
	}, "\n") + "\n"
	if _, err := ParseCodexStream(strings.NewReader(prefix), StreamHooks{ExpectedCommand: expectedCommand}); !errors.Is(err, errCodexStreamIncomplete) {
		t.Fatalf("valid interrupted prefix = %v, want incomplete stream", err)
	}
	invalid := strings.ReplaceAll(prefix, expectedCommand, "/fixture/other --request /fixture/request.json")
	if _, err := ParseCodexStream(strings.NewReader(invalid), StreamHooks{ExpectedCommand: expectedCommand}); err == nil || errors.Is(err, errCodexStreamIncomplete) {
		t.Fatalf("wrong-command prefix = %v, want structural rejection", err)
	}
}

func TestParseCodexStreamRejectsAggregateEventAndByteBudgets(t *testing.T) {
	expectedCommand := "/fixture/effect --request /fixture/request.json"
	prefix := `{"type":"thread.started","thread_id":"` + testThreadID + `"}` + "\n" +
		`{"type":"turn.started"}` + "\n"

	var eventHeavy strings.Builder
	eventHeavy.WriteString(prefix)
	for index := 0; index < maxCodexStreamEvents; index++ {
		eventHeavy.WriteString(`{"type":"item.completed","item":{"id":"reason-` + strconv.Itoa(index) +
			`","type":"reasoning","text":"bounded"}}` + "\n")
	}
	if _, err := ParseCodexStream(strings.NewReader(eventHeavy.String()), StreamHooks{ExpectedCommand: expectedCommand}); err == nil ||
		errors.Is(err, errCodexStreamIncomplete) {
		t.Fatalf("event-heavy stream = %v, want budget rejection", err)
	}

	largeText := strings.Repeat("x", maxCodexStreamBytes/8)
	byteHeavy := prefix + strings.Repeat(
		`{"type":"item.completed","item":{"id":"reason","type":"reasoning","text":"`+largeText+`"}}`+"\n", 9)
	if _, err := ParseCodexStream(strings.NewReader(byteHeavy), StreamHooks{ExpectedCommand: expectedCommand}); err == nil ||
		errors.Is(err, errCodexStreamIncomplete) {
		t.Fatalf("byte-heavy stream = %v, want budget rejection", err)
	}
}
