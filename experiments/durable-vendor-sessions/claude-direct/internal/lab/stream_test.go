package lab

import (
	"strings"
	"testing"
)

func TestParseClaudeStreamCapturesOneSessionAndTerminalResult(t *testing.T) {
	t.Parallel()

	input := strings.Join([]string{
		`{"type":"system","subtype":"init","session_id":"vendor-session-1","model":"claude-haiku"}`,
		`{"type":"assistant","session_id":"vendor-session-1","message":{"content":[]}}`,
		`{"type":"result","subtype":"success","session_id":"vendor-session-1","is_error":false,"result":"EFFECT_COMPLETE.","structured_output":{"status":"EFFECT_COMPLETE"}}`,
	}, "\n")
	result, err := ParseClaudeStream(strings.NewReader(input))
	if err != nil {
		t.Fatalf("parse stream: %v", err)
	}
	if result.SessionID != "vendor-session-1" || result.Result != "EFFECT_COMPLETE" || result.IsError {
		t.Fatalf("result = %+v", result)
	}
	if len(result.Events) != 3 {
		t.Fatalf("events = %d, want 3", len(result.Events))
	}
}

func TestParseClaudeStreamRejectsMissingResultAndSessionDrift(t *testing.T) {
	t.Parallel()

	for _, input := range []string{
		`{"type":"system","subtype":"init","session_id":"vendor-session-1"}`,
		strings.Join([]string{
			`{"type":"system","subtype":"init","session_id":"vendor-session-1"}`,
			`{"type":"result","subtype":"success","session_id":"vendor-session-2","is_error":false,"structured_output":{"status":"EFFECT_COMPLETE"}}`,
		}, "\n"),
		strings.Join([]string{
			`{"type":"system","subtype":"init","session_id":"vendor-session-1"}`,
			`{"type":"result","subtype":"success","session_id":"vendor-session-1","is_error":false,"result":"EFFECT_COMPLETE"}`,
		}, "\n"),
		strings.Join([]string{
			`{"type":"system","subtype":"init","session_id":"vendor-session-1"}`,
			`{"type":"result","subtype":"success","session_id":"vendor-session-1","is_error":false,"structured_output":{"status":"unexpected"}}`,
		}, "\n"),
		strings.Join([]string{
			`{"type":"system","subtype":"init","session_id":"vendor-session-1"}`,
			`{"type":"result","subtype":"success","session_id":"vendor-session-1","is_error":true,"structured_output":{"status":"EFFECT_COMPLETE"}}`,
		}, "\n"),
		strings.Join([]string{
			`{"type":"system","subtype":"init","session_id":"vendor-session-1"}`,
			`{"type":"result","subtype":"error_max_turns","session_id":"vendor-session-1","is_error":false,"structured_output":{"status":"EFFECT_COMPLETE"}}`,
		}, "\n"),
	} {
		if _, err := ParseClaudeStream(strings.NewReader(input)); err == nil {
			t.Fatalf("input %q returned nil error", input)
		}
	}
}
