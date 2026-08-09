package lab

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

type ClaudeStreamResult struct {
	SessionID string
	Result    string
	IsError   bool
	Events    []json.RawMessage
}

type claudeStreamEvent struct {
	Type             string          `json:"type"`
	Subtype          string          `json:"subtype"`
	SessionID        string          `json:"session_id"`
	StructuredOutput json.RawMessage `json:"structured_output"`
	IsError          bool            `json:"is_error"`
}

type claudeEffectResult struct {
	Status string `json:"status"`
}

func ParseClaudeStream(reader io.Reader) (ClaudeStreamResult, error) {
	if reader == nil {
		return ClaudeStreamResult{}, errors.New("claude stream reader is required")
	}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64<<10), 4<<20)
	result := ClaudeStreamResult{Events: make([]json.RawMessage, 0)}
	terminalCount := 0
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		var event claudeStreamEvent
		if err := json.Unmarshal(line, &event); err != nil {
			return ClaudeStreamResult{}, fmt.Errorf("decode Claude stream event: %w", err)
		}
		if event.Type == "" {
			return ClaudeStreamResult{}, errors.New("claude stream event lacks type")
		}
		if event.SessionID != "" {
			if result.SessionID != "" && result.SessionID != event.SessionID {
				return ClaudeStreamResult{}, errors.New("claude stream changed session identity")
			}
			result.SessionID = event.SessionID
		}
		result.Events = append(result.Events, json.RawMessage(line))
		if event.Type == "result" {
			terminalCount++
			if event.Subtype != "success" || event.IsError {
				return ClaudeStreamResult{}, fmt.Errorf(
					"claude terminal result is not successful: subtype=%q is_error=%t", event.Subtype, event.IsError,
				)
			}
			parsedResult, err := decodeClaudeEffectResult(event.StructuredOutput)
			if err != nil {
				return ClaudeStreamResult{}, err
			}
			result.Result = parsedResult
			result.IsError = event.IsError
		}
	}
	if err := scanner.Err(); err != nil {
		return ClaudeStreamResult{}, fmt.Errorf("read Claude stream: %w", err)
	}
	if result.SessionID == "" || terminalCount != 1 {
		return ClaudeStreamResult{}, errors.New("claude stream requires one session and one terminal result")
	}
	return result, nil
}

func decodeClaudeEffectResult(raw json.RawMessage) (string, error) {
	if len(raw) == 0 {
		return "", errors.New("claude terminal result lacks structured output")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var output claudeEffectResult
	if err := decoder.Decode(&output); err != nil {
		return "", fmt.Errorf("decode Claude structured output: %w", err)
	}
	if output.Status != "EFFECT_COMPLETE" {
		return "", fmt.Errorf("claude structured output status %q is invalid", output.Status)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return "", errors.New("claude structured output contains trailing data")
	}
	return output.Status, nil
}
