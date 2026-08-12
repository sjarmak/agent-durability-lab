package lab

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

type TokenUsage struct {
	Input           int64
	CachedInput     int64
	Output          int64
	ReasoningOutput int64
}

type CodexStreamResult struct {
	ThreadID          string
	Result            string
	AgentMessageCount int
	Usage             TokenUsage
	Events            []json.RawMessage
}

type StreamHooks struct {
	ExpectedCommand string
	ProcessStarted  func(ProcessRecord) error
	ThreadStarted   func(string) error
}

type codexStreamEvent struct {
	Type     string `json:"type"`
	ThreadID string `json:"thread_id"`
	Message  string `json:"message"`
	Error    struct {
		Message string `json:"message"`
	} `json:"error"`
	Item struct {
		ID       string `json:"id"`
		Type     string `json:"type"`
		Text     string `json:"text"`
		Command  string `json:"command"`
		Status   string `json:"status"`
		ExitCode *int   `json:"exit_code"`
	} `json:"item"`
	Usage struct {
		Input           int64 `json:"input_tokens"`
		CachedInput     int64 `json:"cached_input_tokens"`
		Output          int64 `json:"output_tokens"`
		ReasoningOutput int64 `json:"reasoning_output_tokens"`
	} `json:"usage"`
}

type codexEffectResult struct {
	Status string `json:"status"`
}

const (
	maxCodexStreamBytes  = 16 << 20
	maxCodexStreamEvents = 1024
)

var errCodexStreamIncomplete = errors.New("codex stream is a valid interrupted prefix")

func ParseCodexStream(reader io.Reader, hooks StreamHooks) (CodexStreamResult, error) {
	if reader == nil {
		return CodexStreamResult{}, errors.New("codex stream reader is required")
	}
	result := CodexStreamResult{Events: make([]json.RawMessage, 0)}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64<<10), 4<<20)
	threadCount, turnStartCount, messageCount, completionCount := 0, 0, 0, 0
	commandStartCount, commandCompletionCount, postCommandMessageCount := 0, 0, 0
	commandItemID, commandLine, lastPostCommandMessage := "", "", ""
	totalBytes, eventCount := 0, 0
	for scanner.Scan() {
		eventCount++
		totalBytes += len(scanner.Bytes()) + 1
		if eventCount > maxCodexStreamEvents || totalBytes > maxCodexStreamBytes {
			return result, errors.New("codex stream exceeds aggregate event or byte budget")
		}
		if completionCount != 0 {
			return result, errors.New("codex stream contains an event after turn completion")
		}
		line := append([]byte(nil), scanner.Bytes()...)
		var event codexStreamEvent
		if err := json.Unmarshal(line, &event); err != nil {
			return result, fmt.Errorf("decode Codex stream event: %w", err)
		}
		if event.Type == "" {
			return result, errors.New("codex stream event lacks type")
		}
		result.Events = append(result.Events, json.RawMessage(line))
		switch event.Type {
		case "thread.started":
			threadCount++
			if threadCount != 1 || !validThreadID(event.ThreadID) {
				return result, errors.New("codex stream requires one canonical thread identity")
			}
			result.ThreadID = event.ThreadID
			if hooks.ThreadStarted != nil {
				if err := hooks.ThreadStarted(event.ThreadID); err != nil {
					return result, fmt.Errorf("observe Codex thread start: %w", err)
				}
			}
		case "turn.started":
			turnStartCount++
			if threadCount != 1 || turnStartCount != 1 {
				return result, errors.New("codex turn started outside one observed thread")
			}
		case "item.started":
			if turnStartCount != 1 {
				return result, errors.New("codex item started before the turn")
			}
			if event.Item.Type == "command_execution" {
				commandStartCount++
				if commandStartCount != 1 || commandCompletionCount != 0 || event.Item.ID == "" ||
					event.Item.Status != "in_progress" || event.Item.ExitCode != nil ||
					!matchesPreparedCommand(event.Item.Command, hooks.ExpectedCommand) {
					return result, errors.New("codex stream contains an invalid controlled command start")
				}
				commandItemID, commandLine = event.Item.ID, event.Item.Command
			}
		case "item.completed":
			if turnStartCount != 1 {
				return result, errors.New("codex item completed before the turn")
			}
			switch event.Item.Type {
			case "command_execution":
				commandCompletionCount++
				if commandStartCount != 1 || commandCompletionCount != 1 || event.Item.ID != commandItemID ||
					event.Item.Command != commandLine || event.Item.Status != "completed" ||
					event.Item.ExitCode == nil || *event.Item.ExitCode != 0 {
					return result, errors.New("codex stream contains an invalid controlled command completion")
				}
			case "agent_message":
				messageCount++
				if commandCompletionCount == 1 {
					postCommandMessageCount++
					lastPostCommandMessage = event.Item.Text
				}
			}
		case "turn.completed":
			completionCount++
			if turnStartCount != 1 || commandStartCount != 1 || commandCompletionCount != 1 ||
				postCommandMessageCount < 1 || completionCount != 1 {
				return result, errors.New("codex turn completed without one successful controlled command and post-command result")
			}
			parsed, err := decodeCodexEffectResult(lastPostCommandMessage)
			if err != nil {
				return result, err
			}
			result.Result = parsed
			result.AgentMessageCount = messageCount
			usage := TokenUsage{
				Input: event.Usage.Input, CachedInput: event.Usage.CachedInput,
				Output: event.Usage.Output, ReasoningOutput: event.Usage.ReasoningOutput,
			}
			if usage.Input < 0 || usage.CachedInput < 0 || usage.Output < 0 || usage.ReasoningOutput < 0 {
				return result, errors.New("codex turn contains negative token usage")
			}
			result.Usage = usage
		case "turn.failed":
			return result, fmt.Errorf("codex turn failed: %s", event.Error.Message)
		case "error":
			return result, fmt.Errorf("codex stream error: %s", event.Message)
		}
	}
	if err := scanner.Err(); err != nil {
		return result, fmt.Errorf("read Codex stream: %w", err)
	}
	if threadCount != 1 || turnStartCount != 1 || commandStartCount != 1 || commandCompletionCount != 1 ||
		postCommandMessageCount < 1 || completionCount != 1 {
		return result, fmt.Errorf("%w: requires one thread, turn, successful controlled command, post-command result, and completion",
			errCodexStreamIncomplete)
	}
	return result, nil
}

func matchesPreparedCommand(actual, expected string) bool {
	if expected == "" || actual == "" || strings.ContainsRune(expected, '\'') {
		return false
	}
	return actual == expected || actual == "/bin/bash -c '"+expected+"'" ||
		actual == "/bin/bash -lc '"+expected+"'"
}

func decodeCodexEffectResult(text string) (string, error) {
	decoder := json.NewDecoder(bytes.NewBufferString(text))
	decoder.DisallowUnknownFields()
	var result codexEffectResult
	if err := decoder.Decode(&result); err != nil {
		return "", fmt.Errorf("decode Codex structured output: %w", err)
	}
	if result.Status != "EFFECT_COMPLETE" {
		return "", fmt.Errorf("codex structured output status %q is invalid", result.Status)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return "", errors.New("codex structured output contains trailing data")
	}
	return result.Status, nil
}
