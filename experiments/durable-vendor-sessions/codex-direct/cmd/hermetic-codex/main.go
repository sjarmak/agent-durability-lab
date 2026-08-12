package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"unicode"
)

const (
	hermeticCodexVersion = "hermetic-codex 1.0"
	promptInstruction    = "You must use the shell execution tool now. Execute this exact command once, without interpreting or simulating it:"
	promptCompletion     = "The final JSON is a report of the command result, not permission to skip tool use. " +
		"Do not emit the final JSON before the command exits with status 0. " +
		"If the command cannot run or fails, do not claim EFFECT_COMPLETE. " +
		"After exit status 0, return only the required structured status."
	threadEnvironment = "CODEX_HERMETIC_THREAD_ID"
	threadGateFDEnv   = "CODEX_HERMETIC_THREAD_GATE_FD"
)

type executeCommand func(context.Context, string, ...string) error

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	threadGate, err := openThreadGate(os.Getenv(threadGateFDEnv))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if threadGate != nil {
		defer threadGate.Close()
	}
	err = run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Getenv(threadEnvironment), threadGate,
		func(ctx context.Context, command string, args ...string) error {
			process := exec.CommandContext(ctx, command, args...)
			process.Stdout = io.Discard
			process.Stderr = io.Discard
			return process.Run()
		})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout io.Writer,
	configuredThreadID string, threadGate io.ReadCloser, execute executeCommand,
) error {
	if len(args) == 1 && args[0] == "--version" {
		_, err := fmt.Fprintln(stdout, hermeticCodexVersion)
		return err
	}
	if execute == nil {
		return errors.New("controlled effect executor is required")
	}
	threadID, err := invocationThreadID(args, configuredThreadID)
	if err != nil {
		return err
	}
	command, commandArgs, err := parsePrompt(stdin)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	if err := encoder.Encode(map[string]any{"thread_id": threadID, "type": "thread.started"}); err != nil {
		return fmt.Errorf("write thread event: %w", err)
	}
	if err := waitForThreadGate(ctx, threadGate); err != nil {
		return err
	}
	if err := encoder.Encode(map[string]any{"type": "turn.started"}); err != nil {
		return fmt.Errorf("write turn event: %w", err)
	}
	item := map[string]any{
		"id": "item_1", "type": "command_execution",
		"command": strings.Join(append([]string{command}, commandArgs...), " "),
	}
	item["status"] = "in_progress"
	item["exit_code"] = nil
	if err := encoder.Encode(map[string]any{"item": item, "type": "item.started"}); err != nil {
		return fmt.Errorf("write command start event: %w", err)
	}
	if err := execute(ctx, command, commandArgs...); err != nil {
		_ = encoder.Encode(map[string]any{
			"error": map[string]string{"message": err.Error()}, "type": "turn.failed",
		})
		return fmt.Errorf("run controlled effect: %w", err)
	}
	item["status"] = "completed"
	item["exit_code"] = 0
	if err := encoder.Encode(map[string]any{"item": item, "type": "item.completed"}); err != nil {
		return fmt.Errorf("write command completion event: %w", err)
	}
	if err := encoder.Encode(map[string]any{
		"item": map[string]string{
			"id": "item_2", "type": "agent_message", "text": `{"status":"EFFECT_COMPLETE"}`,
		},
		"type": "item.completed",
	}); err != nil {
		return fmt.Errorf("write result event: %w", err)
	}
	if err := encoder.Encode(map[string]any{
		"type": "turn.completed",
		"usage": map[string]int{
			"input_tokens": 120, "cached_input_tokens": 80,
			"output_tokens": 12, "reasoning_output_tokens": 4,
		},
	}); err != nil {
		return fmt.Errorf("write turn completion event: %w", err)
	}
	return nil
}

func openThreadGate(value string) (*os.File, error) {
	if value == "" {
		return nil, nil
	}
	fd, err := strconv.Atoi(value)
	if err != nil || fd < 3 {
		return nil, errors.New("hermetic Codex thread gate requires an inherited file descriptor")
	}
	gate := os.NewFile(uintptr(fd), "codex-thread-registration-gate")
	if gate == nil {
		return nil, errors.New("open hermetic Codex thread gate")
	}
	return gate, nil
}

func waitForThreadGate(ctx context.Context, gate io.ReadCloser) error {
	if gate == nil {
		return nil
	}
	waited := make(chan error, 1)
	go func() {
		var release [1]byte
		_, err := io.ReadFull(gate, release[:])
		if err == nil && release[0] != 1 {
			err = errors.New("invalid hermetic Codex thread gate release")
		}
		waited <- err
	}()
	select {
	case err := <-waited:
		if err != nil {
			return fmt.Errorf("wait for durable thread registration: %w", err)
		}
		return nil
	case <-ctx.Done():
		closeErr := gate.Close()
		return errors.Join(ctx.Err(), closeErr, <-waited)
	}
}

func invocationThreadID(args []string, configured string) (string, error) {
	if !validThreadID(configured) {
		return "", errors.New("hermetic Codex requires a canonical configured thread UUID")
	}
	if !containsArg(args, "exec") || !containsArg(args, "--json") ||
		!flagHasSafePath(args, "--output-schema") {
		return "", errors.New("hermetic Codex requires exec JSONL output and a safe output schema")
	}
	for _, forbidden := range []string{
		"--last", "--all", "--ephemeral", "danger-full-access", "--dangerously-bypass-approvals-and-sandbox",
	} {
		if containsArg(args, forbidden) {
			return "", fmt.Errorf("hermetic Codex rejects ambiguous or unsafe argument %q", forbidden)
		}
	}
	if !flagHasValue(args, "--sandbox", "workspace-write") {
		return "", errors.New("hermetic Codex invocation requires workspace-write sandbox")
	}
	resumeCount := 0
	for _, argument := range args {
		if argument == "resume" {
			resumeCount++
		}
	}
	if resumeCount == 0 {
		return configured, nil
	}
	count := 0
	for _, argument := range args {
		if validThreadID(argument) {
			if argument != configured {
				return "", errors.New("resume thread does not match the configured thread")
			}
			count++
		}
	}
	if resumeCount != 1 || count != 1 {
		return "", errors.New("resume requires exactly one explicit configured thread")
	}
	return configured, nil
}

func parsePrompt(reader io.Reader) (string, []string, error) {
	if reader == nil {
		return "", nil, errors.New("prompt reader is required")
	}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	if !scanner.Scan() || scanner.Text() != promptInstruction {
		return "", nil, errors.New("exact controlled effect instruction is required")
	}
	if !scanner.Scan() {
		return "", nil, errors.New("controlled effect command line is required")
	}
	fields := strings.Fields(scanner.Text())
	if len(fields) != 3 || fields[1] != "--request" ||
		!safeAbsolutePath(fields[0]) || !safeAbsolutePath(fields[2]) {
		return "", nil, errors.New("controlled effect command must be an absolute executable and one absolute --request path")
	}
	if !scanner.Scan() || scanner.Text() != promptCompletion {
		return "", nil, errors.New("exact controlled effect completion instruction is required")
	}
	if scanner.Scan() {
		return "", nil, errors.New("unexpected input after controlled effect command")
	}
	if err := scanner.Err(); err != nil {
		return "", nil, fmt.Errorf("read prompt: %w", err)
	}
	return fields[0], fields[1:], nil
}

func validThreadID(value string) bool {
	if len(value) != 36 || value != strings.ToLower(value) ||
		value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	decoded, err := hex.DecodeString(strings.ReplaceAll(value, "-", ""))
	return err == nil && len(decoded) == 16
}

func safeAbsolutePath(path string) bool {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.Contains(path, `\`) {
		return false
	}
	for _, character := range path {
		if unicode.IsLetter(character) || unicode.IsDigit(character) || strings.ContainsRune("/._-", character) {
			continue
		}
		return false
	}
	return true
}

func containsArg(args []string, target string) bool {
	for _, argument := range args {
		if argument == target {
			return true
		}
	}
	return false
}

func flagHasSafePath(args []string, flag string) bool {
	for index, argument := range args {
		if argument == flag && index+1 < len(args) {
			return safeAbsolutePath(args[index+1])
		}
	}
	return false
}

func flagHasValue(args []string, flag, value string) bool {
	for index, argument := range args {
		if argument == flag && index+1 < len(args) && args[index+1] == value {
			return true
		}
	}
	return false
}
