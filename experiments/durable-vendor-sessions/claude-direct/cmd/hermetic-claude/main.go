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
	"strings"
	"syscall"
	"unicode"
)

const (
	hermeticClaudeVersion = "hermetic-claude 1.0"
	promptInstruction     = "Use the Bash tool exactly once to run this exact command and no other command:"
	promptCompletion      = "After it succeeds, reply with EFFECT_COMPLETE."
)

type executeCommand func(context.Context, string, ...string) error

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	err := run(ctx, os.Args[1:], os.Stdin, os.Stdout, func(ctx context.Context, command string, args ...string) error {
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

func run(ctx context.Context, args []string, stdin io.Reader, stdout io.Writer, execute executeCommand) error {
	if len(args) == 1 && args[0] == "--version" {
		_, err := fmt.Fprintln(stdout, hermeticClaudeVersion)
		return err
	}
	if execute == nil {
		return errors.New("controlled effect executor is required")
	}
	sessionID, err := selectedSessionID(args)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(stdin)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	if !scanner.Scan() || scanner.Text() != promptInstruction {
		return errors.New("exact controlled effect instruction is required")
	}
	if !scanner.Scan() {
		return errors.New("controlled effect command line is required")
	}
	command, commandArgs, err := parseControlledEffectCommand(scanner.Text())
	if err != nil {
		return err
	}
	if !scanner.Scan() || scanner.Text() != promptCompletion {
		return errors.New("exact controlled effect completion instruction is required")
	}
	if scanner.Scan() {
		return errors.New("unexpected input after controlled effect command")
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read prompt: %w", err)
	}
	encoder := json.NewEncoder(stdout)
	if err := encoder.Encode(map[string]any{
		"type": "system", "subtype": "init", "session_id": sessionID,
	}); err != nil {
		return fmt.Errorf("write session event: %w", err)
	}
	if err := execute(ctx, command, commandArgs...); err != nil {
		return fmt.Errorf("run controlled effect: %w", err)
	}
	if err := encoder.Encode(map[string]any{
		"type": "result", "subtype": "success", "session_id": sessionID, "is_error": false,
		"structured_output": map[string]string{"status": "EFFECT_COMPLETE"},
	}); err != nil {
		return fmt.Errorf("write result event: %w", err)
	}
	return nil
}

func selectedSessionID(args []string) (string, error) {
	var selected string
	for index := 0; index < len(args); index++ {
		argument := args[index]
		if argument != "--session-id" && argument != "--resume" {
			continue
		}
		if selected != "" || index+1 >= len(args) {
			return "", errors.New("exactly one complete session selection is required")
		}
		selected = args[index+1]
		index++
	}
	if !validSessionID(selected) {
		return "", errors.New("exactly one valid session selection is required")
	}
	return selected, nil
}

func validSessionID(value string) bool {
	if len(value) != 36 || value != strings.ToLower(value) ||
		value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	decoded, err := hex.DecodeString(strings.ReplaceAll(value, "-", ""))
	return err == nil && len(decoded) == 16
}

func parseControlledEffectCommand(line string) (string, []string, error) {
	fields := strings.Fields(line)
	if len(fields) != 3 || fields[1] != "--request" || !safeAbsolutePath(fields[0]) || !safeAbsolutePath(fields[2]) {
		return "", nil, errors.New("controlled effect command must be an absolute executable and one absolute --request path")
	}
	return fields[0], fields[1:], nil
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
