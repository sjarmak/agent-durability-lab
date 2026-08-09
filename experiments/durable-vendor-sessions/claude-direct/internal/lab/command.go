package lab

import (
	"errors"
	"math"
	"strconv"
	"strings"
)

const effectResultSchema = `{"type":"object","properties":{"status":{"type":"string","enum":["EFFECT_COMPLETE"]}},"required":["status"],"additionalProperties":false}`

// ClaudeCommand describes the deliberately unsafe one-process-per-delivery arm.
type ClaudeCommand struct {
	Binary       string
	WorkDir      string
	Model        string
	MaxBudgetUSD string
	MaxTurns     int
	AllowedTool  string
}

// Invocation is a validated subprocess request. Args never contain a resume,
// caller-selected session, or background-session control in the unsafe arm.
type Invocation struct {
	Binary  string
	Args    []string
	Env     []string
	WorkDir string
	Stdin   string
}

func (c ClaudeCommand) Invocation(prompt string) (Invocation, error) {
	if strings.TrimSpace(c.Binary) == "" || strings.TrimSpace(c.WorkDir) == "" ||
		strings.TrimSpace(c.Model) == "" || strings.TrimSpace(prompt) == "" || c.MaxTurns < 1 ||
		!strings.HasPrefix(c.AllowedTool, "Bash(") || !strings.HasSuffix(c.AllowedTool, ")") ||
		strings.ContainsAny(c.AllowedTool, "\r\n") {
		return Invocation{}, errors.New("claude invocation requires binary, work directory, model, prompt, and a positive turn limit")
	}
	budget, err := strconv.ParseFloat(c.MaxBudgetUSD, 64)
	if err != nil || budget <= 0 || math.IsInf(budget, 0) || math.IsNaN(budget) {
		return Invocation{}, errors.New("claude invocation requires a finite positive budget")
	}
	args := []string{
		"-p",
		"--output-format", "stream-json",
		"--verbose",
		"--json-schema", effectResultSchema,
		"--safe-mode",
		"--permission-mode", "dontAsk",
		"--tools", "Bash",
		"--allowedTools", c.AllowedTool,
		"--model", c.Model,
		"--max-turns", strconv.Itoa(c.MaxTurns),
		"--max-budget-usd", c.MaxBudgetUSD,
	}
	return Invocation{
		Binary: c.Binary, Args: args, WorkDir: c.WorkDir, Stdin: strings.TrimRight(prompt, "\n") + "\n",
	}, nil
}
