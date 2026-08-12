package lab

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/sjarmak/temporal_projects/internal/failureinject"
)

type Invocation struct {
	Binary            string
	Args              []string
	Env               []string
	WorkDir           string
	Stdin             string
	BarrierCredential failureinject.Credential
}

type CodexCommand struct {
	Binary          string
	WorkDir         string
	CodexHome       string
	Model           string
	ReasoningEffort string
	OutputSchema    string
	Sandbox         string
}

func (c CodexCommand) InitialInvocation(prompt string) (Invocation, error) {
	if err := c.validate(prompt); err != nil {
		return Invocation{}, err
	}
	args := append(c.commandPrefix(),
		"--sandbox", c.Sandbox,
		"--output-schema", c.OutputSchema,
		"-",
	)
	return c.invocation(args, prompt), nil
}

func (c CodexCommand) ResumeInvocation(prompt, threadID string) (Invocation, error) {
	if err := c.validate(prompt); err != nil {
		return Invocation{}, err
	}
	if !validThreadID(threadID) {
		return Invocation{}, errors.New("codex resume requires a canonical thread UUID")
	}
	args := []string{
		"--cd", c.WorkDir,
		"exec", "--sandbox", c.Sandbox, "resume",
		"--json", "--ignore-user-config", "--ignore-rules",
		"--model", c.Model,
		"-c", fmt.Sprintf("model_reasoning_effort=%q", c.ReasoningEffort),
		"--output-schema", c.OutputSchema,
		threadID, "-",
	}
	return c.invocation(args, prompt), nil
}

func (c CodexCommand) commandPrefix() []string {
	return []string{
		"--cd", c.WorkDir,
		"exec",
		"--json", "--ignore-user-config", "--ignore-rules",
		"--model", c.Model,
		"-c", fmt.Sprintf("model_reasoning_effort=%q", c.ReasoningEffort),
	}
}

func (c CodexCommand) invocation(args []string, prompt string) Invocation {
	return Invocation{
		Binary: c.Binary, Args: args, Env: []string{"CODEX_HOME=" + c.CodexHome},
		WorkDir: c.WorkDir, Stdin: strings.TrimRight(prompt, "\n") + "\n",
	}
}

func (c CodexCommand) validate(prompt string) error {
	if strings.TrimSpace(c.Binary) == "" || strings.TrimSpace(c.WorkDir) == "" ||
		strings.TrimSpace(c.CodexHome) == "" || strings.TrimSpace(c.Model) == "" ||
		strings.TrimSpace(c.OutputSchema) == "" || strings.TrimSpace(prompt) == "" {
		return errors.New("codex invocation requires binary, work directory, profile, model, schema, and prompt")
	}
	switch c.ReasoningEffort {
	case "low", "medium", "high", "xhigh":
	default:
		return fmt.Errorf("unsupported Codex reasoning effort %q", c.ReasoningEffort)
	}
	if c.Sandbox != "workspace-write" {
		return fmt.Errorf("unsupported Codex experiment sandbox %q", c.Sandbox)
	}
	return nil
}

func validThreadID(value string) bool {
	if len(value) != 36 || value != strings.ToLower(value) ||
		value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	decoded, err := hex.DecodeString(strings.ReplaceAll(value, "-", ""))
	return err == nil && len(decoded) == 16
}
