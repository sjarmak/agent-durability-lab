package lab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const effectRequestFile = "effect-request.json"

type AttemptInput struct {
	Directory         string
	EffectBinary      string
	DestinationPath   string
	WorkspacePath     string
	Payload           string
	BarrierURL        string
	BarrierPoint      string
	LogicalSessionID  string
	LogicalTurnID     string
	LogicalEffectID   string
	PhysicalAttemptID string
	ActorID           string
}

type PreparedAttempt struct {
	RequestPath string
	Command     string
	AllowedTool string
	Prompt      string
}

func PrepareAttempt(ctx context.Context, input AttemptInput) (PreparedAttempt, error) {
	if err := ctx.Err(); err != nil {
		return PreparedAttempt{}, err
	}
	if !input.valid() {
		return PreparedAttempt{}, errors.New("attempt requires safe paths and complete effect identities")
	}
	if err := os.MkdirAll(input.Directory, 0o750); err != nil {
		return PreparedAttempt{}, fmt.Errorf("create attempt directory: %w", err)
	}
	requestPath := filepath.Join(input.Directory, effectRequestFile)
	request := ControlledEffectInput{
		DestinationPath: input.DestinationPath, BarrierURL: input.BarrierURL,
		WorkspacePath: input.WorkspacePath, Payload: input.Payload,
		BarrierPoint: input.BarrierPoint, LogicalSessionID: input.LogicalSessionID,
		LogicalTurnID: input.LogicalTurnID, LogicalEffectID: input.LogicalEffectID,
		PhysicalAttemptID: input.PhysicalAttemptID, ActorID: input.ActorID,
	}
	if err := writeJSONExclusive(requestPath, request); err != nil {
		return PreparedAttempt{}, err
	}
	command := input.EffectBinary + " --request " + requestPath
	return PreparedAttempt{
		RequestPath: requestPath,
		Command:     command,
		AllowedTool: "Bash(" + command + ")",
		Prompt: "Use the Bash tool exactly once to run this exact command and no other command:\n" +
			command + "\nAfter it succeeds, reply with EFFECT_COMPLETE.",
	}, nil
}

func ReadControlledEffectRequest(path string) (input ControlledEffectInput, returnErr error) {
	info, err := os.Stat(path)
	if err != nil {
		return ControlledEffectInput{}, fmt.Errorf("inspect controlled effect request: %w", err)
	}
	if info.Size() > 64<<10 {
		return ControlledEffectInput{}, errors.New("controlled effect request exceeds 64 KiB")
	}
	file, err := os.Open(path)
	if err != nil {
		return ControlledEffectInput{}, fmt.Errorf("open controlled effect request: %w", err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, file.Close())
	}()
	decoder := json.NewDecoder(io.LimitReader(file, 64<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		return ControlledEffectInput{}, fmt.Errorf("decode controlled effect request: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return ControlledEffectInput{}, errors.New("controlled effect request contains trailing data")
	}
	if !input.valid() {
		return ControlledEffectInput{}, errors.New("controlled effect request is incomplete")
	}
	return input, nil
}

func (i AttemptInput) valid() bool {
	return safeCommandPath(i.Directory) && safeCommandPath(i.EffectBinary) &&
		safeCommandPath(i.DestinationPath) && safeCommandPath(i.WorkspacePath) && i.Payload != "" &&
		i.BarrierURL != "" && i.BarrierPoint != "" &&
		i.LogicalSessionID != "" && i.LogicalTurnID != "" && i.LogicalEffectID != "" &&
		i.PhysicalAttemptID != "" && i.ActorID != ""
}

func safeCommandPath(value string) bool {
	if value == "" || !filepath.IsAbs(value) || strings.Contains(value, "..") {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') && (character < 'A' || character > 'Z') &&
			(character < '0' || character > '9') && !strings.ContainsRune("/._:-", character) {
			return false
		}
	}
	return true
}
