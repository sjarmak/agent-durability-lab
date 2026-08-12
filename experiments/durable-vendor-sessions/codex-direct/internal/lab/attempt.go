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

	"golang.org/x/sys/unix"
)

const effectRequestFile = "effect-request.json"

type AttemptInput struct {
	Directory              string
	EffectBinary           string
	DestinationPath        string
	WorkspacePath          string
	ThreadReceiptPath      string
	CanonicalThreadPath    string
	EnforceThreadAuthority bool
	SupervisorURL          string
	AuthorityStorePath     string
	Generation             uint64
	OwnerCapability        string
	Payload                string
	BarrierURL             string
	BarrierDirectory       string
	BarrierPoint           string
	LogicalSessionID       string
	LogicalTurnID          string
	LogicalEffectID        string
	PhysicalAttemptID      string
	ActorID                string
}

type PreparedAttempt struct {
	RequestPath string
	Command     string
	Prompt      string
}

func PrepareAttempt(ctx context.Context, input AttemptInput) (PreparedAttempt, error) {
	if err := ctx.Err(); err != nil {
		return PreparedAttempt{}, err
	}
	if !input.valid() {
		return PreparedAttempt{}, errors.New("attempt requires safe paths and complete effect identities")
	}
	if err := ensureRealAttemptDirectory(input.Directory); err != nil {
		return PreparedAttempt{}, fmt.Errorf("create Codex attempt directory: %w", err)
	}
	requestPath := filepath.Join(input.Directory, effectRequestFile)
	request := ControlledEffectInput{
		DestinationPath: input.DestinationPath, WorkspacePath: input.WorkspacePath,
		SupervisorURL: input.SupervisorURL, OwnershipGeneration: input.Generation,
		AuthorityStorePath: input.AuthorityStorePath,
		OwnerCapability:    input.OwnerCapability, Payload: input.Payload,
		BarrierURL: input.BarrierURL, BarrierDirectory: input.BarrierDirectory, BarrierPoint: input.BarrierPoint,
		LogicalSessionID: input.LogicalSessionID, LogicalTurnID: input.LogicalTurnID,
		LogicalEffectID: input.LogicalEffectID, PhysicalAttemptID: input.PhysicalAttemptID, ActorID: input.ActorID,
	}
	if input.EnforceThreadAuthority {
		request.ThreadReceiptPath = input.ThreadReceiptPath
		request.CanonicalThreadPath = input.CanonicalThreadPath
	}
	if !request.valid() {
		return PreparedAttempt{}, errors.New("attempt effect authority is inconsistent")
	}
	if err := writeJSONExclusive(requestPath, request); err != nil {
		return PreparedAttempt{}, err
	}
	command := input.EffectBinary + " --request " + requestPath
	return PreparedAttempt{
		RequestPath: requestPath, Command: command,
		Prompt: ControlledEffectPrompt(command),
	}, nil
}

func ControlledEffectPrompt(command string) string {
	return "You must use the shell execution tool now. Execute this exact command once, without interpreting or simulating it:\n" + command +
		"\nThe final JSON is a report of the command result, not permission to skip tool use." +
		" Do not emit the final JSON before the command exits with status 0." +
		" If the command cannot run or fails, do not claim EFFECT_COMPLETE." +
		" After exit status 0, return only the required structured status."
}

func ReadControlledEffectRequest(path string) (input ControlledEffectInput, returnErr error) {
	fd, err := unix.Openat2(unix.AT_FDCWD, path, &unix.OpenHow{
		Flags:   unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW,
		Resolve: unix.RESOLVE_NO_SYMLINKS,
	})
	if err != nil {
		return input, fmt.Errorf("open controlled-effect request without symlinks: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return input, errors.New("open controlled-effect request returned an invalid descriptor")
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	info, err := file.Stat()
	if err != nil {
		return input, fmt.Errorf("inspect controlled-effect request: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() > 64<<10 {
		return input, errors.New("controlled-effect request is not a bounded regular file")
	}
	decoder := json.NewDecoder(io.LimitReader(file, 64<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		return input, fmt.Errorf("decode controlled-effect request: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return ControlledEffectInput{}, errors.New("controlled-effect request contains trailing data")
	}
	if !input.valid() {
		return ControlledEffectInput{}, errors.New("controlled-effect request is incomplete")
	}
	return input, nil
}

func ensureRealAttemptDirectory(path string) error {
	if err := os.MkdirAll(path, 0o750); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("attempt directory is not a real directory")
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil || filepath.Clean(canonical) != filepath.Clean(path) {
		return errors.New("attempt directory traverses a symlink")
	}
	return nil
}

func (i AttemptInput) valid() bool {
	return safeCommandPath(i.Directory) && safeCommandPath(i.EffectBinary) &&
		safeCommandPath(i.WorkspacePath) && safeCommandPath(i.ThreadReceiptPath) &&
		filepath.Dir(i.ThreadReceiptPath) == filepath.Clean(i.Directory) &&
		(!i.EnforceThreadAuthority || safeCommandPath(i.CanonicalThreadPath)) &&
		i.Payload != "" && (i.BarrierURL != "" || safeCommandPath(i.BarrierDirectory)) &&
		!(i.BarrierURL != "" && i.BarrierDirectory != "") && i.BarrierPoint != "" &&
		i.LogicalSessionID != "" && i.LogicalTurnID != "" && i.LogicalEffectID != "" &&
		i.PhysicalAttemptID != "" && i.ActorID != "" &&
		((safeCommandPath(i.DestinationPath) && i.SupervisorURL == "" && i.Generation == 0 && i.OwnerCapability == "") ||
			(i.DestinationPath == "" && (i.SupervisorURL != "" || safeCommandPath(i.AuthorityStorePath)) &&
				!(i.SupervisorURL != "" && i.AuthorityStorePath != "") && i.Generation > 0 && i.OwnerCapability != ""))
}

func safeCommandPath(value string) bool {
	if value == "" || !filepath.IsAbs(value) || filepath.Clean(value) != value || strings.Contains(value, `\`) {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || strings.ContainsRune("/._:-", character) {
			continue
		}
		return false
	}
	return true
}
