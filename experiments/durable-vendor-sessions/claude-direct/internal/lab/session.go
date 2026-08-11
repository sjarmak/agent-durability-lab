package lab

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
)

type RecoveryMode string

const (
	RecoveryModeUnsafeFresh RecoveryMode = "unsafe-fresh"
	RecoveryModeResumeOnly  RecoveryMode = "resume-only"
	RecoveryModeFenced      RecoveryMode = "fenced-start-or-attach"
)

func (m RecoveryMode) normalized() RecoveryMode {
	if m == "" {
		return RecoveryModeUnsafeFresh
	}
	return m
}

func (m RecoveryMode) valid() bool {
	switch m.normalized() {
	case RecoveryModeUnsafeFresh, RecoveryModeResumeOnly, RecoveryModeFenced:
		return true
	default:
		return false
	}
}

func (m RecoveryMode) usesSelectedSession() bool {
	return m.normalized() == RecoveryModeResumeOnly || m.normalized() == RecoveryModeFenced
}

func ParseRecoveryMode(value string) (RecoveryMode, error) {
	if value == "" {
		return "", fmt.Errorf("unsupported Claude recovery mode %q", value)
	}
	mode := RecoveryMode(value)
	if !mode.valid() {
		return "", fmt.Errorf("unsupported Claude recovery mode %q", value)
	}
	return mode, nil
}

func newVendorSessionID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate Claude session UUID: %w", err)
	}
	value[6] = value[6]&0x0f | 0x40
	value[8] = value[8]&0x3f | 0x80
	encoded := hex.EncodeToString(value[:])
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32], nil
}

func validVendorSessionID(value string) bool {
	if len(value) != 36 || value != strings.ToLower(value) ||
		value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	compact := strings.ReplaceAll(value, "-", "")
	decoded, err := hex.DecodeString(compact)
	return err == nil && len(decoded) == 16
}

func validateRecordedInvocation(process ProcessRecord, mode RecoveryMode, selected string, temporalAttempt int32) error {
	if process.Binary == "" || process.WorkDir == "" || len(process.Args) == 0 || temporalAttempt < 1 || !mode.valid() {
		return fmt.Errorf("recorded Claude invocation is incomplete")
	}
	for _, forbidden := range []string{"--fork-session", "--continue"} {
		if invocationFlagSyntaxCount(process.Args, forbidden) != 0 {
			return fmt.Errorf("recorded Claude invocation contains forbidden flag %s", forbidden)
		}
	}
	if mode.normalized() == RecoveryModeUnsafeFresh {
		if selected != "" || invocationFlagSyntaxCount(process.Args, "--session-id") != 0 ||
			invocationFlagSyntaxCount(process.Args, "--resume") != 0 {
			return fmt.Errorf("recorded unsafe Claude invocation contains session recovery controls")
		}
		return nil
	}
	if !validVendorSessionID(selected) {
		return fmt.Errorf("recorded Claude invocation lacks a selected session UUID")
	}
	wantFlag, otherFlag := "--session-id", "--resume"
	if temporalAttempt > 1 {
		wantFlag, otherFlag = otherFlag, wantFlag
	}
	if invocationFlagSyntaxCount(process.Args, wantFlag) != 1 ||
		invocationFlagCount(process.Args, wantFlag, selected) != 1 ||
		invocationFlagSyntaxCount(process.Args, otherFlag) != 0 {
		return fmt.Errorf("recorded Claude invocation does not match the attempt session strategy")
	}
	return nil
}

func invocationFlagSyntaxCount(args []string, flag string) int {
	count := 0
	for _, argument := range args {
		if argument == flag || strings.HasPrefix(argument, flag+"=") {
			count++
		}
	}
	return count
}

func invocationFlagCount(args []string, flag, value string) int {
	count := 0
	for index, argument := range args {
		if argument != flag {
			continue
		}
		if value == "" || index+1 < len(args) && args[index+1] == value {
			count++
		}
	}
	return count
}
