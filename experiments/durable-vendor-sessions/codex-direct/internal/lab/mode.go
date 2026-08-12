package lab

import "fmt"

type RecoveryMode string

const (
	RecoveryModeUnsafeFresh RecoveryMode = "unsafe-fresh"
	RecoveryModeResumeOnly  RecoveryMode = "explicit-thread-resume"
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

func (m RecoveryMode) usesCanonicalThread() bool {
	return m.normalized() == RecoveryModeResumeOnly || m.normalized() == RecoveryModeFenced
}

func ParseRecoveryMode(value string) (RecoveryMode, error) {
	mode := RecoveryMode(value)
	if value == "" || !mode.valid() {
		return "", fmt.Errorf("unsupported Codex recovery mode %q", value)
	}
	return mode, nil
}
