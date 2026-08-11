package lab

import "testing"

func TestNewVendorSessionIDProducesCanonicalUUID(t *testing.T) {
	t.Parallel()

	first, err := newVendorSessionID()
	if err != nil {
		t.Fatalf("generate first session ID: %v", err)
	}
	second, err := newVendorSessionID()
	if err != nil {
		t.Fatalf("generate second session ID: %v", err)
	}
	if first == second || !validVendorSessionID(first) || !validVendorSessionID(second) {
		t.Fatalf("generated session IDs = %q, %q", first, second)
	}
	if first[14] != '4' || (first[19] != '8' && first[19] != '9' && first[19] != 'a' && first[19] != 'b') {
		t.Fatalf("session ID is not RFC 4122 UUIDv4: %q", first)
	}
}

func TestRecoveryModeValidation(t *testing.T) {
	t.Parallel()

	if RecoveryMode("").normalized() != RecoveryModeUnsafeFresh ||
		!RecoveryModeUnsafeFresh.valid() || !RecoveryModeResumeOnly.valid() || !RecoveryModeFenced.valid() ||
		RecoveryMode("fork").valid() || RecoveryModeUnsafeFresh.usesSelectedSession() ||
		!RecoveryModeResumeOnly.usesSelectedSession() || !RecoveryModeFenced.usesSelectedSession() {
		t.Fatal("recovery mode validation does not preserve the unsafe, resume-only, and fenced arms")
	}
}

func TestParseRecoveryModeRejectsExplicitEmptyValue(t *testing.T) {
	t.Parallel()

	if _, err := ParseRecoveryMode(""); err == nil {
		t.Fatal("explicit empty recovery mode returned nil error")
	}
	if _, err := ParseRecoveryMode("fork"); err == nil {
		t.Fatal("unsupported recovery mode returned nil error")
	}
	for _, want := range []RecoveryMode{RecoveryModeUnsafeFresh, RecoveryModeResumeOnly, RecoveryModeFenced} {
		got, err := ParseRecoveryMode(string(want))
		if err != nil || got != want {
			t.Fatalf("parse recovery mode %q = %q, %v", want, got, err)
		}
	}
}

func TestValidateRecordedSessionInvocationMatchesAttemptStrategy(t *testing.T) {
	t.Parallel()

	const selected = "01890f3e-7b5a-4c2d-8e1f-0123456789ab"
	base := ProcessRecord{Binary: "/opt/claude", WorkDir: "/work", Args: []string{"-p", "--session-id", selected}}
	if err := validateRecordedInvocation(base, RecoveryModeResumeOnly, selected, 1); err != nil {
		t.Fatalf("validate selected-session invocation: %v", err)
	}
	resumed := base
	resumed.Args = []string{"-p", "--resume", selected}
	if err := validateRecordedInvocation(resumed, RecoveryModeResumeOnly, selected, 2); err != nil {
		t.Fatalf("validate resume invocation: %v", err)
	}
	for _, process := range []ProcessRecord{
		{},
		{Binary: "/opt/claude", WorkDir: "/work", Args: []string{"-p", "--resume", "wrong"}},
		{Binary: "/opt/claude", WorkDir: "/work", Args: []string{"-p", "--resume", selected, "--fork-session"}},
		{Binary: "/opt/claude", WorkDir: "/work", Args: []string{"-p", "--resume", selected, "--resume", "wrong"}},
		{Binary: "/opt/claude", WorkDir: "/work", Args: []string{"-p", "--resume=" + selected}},
	} {
		if err := validateRecordedInvocation(process, RecoveryModeResumeOnly, selected, 2); err == nil {
			t.Fatalf("invalid recorded invocation returned nil error: %+v", process)
		}
	}
}
