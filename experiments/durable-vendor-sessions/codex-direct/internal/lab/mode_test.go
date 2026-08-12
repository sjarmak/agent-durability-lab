package lab

import "testing"

func TestRecoveryModePreservesThreeMatchedArms(t *testing.T) {
	if RecoveryMode("").normalized() != RecoveryModeUnsafeFresh ||
		!RecoveryModeUnsafeFresh.valid() || !RecoveryModeResumeOnly.valid() || !RecoveryModeFenced.valid() ||
		RecoveryMode("last-thread").valid() || RecoveryModeUnsafeFresh.usesCanonicalThread() ||
		!RecoveryModeResumeOnly.usesCanonicalThread() || !RecoveryModeFenced.usesCanonicalThread() {
		t.Fatal("recovery modes do not preserve unsafe, explicit-thread resume, and fenced arms")
	}
	for _, want := range []RecoveryMode{RecoveryModeUnsafeFresh, RecoveryModeResumeOnly, RecoveryModeFenced} {
		got, err := ParseRecoveryMode(string(want))
		if err != nil || got != want {
			t.Fatalf("parse %q = %q, %v", want, got, err)
		}
	}
	if _, err := ParseRecoveryMode(""); err == nil {
		t.Fatal("explicit empty recovery mode unexpectedly succeeded")
	}
}
