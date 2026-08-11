package lab

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

func TestEvidenceCaptureRejectsIncompleteAndUnbracketedTrials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*EvidenceCapture)
	}{
		{name: "unsupported probe", mutate: func(c *EvidenceCapture) { c.Probe = protocol.ProbeProtected }},
		{name: "missing version", mutate: func(c *EvidenceCapture) { c.ClaudeVersion = "" }},
		{name: "wrong attempt count", mutate: func(c *EvidenceCapture) { c.Attempts = c.Attempts[:1] }},
		{name: "duplicate physical identity", mutate: func(c *EvidenceCapture) { c.Attempts[1].PhysicalAttemptID = c.Attempts[0].PhysicalAttemptID }},
		{name: "attempt before capture", mutate: func(c *EvidenceCapture) { c.Attempts[0].StartedAt = c.StartedAt.Add(-time.Second) }},
		{name: "completion before effect", mutate: func(c *EvidenceCapture) { c.CompletedAt = c.Attempts[1].AppliedAt }},
		{name: "fault before first effect", mutate: func(c *EvidenceCapture) { c.FaultAt = c.Attempts[0].StartedAt }},
		{name: "invalid native record", mutate: func(c *EvidenceCapture) { c.Native[0].Detail = "" }},
		{name: "unfaulted capture retains fault", mutate: func(c *EvidenceCapture) {
			c.Probe = protocol.ProbeUnfaulted
			c.Attempts = c.Attempts[:1]
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			capture := unsafeCapture()
			test.mutate(&capture)
			if _, err := BuildEvidenceBundle(capture); err == nil {
				t.Fatal("invalid capture returned nil error")
			}
		})
	}
}

func TestExclusiveFilesRequestsAndWorkspaceRejectAmbiguousInput(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	path := filepath.Join(directory, "exclusive.json")
	if err := writeBytesExclusive(path, []byte("first")); err != nil {
		t.Fatalf("write exclusive file: %v", err)
	}
	if err := writeBytesExclusive(path, []byte("second")); err == nil {
		t.Fatal("exclusive overwrite returned nil error")
	}
	oversized := filepath.Join(directory, "oversized.json")
	if err := os.WriteFile(oversized, []byte(strings.Repeat(" ", (64<<10)+1)), 0o600); err != nil {
		t.Fatalf("write oversized request: %v", err)
	}
	if _, err := ReadControlledEffectRequest(oversized); err == nil {
		t.Fatal("oversized request returned nil error")
	}
	unknown := filepath.Join(directory, "unknown.json")
	if err := os.WriteFile(unknown, []byte(`{"unknown":true}`), 0o600); err != nil {
		t.Fatalf("write unknown request: %v", err)
	}
	if _, err := ReadControlledEffectRequest(unknown); err == nil {
		t.Fatal("unknown request field returned nil error")
	}

	workspacePath := filepath.Join(directory, "effects.jsonl")
	if err := AppendWorkspaceEffect(context.Background(), workspacePath, WorkspaceEffect{}); err == nil {
		t.Fatal("incomplete workspace effect returned nil error")
	}
	effect := WorkspaceEffect{
		LogicalEffectID: "effect", PhysicalAttemptID: "physical", Payload: "payload",
		ActorID: "actor", ProcessIdentity: "pid:1:start:one", AppliedAt: time.Now().UTC(),
	}
	encoded, err := json.Marshal(effect)
	if err != nil {
		t.Fatalf("encode workspace effect: %v", err)
	}
	duplicate := append(append(append([]byte(nil), encoded...), '\n'), append(encoded, '\n')...)
	if err := os.WriteFile(workspacePath, duplicate, 0o600); err != nil {
		t.Fatalf("write duplicate workspace effects: %v", err)
	}
	if _, err := ReadWorkspaceEffects(workspacePath); err == nil {
		t.Fatal("duplicate workspace identity returned nil error")
	}
}

func TestSmallValidationHelpersRejectMalformedValues(t *testing.T) {
	t.Parallel()

	if _, err := parseAttemptNumber("missing"); err == nil {
		t.Fatal("missing attempt suffix returned nil error")
	}
	if _, err := parseAttemptNumber("run-attempt-zero"); err == nil {
		t.Fatal("non-numeric attempt suffix returned nil error")
	}
	if _, err := parseAttemptNumber("run-attempt-0"); err == nil {
		t.Fatal("zero attempt suffix returned nil error")
	}
	if _, err := readJSONFile[ProcessRecord](filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Fatal("missing JSON file returned nil error")
	}
	if err := verifyTrialVerdict(RecoveryModeUnsafeFresh, protocol.ProbeUnsafe,
		protocol.Verdict{Class: protocol.VerdictValidPass}); err == nil {
		t.Fatal("unexpected unsafe pass returned nil error")
	}
	if err := verifyTrialVerdict(RecoveryModeResumeOnly, protocol.ProbeUnsafe,
		protocol.Verdict{Class: protocol.VerdictValidPass}); err != nil {
		t.Fatalf("resume-only valid pass was rejected: %v", err)
	}
	if err := verifyTrialVerdict(RecoveryModeResumeOnly, protocol.ProbeUnsafe,
		protocol.Verdict{Class: protocol.VerdictInvalid}); err == nil {
		t.Fatal("resume-only invalid verdict returned nil error")
	}
	if identity := (&managedWorker{}).processIdentity(); identity != "" {
		t.Fatalf("empty Worker identity = %q", identity)
	}
}
