package lab

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/evidence"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/oracle"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

func TestUnsafeClaudeCaptureProducesValidFailForDuplicatePhysicalEffect(t *testing.T) {
	t.Parallel()

	bundle, err := BuildEvidenceBundle(unsafeCapture())
	if err != nil {
		t.Fatalf("build unsafe evidence: %v", err)
	}
	runDirectory, err := evidence.WriteRun(context.Background(), t.TempDir(), bundle)
	if err != nil {
		t.Fatalf("write unsafe evidence: %v", err)
	}
	verdict, err := oracle.EvaluateAndWrite(context.Background(), runDirectory)
	if err != nil {
		t.Fatalf("evaluate unsafe evidence: %v", err)
	}
	if verdict.Class != protocol.VerdictValidFail ||
		!containsReason(verdict.ReasonCodes, protocol.ReasonDuplicateEffect) {
		t.Fatalf("verdict = %+v", verdict)
	}
	if verdict.Metrics.PhysicalEffectCount != 2 || verdict.Metrics.AcceptedOutcomeCount != 1 {
		t.Fatalf("metrics = %+v", verdict.Metrics)
	}
}

func TestUnsafeClaudeCaptureSupportsEveryDeclaredFaultOrder(t *testing.T) {
	t.Parallel()
	for _, boundary := range unsafeFaultSchedule() {
		boundary := boundary
		t.Run(string(boundary), func(t *testing.T) {
			t.Parallel()
			capture := unsafeCapture()
			capture.FaultBoundary = boundary
			capture.Boundary.Point = boundary
			if boundary == FaultBeforeVendorRegistration {
				capture.Boundary.ReachedAt = capture.Attempts[0].StartedAt.Add(250 * time.Millisecond)
				capture.FaultAt = capture.Attempts[0].StartedAt.Add(500 * time.Millisecond)
			}
			bundle, err := BuildEvidenceBundle(capture)
			if err != nil {
				t.Fatalf("build %s evidence: %v", boundary, err)
			}
			runDirectory, err := evidence.WriteRun(context.Background(), t.TempDir(), bundle)
			if err != nil {
				t.Fatalf("write %s evidence: %v", boundary, err)
			}
			verdict, err := oracle.EvaluateAndWrite(context.Background(), runDirectory)
			if err != nil {
				t.Fatalf("evaluate %s evidence: %v", boundary, err)
			}
			if verdict.Class != protocol.VerdictValidFail ||
				!containsReason(verdict.ReasonCodes, protocol.ReasonDuplicateEffect) {
				t.Fatalf("%s verdict = %+v", boundary, verdict)
			}
		})
	}
}

func TestUnfaultedClaudeCaptureProducesValidPass(t *testing.T) {
	t.Parallel()

	capture := unsafeCapture()
	capture.Probe = protocol.ProbeUnfaulted
	capture.FaultBoundary = FaultNone
	capture.Boundary = BoundaryCapture{}
	capture.Attempts = append([]ClaudeAttemptCapture(nil), capture.Attempts[:1]...)
	capture.FaultAt = time.Time{}
	capture.CompletedAt = capture.Attempts[0].AppliedAt.Add(time.Second)
	bundle, err := BuildEvidenceBundle(capture)
	if err != nil {
		t.Fatalf("build unfaulted evidence: %v", err)
	}
	runDirectory, err := evidence.WriteRun(context.Background(), t.TempDir(), bundle)
	if err != nil {
		t.Fatalf("write unfaulted evidence: %v", err)
	}
	verdict, err := oracle.EvaluateAndWrite(context.Background(), runDirectory)
	if err != nil {
		t.Fatalf("evaluate unfaulted evidence: %v", err)
	}
	if verdict.Class != protocol.VerdictValidPass || len(verdict.ReasonCodes) != 0 {
		t.Fatalf("verdict = %+v", verdict)
	}
}

func TestResumeOnlyCaptureBindsSelectedAndObservedVendorSession(t *testing.T) {
	t.Parallel()

	const selected = "01890f3e-7b5a-4c2d-8e1f-0123456789ab"
	capture := unsafeCapture()
	capture.RecoveryMode = RecoveryModeResumeOnly
	capture.SelectedVendorSessionID = selected
	for index := range capture.Attempts {
		capture.Attempts[index].VendorSessionID = selected
	}
	bundle, err := BuildEvidenceBundle(capture)
	if err != nil {
		t.Fatalf("build resume-only evidence: %v", err)
	}
	if bundle.Input.Settings["recovery_mode"] != string(RecoveryModeResumeOnly) ||
		bundle.Input.Settings["selected_vendor_session_id"] != selected {
		t.Fatalf("effective input settings = %+v", bundle.Input.Settings)
	}

	capture.Attempts[1].VendorSessionID = "11890f3e-7b5a-4c2d-8e1f-0123456789ab"
	if _, err := BuildEvidenceBundle(capture); err == nil || !strings.Contains(err.Error(), "selected Claude session") {
		t.Fatalf("session mismatch error = %v", err)
	}
}

func TestFencedCaptureProvesOneExecutionEffectAndRecoveryAttachment(t *testing.T) {
	t.Parallel()
	for _, boundary := range fencedFaultSchedule() {
		boundary := boundary
		t.Run(string(boundary), func(t *testing.T) {
			t.Parallel()
			capture := protectedCapture(boundary)
			bundle, err := BuildEvidenceBundle(capture)
			if err != nil {
				t.Fatalf("build protected evidence: %v", err)
			}
			runDirectory, err := evidence.WriteRun(context.Background(), t.TempDir(), bundle)
			if err != nil {
				t.Fatalf("write protected evidence: %v", err)
			}
			verdict, err := oracle.EvaluateAndWrite(context.Background(), runDirectory)
			if err != nil {
				t.Fatalf("evaluate protected evidence: %v", err)
			}
			if verdict.Class != protocol.VerdictValidPass || verdict.Metrics.PhysicalEffectCount != 1 ||
				verdict.Metrics.ConcurrentOwnerCount != 1 || verdict.Metrics.AcceptedOutcomeCount != 1 {
				t.Fatalf("protected verdict = %+v", verdict)
			}
		})
	}
}

func TestFencedCaptureRejectsAmbiguousRecoveryAttachments(t *testing.T) {
	t.Parallel()
	capture := protectedCapture(FaultAfterToolEffect)
	capture.Attachments = append(capture.Attachments, capture.Attachments[0])
	if _, err := BuildEvidenceBundle(capture); err == nil {
		t.Fatal("duplicate recovery attachment returned nil error")
	}
}

func protectedCapture(boundary FaultBoundary) EvidenceCapture {
	const selected = "01890f3e-7b5a-4c2d-8e1f-0123456789ab"
	started := time.Date(2026, 8, 11, 1, 0, 0, 0, time.UTC)
	attemptStarted := started.Add(time.Second)
	applied := started.Add(2 * time.Second)
	boundaryAt := started.Add(2500 * time.Millisecond)
	faultAt := started.Add(3 * time.Second)
	attachedAt := started.Add(4 * time.Second)
	if boundary == FaultBeforeVendorRegistration {
		boundaryAt = started.Add(1500 * time.Millisecond)
		faultAt = started.Add(2 * time.Second)
		attachedAt = started.Add(3 * time.Second)
		applied = started.Add(4 * time.Second)
	} else if boundary == FaultAfterClaimBeforeExec {
		boundaryAt = started.Add(time.Second)
		faultAt = started.Add(2 * time.Second)
		attachedAt = started.Add(3 * time.Second)
		attemptStarted = started.Add(4 * time.Second)
		applied = started.Add(5 * time.Second)
	}
	processIdentity := "pid:101:start:claude-one"
	return EvidenceCapture{
		AdapterVersion: "claude-fenced-v1", ClaudeBinarySHA256: strings.Repeat("a", 64),
		ClaudeVersion: "2.1.226", Model: "haiku", Runtime: "linux/amd64",
		Probe: protocol.ProbeProtected, FaultBoundary: boundary, Trial: 1,
		LogicalSessionID: "logical-session-1", LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
		DestinationID: "fenced-destination-1", RecoveryMode: RecoveryModeFenced,
		SelectedVendorSessionID: selected, StartedAt: started,
		Attempts: []ClaudeAttemptCapture{{
			TemporalAttempt: 1, ActorID: "claude-supervisor-g1", ProcessIdentity: processIdentity,
			VendorSessionID: selected, PhysicalAttemptID: "supervisor-generation-1-attempt-1",
			StartedAt: attemptStarted, AppliedAt: applied,
		}},
		Attachments: []AttachmentCapture{{
			TemporalAttempt: 2, ActorID: "worker-two", ProcessIdentity: processIdentity,
			Generation: 1, AttachedAt: attachedAt,
		}},
		Boundary: BoundaryCapture{
			Point: boundary, ActorID: "worker-one",
			ProcessIdentity: "pid:100:start:worker-one", ReachedAt: boundaryAt,
		},
		FaultAt: faultAt, CompletedAt: started.Add(6 * time.Second),
		Settings: map[string]string{"fault_selection": "named-barrier"},
		Native:   []NativeCapture{{Kind: "supervisor", Detail: "generation one attached by Activity attempt two"}},
	}
}

func unsafeCapture() EvidenceCapture {
	started := time.Date(2026, 8, 8, 13, 0, 0, 0, time.UTC)
	return EvidenceCapture{
		AdapterVersion: "claude-direct-v1", ClaudeBinarySHA256: strings.Repeat("a", 64),
		ClaudeVersion: "2.1.226", Model: "haiku", Runtime: "linux/amd64",
		Probe: protocol.ProbeUnsafe, FaultBoundary: FaultAfterToolEffect,
		Trial: 1, LogicalSessionID: "logical-session-1",
		LogicalTurnID: "turn-1", LogicalEffectID: "effect-1", DestinationID: "fixture-destination-1",
		StartedAt: started,
		Attempts: []ClaudeAttemptCapture{
			{
				TemporalAttempt: 1, ActorID: "worker-one-attempt-1",
				ProcessIdentity: "pid:101:start:one", VendorSessionID: "vendor-session-1",
				PhysicalAttemptID: "physical-attempt-1", StartedAt: started.Add(time.Second),
				AppliedAt: started.Add(2 * time.Second),
			},
			{
				TemporalAttempt: 2, ActorID: "worker-two-attempt-2",
				ProcessIdentity: "pid:202:start:two", VendorSessionID: "vendor-session-2",
				PhysicalAttemptID: "physical-attempt-2", StartedAt: started.Add(4 * time.Second),
				AppliedAt: started.Add(5 * time.Second),
			},
		},
		Boundary: BoundaryCapture{
			Point: FaultAfterToolEffect, ActorID: "worker-one",
			ProcessIdentity: "pid:100:start:worker-one", ReachedAt: started.Add(2500 * time.Millisecond),
		},
		FaultAt: started.Add(3 * time.Second), CompletedAt: started.Add(6 * time.Second),
		Settings: map[string]string{
			"fault_selection": "named-barrier", "workspace_before_sha256": strings.Repeat("b", 64),
			"workspace_after_sha256": strings.Repeat("c", 64),
		},
		Native: []NativeCapture{{Kind: "temporal-history", Detail: "two Activity deliveries; one accepted result"}},
	}
}

func containsReason(reasons []string, want string) bool {
	for _, reason := range reasons {
		if reason == want {
			return true
		}
	}
	return false
}
