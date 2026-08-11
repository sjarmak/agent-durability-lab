package lab

import (
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

func TestValidateResumeAuditTrialRequiresIndependentDuplicateEffects(t *testing.T) {
	now := time.Date(2026, 8, 11, 15, 0, 0, 0, time.UTC)
	const (
		sessionID   = "logical-session"
		sessionUUID = "11111111-2222-4333-8444-555555555555"
	)
	attempts := make([]auditedResumeAttempt, 0, 2)
	processes := make([]protocol.ProcessObservation, 0, 2)
	workspace := make([]WorkspaceEffect, 0, 2)
	destination := make([]EffectAttempt, 0, 2)
	for number := int32(1); number <= 2; number++ {
		physicalID := "physical-attempt-" + string(rune('0'+number))
		actorID := "worker-attempt-" + string(rune('0'+number))
		effectProcess := "pid:20" + string(rune('0'+number)) + ":start:boot:1"
		flag := "--session-id"
		if number == 2 {
			flag = "--resume"
		}
		request := ControlledEffectInput{
			DestinationPath: "/tmp/destination.db", WorkspacePath: "/tmp/effects.jsonl",
			Payload: "controlled-edit", BarrierURL: "http://127.0.0.1", BarrierPoint: committedEffectBarrier,
			LogicalSessionID: sessionID, LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
			PhysicalAttemptID: physicalID, ActorID: actorID,
		}
		process := ProcessRecord{
			AttemptID: physicalID, ActorID: actorID, Binary: "/tmp/claude", Args: []string{flag, sessionUUID},
			WorkDir: "/tmp/work", PID: int(100 + number), StartIdentity: "boot:1",
			Identity: "pid:10" + string(rune('0'+number)) + ":start:boot:1", State: "running",
		}
		attempts = append(attempts, auditedResumeAttempt{
			number: number, request: request, process: process,
			stream: ClaudeStreamResult{SessionID: sessionUUID, Result: "EFFECT_COMPLETE"},
		})
		processes = append(processes, protocol.ProcessObservation{
			Sequence: uint64(number), ActorID: actorID, Generation: 1, ProcessIdentity: process.Identity, State: "running",
		})
		workspace = append(workspace, WorkspaceEffect{
			LogicalEffectID: "effect-1", PhysicalAttemptID: physicalID, Payload: "controlled-edit",
			ActorID: actorID, ProcessIdentity: effectProcess, AppliedAt: now.Add(time.Duration(number) * time.Second),
		})
		destination = append(destination, EffectAttempt{
			LogicalSessionID: sessionID, LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
			PhysicalAttemptID: physicalID, ActorID: actorID, ProcessIdentity: effectProcess,
			Applied: true, AppliedAt: now.Add(time.Duration(number) * time.Second),
		})
	}
	summary := trialSummary{
		Probe: protocol.ProbeUnsafe, FaultBoundary: FaultAfterToolEffect, Trial: 1,
		RecoveryMode: RecoveryModeResumeOnly, SelectedVendorSessionID: sessionUUID,
		WorkflowResult: ClaudeActivityResult{
			TemporalAttempt: 2, PhysicalAttemptID: attempts[1].request.PhysicalAttemptID,
			VendorSessionID: sessionUUID, Result: "EFFECT_COMPLETE", ProcessIdentity: attempts[1].process.Identity,
		},
		WorkspaceBeforeHash: "before", WorkspaceAfterHash: "after", WorkspaceEffects: workspace,
		Destination: DestinationSnapshot{Attempts: destination}, ReplayVerified: true,
	}
	manifest := protocol.Manifest{
		RunID: "run-1", Case: protocol.CaseAmbiguousEffect, Probe: protocol.ProbeUnsafe,
		Trial: 1, SessionID: sessionID,
	}
	input := protocol.EffectiveInput{Settings: map[string]string{
		"recovery_mode": string(RecoveryModeResumeOnly), "selected_vendor_session_id": sessionUUID,
		"fault_boundary": string(FaultAfterToolEffect), "workspace_effect_count": "2",
		"workflow_history_replay_verified": "true", "workspace_before_sha256": "before",
		"workspace_after_sha256": "after", "session_identity": "caller-selected-before-workflow-start",
		"resume_control": "first-delivery-session-id-later-deliveries-resume",
	}}
	verdict := protocol.Verdict{
		RunID: manifest.RunID, Case: manifest.Case, Probe: manifest.Probe, Trial: manifest.Trial,
		Class: protocol.VerdictValidFail, ReasonCodes: []string{protocol.ReasonDuplicateEffect},
		Metrics: protocol.Metrics{
			AcceptedOutcomeCount: 1, PhysicalEffectCount: 2, PhysicalAttemptCount: 2, ConcurrentOwnerCount: 2,
		},
	}
	if err := validateResumeAuditTrial(manifest, input, verdict, summary, processes, attempts); err != nil {
		t.Fatalf("validate resume-only control: %v", err)
	}

	t.Run("hidden capability", func(t *testing.T) {
		changed := append([]auditedResumeAttempt(nil), attempts...)
		changed[0].request.OwnerCapability = "unexpected"
		if err := validateResumeAuditTrial(manifest, input, verdict, summary, processes, changed); err == nil {
			t.Fatal("resume request with fencing capability returned nil error")
		}
	})
	t.Run("missing duplicate", func(t *testing.T) {
		changed := verdict
		changed.Metrics.PhysicalEffectCount = 1
		if err := validateResumeAuditTrial(manifest, input, changed, summary, processes, attempts); err == nil {
			t.Fatal("single physical effect returned nil error")
		}
	})
	t.Run("wrong accepted process", func(t *testing.T) {
		changed := summary
		changed.WorkflowResult.ProcessIdentity = attempts[0].process.Identity
		if err := validateResumeAuditTrial(manifest, input, verdict, changed, processes, attempts); err == nil {
			t.Fatal("result from stale delivery returned nil error")
		}
	})
	t.Run("process observation differs", func(t *testing.T) {
		changed := append([]auditedResumeAttempt(nil), attempts...)
		changed[0].process.Identity = "pid:999:start:boot:1"
		if err := validateResumeAuditTrial(manifest, input, verdict, summary, processes, changed); err == nil {
			t.Fatal("process receipt differing from public observation returned nil error")
		}
	})
}
