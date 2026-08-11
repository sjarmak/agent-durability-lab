package lab

import (
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

func TestValidateDirectAuditTrialDerivesVerdictFromRawFacts(t *testing.T) {
	const (
		sessionID = "logical-session"
		attemptID = "attempt-1"
		processID = "pid:10:start:boot:1"
		vendorID  = "11111111-2222-4333-8444-555555555555"
	)
	run := auditedRun{
		manifest: protocol.Manifest{RunID: "run-1", Case: protocol.CaseAmbiguousEffect, Probe: protocol.ProbeUnfaulted, Trial: 1, SessionID: sessionID},
		processes: []protocol.ProcessObservation{{
			Sequence: 1, ActorID: "actor-1", Generation: 1, ProcessIdentity: processID, State: "running",
		}},
		verdict: protocol.Verdict{
			RunID: "run-1", Case: protocol.CaseAmbiguousEffect, Probe: protocol.ProbeUnfaulted, Trial: 1,
			Class: protocol.VerdictValidPass,
			Metrics: protocol.Metrics{
				AcceptedOutcomeCount: 1,
				PhysicalEffectCount:  1,
				PhysicalAttemptCount: 1,
				ConcurrentOwnerCount: 1,
			},
		},
		input: protocol.EffectiveInput{Settings: map[string]string{
			"fault_boundary": string(FaultNone), "session_identity": "vendor-assigned-after-start",
			"resume_control": "none", "workspace_before_sha256": "before", "workspace_after_sha256": "after",
			"workspace_effect_count": "1",
		}},
		summary: trialSummary{
			WorkflowID: "workflow-1", WorkflowRunID: "run-1", Probe: protocol.ProbeUnfaulted,
			FaultBoundary: FaultNone, Trial: 1, WorkspaceBeforeHash: "before", WorkspaceAfterHash: "after",
			WorkflowResult: ClaudeActivityResult{
				TemporalAttempt: 1, PhysicalAttemptID: attemptID, VendorSessionID: vendorID,
				ProcessIdentity: processID, Result: "EFFECT_COMPLETE",
			},
			Destination: DestinationSnapshot{Attempts: []EffectAttempt{{
				LogicalSessionID: sessionID, LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
				PhysicalAttemptID: attemptID, ActorID: "actor-1", ProcessIdentity: "effect-process", Applied: true,
			}}},
			WorkspaceEffects: []WorkspaceEffect{{
				LogicalEffectID: "effect-1", PhysicalAttemptID: attemptID, Payload: "controlled-edit",
				ActorID: "actor-1", ProcessIdentity: "effect-process",
			}},
		},
	}
	attempts := []auditedDirectAttempt{{
		number: 1,
		request: ControlledEffectInput{
			LogicalSessionID: sessionID, LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
			PhysicalAttemptID: attemptID, ActorID: "actor-1", Payload: "controlled-edit",
			BarrierPoint: committedEffectBarrier,
		},
		process: ProcessRecord{AttemptID: attemptID, ActorID: "actor-1", Identity: processID, PID: 10, State: "running"},
		stream:  ClaudeStreamResult{SessionID: vendorID, Result: "EFFECT_COMPLETE"},
	}}
	if err := validateDirectAuditTrial(run, attempts); err != nil {
		t.Fatalf("validate direct trial: %v", err)
	}

	changed := run
	changed.summary.WorkspaceEffects = nil
	if err := validateDirectAuditTrial(changed, attempts); err == nil {
		t.Fatal("missing raw workspace effect returned nil error")
	}
	changedAttempts := append([]auditedDirectAttempt(nil), attempts...)
	changedAttempts[0].stream.SessionID = ""
	if err := validateDirectAuditTrial(run, changedAttempts); err == nil {
		t.Fatal("blank raw Claude session returned nil error")
	}
	changedAttempts = append([]auditedDirectAttempt(nil), attempts...)
	changedAttempts[0].process.Identity = "pid:11:start:boot:1"
	changed = run
	changed.summary.WorkflowResult.ProcessIdentity = changedAttempts[0].process.Identity
	if err := validateDirectAuditTrial(changed, changedAttempts); err == nil {
		t.Fatal("process receipt differing from public observation returned nil error")
	}
}
