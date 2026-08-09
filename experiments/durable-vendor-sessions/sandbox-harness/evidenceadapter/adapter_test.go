package evidenceadapter

import (
	"context"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/evidence"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/oracle"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

func TestAmbiguousProviderEffectUsesCommonFailClosedOracle(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		probe     protocol.Probe
		second    bool
		wantClass protocol.VerdictClass
	}{
		{probe: protocol.ProbeUnsafe, second: true, wantClass: protocol.VerdictValidFail},
		{probe: protocol.ProbeProtected, second: false, wantClass: protocol.VerdictValidPass},
	} {
		t.Run(string(test.probe), func(t *testing.T) {
			t.Parallel()
			bundle, err := BuildAmbiguousEffectBundle(validCapture(test.probe, test.second))
			if err != nil {
				t.Fatalf("BuildAmbiguousEffectBundle() error = %v", err)
			}
			runDirectory, err := evidence.WriteRun(context.Background(), t.TempDir(), bundle)
			if err != nil {
				t.Fatalf("WriteRun() error = %v", err)
			}
			verdict, err := oracle.EvaluateAndWrite(context.Background(), runDirectory)
			if err != nil {
				t.Fatalf("EvaluateAndWrite() error = %v", err)
			}
			if verdict.Class != test.wantClass {
				t.Fatalf("verdict = %s (%v), want %s", verdict.Class, verdict.ReasonCodes, test.wantClass)
			}
		})
	}
}

func TestAttachedWriterUsesCommonStaleGenerationOracle(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		probe     protocol.Probe
		applied   bool
		wantClass protocol.VerdictClass
	}{
		{probe: protocol.ProbeUnsafe, applied: true, wantClass: protocol.VerdictValidFail},
		{probe: protocol.ProbeProtected, applied: false, wantClass: protocol.VerdictValidPass},
	} {
		t.Run(string(test.probe), func(t *testing.T) {
			t.Parallel()
			base := time.Date(2026, 8, 8, 13, 0, 0, 0, time.UTC)
			capture := AttachedWriterCapture{
				AdapterVersion: "adapter-sha", AgentSourceSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				Trial: 1, Probe: test.probe, SessionID: "sandbox/session-1", DestinationID: "provider:test",
				LogicalEffectID: "stale-command", Runtime: "Go test", StartedAt: base,
				OldOwner:      ProcessCapture{ActorID: "attached-old", Identity: "worker-old/pid-1"},
				CurrentOwner:  ProcessCapture{ActorID: "attached-current", Identity: "worker-current/pid-2"},
				ReplacementAt: base.Add(time.Second), BoundaryAt: base.Add(2 * time.Second),
				StaleAttempt: AttemptCapture{PhysicalAttemptID: "stale-attempt-1", Applied: test.applied, ObservedAt: base.Add(3 * time.Second)},
				CompletedAt:  base.Add(4 * time.Second), Native: []NativeCapture{{Kind: "provider_journal", Detail: "{}"}},
			}
			bundle, err := BuildAttachedWriterBundle(capture)
			if err != nil {
				t.Fatalf("BuildAttachedWriterBundle() error = %v", err)
			}
			runDirectory, err := evidence.WriteRun(context.Background(), t.TempDir(), bundle)
			if err != nil {
				t.Fatalf("WriteRun() error = %v", err)
			}
			verdict, err := oracle.EvaluateAndWrite(context.Background(), runDirectory)
			if err != nil {
				t.Fatalf("EvaluateAndWrite() error = %v", err)
			}
			if verdict.Class != test.wantClass {
				t.Fatalf("verdict = %s (%v), want %s", verdict.Class, verdict.ReasonCodes, test.wantClass)
			}
		})
	}
}

func validCapture(probe protocol.Probe, secondApplied bool) AmbiguousEffectCapture {
	base := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	return AmbiguousEffectCapture{
		AdapterVersion: "adapter-sha", AgentSourceSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Operation: "command", Trial: 1, Probe: probe, SessionID: "sandbox/session-1",
		DestinationID: "provider:test", LogicalEffectID: "command-1", Generation: 1,
		Runtime: "Go test", StartedAt: base,
		FirstWorker:    ProcessCapture{ActorID: "sandbox", Identity: "worker-1/pid-1"},
		RecoveryWorker: ProcessCapture{ActorID: "sandbox", Identity: "worker-2/pid-2"},
		Attempts: []AttemptCapture{
			{PhysicalAttemptID: "attempt-1", Applied: true, ObservedAt: base.Add(time.Second)},
			{PhysicalAttemptID: "attempt-2", Applied: secondApplied, ObservedAt: base.Add(3 * time.Second)},
		},
		Fault:       FaultCapture{Point: "provider-effect-committed", TriggeredAt: base.Add(2 * time.Second)},
		CompletedAt: base.Add(4 * time.Second),
		Native:      []NativeCapture{{Kind: "temporal_history_event", Detail: "{}"}},
		Settings:    map[string]string{"provider_mode": string(probe)},
	}
}

func TestCaptureValidationRejectsInvalidEvidence(t *testing.T) {
	t.Parallel()
	valid := validCapture(protocol.ProbeProtected, false)
	invalidAmbiguous := []AmbiguousEffectCapture{
		{},
		func() AmbiguousEffectCapture { value := valid; value.Probe = protocol.ProbeUnfaulted; return value }(),
		func() AmbiguousEffectCapture { value := valid; value.Attempts[1].Applied = true; return value }(),
		func() AmbiguousEffectCapture {
			value := valid
			value.Attempts[1].PhysicalAttemptID = value.Attempts[0].PhysicalAttemptID
			return value
		}(),
		func() AmbiguousEffectCapture { value := valid; value.CompletedAt = value.StartedAt; return value }(),
	}
	for _, capture := range invalidAmbiguous {
		if _, err := BuildAmbiguousEffectBundle(capture); err == nil {
			t.Fatalf("BuildAmbiguousEffectBundle(%+v) error = nil", capture)
		}
	}

	base := valid.StartedAt
	attached := AttachedWriterCapture{
		AdapterVersion: "adapter", AgentSourceSHA256: valid.AgentSourceSHA256,
		Trial: 1, Probe: protocol.ProbeProtected, SessionID: "session", DestinationID: "destination",
		LogicalEffectID: "effect", Runtime: "runtime", StartedAt: base,
		OldOwner:      ProcessCapture{ActorID: "old", Identity: "old-process"},
		CurrentOwner:  ProcessCapture{ActorID: "current", Identity: "current-process"},
		ReplacementAt: base.Add(time.Second), BoundaryAt: base.Add(2 * time.Second),
		StaleAttempt: AttemptCapture{PhysicalAttemptID: "attempt", ObservedAt: base.Add(3 * time.Second)},
		CompletedAt:  base.Add(4 * time.Second), Native: []NativeCapture{{Kind: "journal", Detail: "{}"}},
	}
	invalidAttached := []AttachedWriterCapture{
		{},
		func() AttachedWriterCapture { value := attached; value.Probe = protocol.ProbeUnfaulted; return value }(),
		func() AttachedWriterCapture { value := attached; value.StaleAttempt.Applied = true; return value }(),
		func() AttachedWriterCapture { value := attached; value.BoundaryAt = base; return value }(),
	}
	for _, capture := range invalidAttached {
		if _, err := BuildAttachedWriterBundle(capture); err == nil {
			t.Fatalf("BuildAttachedWriterBundle(%+v) error = nil", capture)
		}
	}
}
