package evidenceadapter

import (
	"context"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/evidence"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/oracle"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

func TestAmbiguousEffectCaptureUsesCommonWriterAndOracle(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		probe      protocol.Probe
		applied    []bool
		wantClass  protocol.VerdictClass
		wantReason string
	}{
		{name: "unsafe", probe: protocol.ProbeUnsafe, applied: []bool{true, true}, wantClass: protocol.VerdictValidFail, wantReason: protocol.ReasonDuplicateEffect},
		{name: "protected", probe: protocol.ProbeProtected, applied: []bool{true, false}, wantClass: protocol.VerdictValidPass},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			capture := fixtureCapture(test.probe, test.applied)
			bundle, err := BuildBundle(capture)
			if err != nil {
				t.Fatalf("BuildBundle() error = %v", err)
			}
			runDir, err := evidence.WriteRun(context.Background(), t.TempDir(), bundle)
			if err != nil {
				t.Fatalf("WriteRun() error = %v", err)
			}
			verdict, err := oracle.EvaluateAndWrite(context.Background(), runDir)
			if err != nil {
				t.Fatalf("EvaluateAndWrite() error = %v", err)
			}
			if verdict.Class != test.wantClass {
				t.Fatalf("verdict class = %q, want %q; reasons=%v", verdict.Class, test.wantClass, verdict.ReasonCodes)
			}
			if test.wantReason != "" && !contains(verdict.ReasonCodes, test.wantReason) {
				t.Fatalf("verdict reasons = %v, want %q", verdict.ReasonCodes, test.wantReason)
			}
		})
	}
}

func TestBuildBundleRejectsMalformedNativeCapture(t *testing.T) {
	t.Parallel()
	valid := fixtureCapture(protocol.ProbeProtected, []bool{true, false})
	tests := []struct {
		name   string
		mutate func(Capture) Capture
	}{
		{name: "incomplete", mutate: func(c Capture) Capture { c.SessionID = ""; return c }},
		{name: "unsupported probe", mutate: func(c Capture) Capture { c.Probe = protocol.ProbeUnfaulted; return c }},
		{name: "wrong barrier", mutate: func(c Capture) Capture { c.Fault.Point = "wrong"; return c }},
		{name: "reused attempt", mutate: func(c Capture) Capture { c.Attempts[1].PhysicalAttemptID = c.Attempts[0].PhysicalAttemptID; return c }},
		{name: "unordered fault", mutate: func(c Capture) Capture { c.Fault.TriggeredAt = c.CompletedAt.Add(time.Second); return c }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			capture := valid
			capture.Attempts = append([]AttemptCapture(nil), valid.Attempts...)
			if _, err := BuildBundle(test.mutate(capture)); err == nil {
				t.Fatal("BuildBundle() error = nil, want malformed capture rejection")
			}
		})
	}
}

func fixtureCapture(probe protocol.Probe, applied []bool) Capture {
	base := time.Date(2026, 8, 8, 1, 0, 0, 0, time.UTC)
	return Capture{
		AdapterVersion: "test-v1", Trial: 1, Probe: probe,
		SessionID: "native/session-1", DestinationID: "sqlite:test",
		LogicalEffectID: "native/session-1/turn/1/effect/1", Generation: 1,
		AgentSourceSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Runtime:           "python-test", StartedAt: base,
		FirstWorker:    ProcessCapture{ActorID: "agent-1", Identity: "worker-1/pid-101"},
		RecoveryWorker: ProcessCapture{ActorID: "agent-1", Identity: "worker-2/pid-202"},
		Attempts: []AttemptCapture{
			{PhysicalAttemptID: "run/activity/attempt/1", Applied: applied[0], ObservedAt: base.Add(time.Second)},
			{PhysicalAttemptID: "run/activity/attempt/2", Applied: applied[1], ObservedAt: base.Add(3 * time.Second)},
		},
		Fault:       FaultCapture{Point: "tool-effect-committed", TriggeredAt: base.Add(2 * time.Second)},
		CompletedAt: base.Add(4 * time.Second),
		History:     []NativeCapture{{Kind: "temporal_history_event", Detail: `{"event_id":1}`}},
	}
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
