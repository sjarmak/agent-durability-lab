package livecommon

import (
	"os"
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/evidence"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
	"github.com/sjarmak/temporal_projects/internal/workstore"
)

func TestValidateConfigRequiresAdapterVersion(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("current executable: %v", err)
	}
	config := Config{
		Root: t.TempDir(), Case: protocol.CaseSurvivingExecutor,
		Probe: protocol.ProbeUnfaulted, Trial: 1, AgentBinary: executable,
	}
	if err := validateConfig(config); err == nil {
		t.Fatal("config without adapter version passed validation")
	}
}

func TestValidateCaptureRejectsMatchingEffectCountsWithDifferentIdentity(t *testing.T) {
	identity := evidence.RunIdentity{
		RunID: "run-1", Case: protocol.CaseAmbiguousEffect,
		Probe: protocol.ProbeUnsafe, Trial: 1, SessionID: "session-1",
	}
	h := &harness{recorder: newRecorder(identity)}
	h.recorder.destination.Attempts = []protocol.DestinationAttempt{{
		LogicalEffectID: "effect-1", PhysicalAttemptID: "attempt-1",
		Generation: 1, Sequence: 1, Applied: true,
	}}
	snapshot := workstore.Snapshot{
		SessionID: "session-1", ActiveGeneration: 1,
		Effects: []workstore.AcceptedEffect{{
			Effect: workstore.Effect{ID: "different-effect"}, Generation: 1,
		}},
	}
	if err := h.validateCapture(snapshot); err == nil {
		t.Fatal("capture with wrong effect identity passed validation")
	}
}
