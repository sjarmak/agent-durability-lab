package benchmark_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/calibration"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/oracle"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

func TestOracleRejectsDerivedStateContradictions(t *testing.T) {
	t.Parallel()

	t.Run("active generation", func(t *testing.T) {
		runDir := calibrationRun(t, protocol.CaseStaleGeneration)
		authority := readJSON[protocol.AuthorityState](t, filepath.Join(runDir, protocol.AuthorityStateFile))
		authority.ActiveGeneration = 1
		rewriteAndRehash(t, runDir, protocol.AuthorityStateFile, authority)
		assertOracle(t, runDir, protocol.VerdictInvalid, protocol.ReasonEvidenceInconsistent)
	})

	t.Run("unreported competitor", func(t *testing.T) {
		runDir := calibrationRun(t, protocol.CaseSurvivingExecutor)
		manifest := readJSON[protocol.Manifest](t, filepath.Join(runDir, protocol.ManifestFile))
		appendJSONLEvent(t, runDir, protocol.Event{Sequence: 6, Time: "2026-08-07T00:02:00Z", Kind: protocol.EventExecutorRegistered, SessionID: manifest.SessionID, ActorID: "agent-2", Generation: 2, ProcessIdentity: "pid:202:start:fixture", Decision: "observed"})
		processes := readJSON[[]protocol.ProcessObservation](t, filepath.Join(runDir, protocol.ProcessObservationsFile))
		processes = append(processes, protocol.ProcessObservation{Sequence: 6, ActorID: "agent-2", Generation: 2, ProcessIdentity: "pid:202:start:fixture", State: "running"})
		rewriteAndRehash(t, runDir, protocol.ProcessObservationsFile, processes)
		rehashFile(t, runDir, protocol.CommonEventsFile)
		assertOracle(t, runDir, protocol.VerdictInvalid, protocol.ReasonEvidenceInconsistent)
	})
}

func TestOracleRejectsUnknownEventsAndMissedCancellationBoundary(t *testing.T) {
	t.Parallel()

	t.Run("unknown accepted event", func(t *testing.T) {
		runDir := calibrationRun(t, protocol.CaseCancellationUnreachable)
		manifest := readJSON[protocol.Manifest](t, filepath.Join(runDir, protocol.ManifestFile))
		appendJSONLEvent(t, runDir, protocol.Event{Sequence: 5, Time: "2026-08-07T00:02:00Z", Kind: "workspace_mutation", SessionID: manifest.SessionID, ActorID: "agent-1", Generation: 1, ProcessIdentity: "pid:101:start:fixture", Decision: "accepted"})
		rehashFile(t, runDir, protocol.CommonEventsFile)
		assertOracle(t, runDir, protocol.VerdictInvalid, protocol.ReasonEvidenceMalformed)
	})

	t.Run("process not frozen", func(t *testing.T) {
		runDir := calibrationRun(t, protocol.CaseCancellationUnreachable)
		processes := readJSON[[]protocol.ProcessObservation](t, filepath.Join(runDir, protocol.ProcessObservationsFile))
		processes[0].State = "running"
		rewriteAndRehash(t, runDir, protocol.ProcessObservationsFile, processes)
		assertOracle(t, runDir, protocol.VerdictInvalid, protocol.ReasonFaultNotBracketed)
	})
}

func TestOracleCountsPostCancellationOutcome(t *testing.T) {
	t.Parallel()

	runDir := calibrationRun(t, protocol.CaseCancellationUnreachable)
	manifest := readJSON[protocol.Manifest](t, filepath.Join(runDir, protocol.ManifestFile))
	authority := readJSON[protocol.AuthorityState](t, filepath.Join(runDir, protocol.AuthorityStateFile))
	appendJSONLEvent(t, runDir, protocol.Event{Sequence: 5, Time: "2026-08-07T00:02:00Z", Kind: protocol.EventOutcomeAccepted, SessionID: manifest.SessionID, ActorID: "agent-1", Generation: 1, ProcessIdentity: "pid:101:start:fixture", Decision: "accepted"})
	authority.AcceptedOutcomes = append(authority.AcceptedOutcomes, protocol.AcceptedAction{Kind: "outcome", Generation: 1, Sequence: 5})
	rewriteAndRehash(t, runDir, protocol.AuthorityStateFile, authority)
	rehashFile(t, runDir, protocol.CommonEventsFile)
	assertOracle(t, runDir, protocol.VerdictValidFail, protocol.ReasonPostCancelMutation)
}

func calibrationRun(t *testing.T, benchmarkCase protocol.CaseID) string {
	t.Helper()
	runDir, err := calibration.Run(context.Background(), calibration.Config{Root: t.TempDir(), Case: benchmarkCase, Probe: protocol.ProbeProtected, Trial: 1})
	if err != nil {
		t.Fatalf("run calibration: %v", err)
	}
	return runDir
}

func assertOracle(t *testing.T, runDir string, class protocol.VerdictClass, reason string) {
	t.Helper()
	verdict, err := oracle.EvaluateAndWrite(context.Background(), runDir)
	if err != nil {
		t.Fatalf("evaluate evidence: %v", err)
	}
	if verdict.Class != class || !contains(verdict.ReasonCodes, reason) {
		t.Fatalf("verdict = %+v, want %s with %s", verdict, class, reason)
	}
}
