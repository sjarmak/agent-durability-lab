package benchmark_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/calibration"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/oracle"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

func TestCalibrationCasesProduceExpectedIndependentVerdicts(t *testing.T) {
	t.Parallel()

	for _, benchmarkCase := range protocol.Cases() {
		benchmarkCase := benchmarkCase
		for _, probe := range []protocol.Probe{protocol.ProbeUnsafe, protocol.ProbeProtected} {
			probe := probe
			t.Run(string(benchmarkCase)+"/"+string(probe), func(t *testing.T) {
				t.Parallel()
				for trial := 1; trial <= 3; trial++ {
					runDir, err := calibration.Run(context.Background(), calibration.Config{
						Root: t.TempDir(), Case: benchmarkCase, Probe: probe, Trial: trial,
					})
					if err != nil {
						t.Fatalf("run calibration trial %d: %v", trial, err)
					}
					assertRawEvidenceExistsWithoutVerdict(t, runDir)

					verdict, err := oracle.EvaluateAndWrite(context.Background(), runDir)
					if err != nil {
						t.Fatalf("evaluate trial %d: %v", trial, err)
					}
					want := protocol.VerdictValidPass
					if probe == protocol.ProbeUnsafe {
						want = protocol.VerdictValidFail
					}
					if verdict.Class != want {
						t.Errorf("trial %d verdict = %q, want %q; reasons=%v", trial, verdict.Class, want, verdict.ReasonCodes)
					}
					if verdict.Case != benchmarkCase || verdict.Probe != probe || verdict.Trial != trial {
						t.Errorf("trial %d identity mismatch: %+v", trial, verdict)
					}
				}
			})
		}
	}
}

func TestUnfaultedCalibrationPassesEveryCase(t *testing.T) {
	t.Parallel()

	for _, benchmarkCase := range protocol.Cases() {
		runDir, err := calibration.Run(context.Background(), calibration.Config{
			Root: t.TempDir(), Case: benchmarkCase, Probe: protocol.ProbeUnfaulted, Trial: 1,
		})
		if err != nil {
			t.Fatalf("run %s: %v", benchmarkCase, err)
		}
		verdict, err := oracle.EvaluateAndWrite(context.Background(), runDir)
		if err != nil {
			t.Fatalf("evaluate %s: %v", benchmarkCase, err)
		}
		if verdict.Class != protocol.VerdictValidPass {
			t.Errorf("%s verdict = %q, want %q; reasons=%v", benchmarkCase, verdict.Class, protocol.VerdictValidPass, verdict.ReasonCodes)
		}
	}
}

func TestOraclePreservesInvalidVerdictForTamperedEvidence(t *testing.T) {
	t.Parallel()

	runDir, err := calibration.Run(context.Background(), calibration.Config{
		Root: t.TempDir(), Case: protocol.CaseStaleGeneration, Probe: protocol.ProbeProtected, Trial: 1,
	})
	if err != nil {
		t.Fatalf("run calibration: %v", err)
	}
	eventsPath := filepath.Join(runDir, protocol.CommonEventsFile)
	file, err := os.OpenFile(eventsPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open events: %v", err)
	}
	_, writeErr := file.WriteString("{}\n")
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatalf("tamper events: write=%v close=%v", writeErr, closeErr)
	}

	verdict, err := oracle.EvaluateAndWrite(context.Background(), runDir)
	if err != nil {
		t.Fatalf("evaluate tampered evidence: %v", err)
	}
	if verdict.Class != protocol.VerdictInvalid || !contains(verdict.ReasonCodes, protocol.ReasonEvidenceHashMismatch) {
		t.Fatalf("verdict = %+v, want invalid hash mismatch", verdict)
	}
	assertVerdictFile(t, runDir, verdict)
}

func TestOraclePreservesInvalidVerdictWhenFaultMissesBoundary(t *testing.T) {
	t.Parallel()

	runDir, err := calibration.Run(context.Background(), calibration.Config{
		Root: t.TempDir(), Case: protocol.CaseSurvivingExecutor, Probe: protocol.ProbeProtected, Trial: 1,
	})
	if err != nil {
		t.Fatalf("run calibration: %v", err)
	}
	faultPath := filepath.Join(runDir, protocol.FaultBoundaryFile)
	fault := readJSON[protocol.FaultBoundary](t, faultPath)
	fault.AfterSequence = fault.BeforeSequence
	data, err := json.MarshalIndent(fault, "", "  ")
	if err != nil {
		t.Fatalf("encode altered fault: %v", err)
	}
	if err := os.WriteFile(faultPath, append(data, '\n'), 0o600); err != nil {
		t.Fatalf("alter fault: %v", err)
	}
	manifestPath := filepath.Join(runDir, protocol.ManifestFile)
	manifest := readJSON[protocol.Manifest](t, manifestPath)
	manifest.EvidenceSHA256[protocol.FaultBoundaryFile], err = protocol.FileSHA256(faultPath)
	if err != nil {
		t.Fatalf("rehash altered fault: %v", err)
	}
	data, err = json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatalf("encode manifest: %v", err)
	}
	if err := os.WriteFile(manifestPath, append(data, '\n'), 0o600); err != nil {
		t.Fatalf("rewrite manifest: %v", err)
	}

	verdict, err := oracle.EvaluateAndWrite(context.Background(), runDir)
	if err != nil {
		t.Fatalf("evaluate missed boundary: %v", err)
	}
	if verdict.Class != protocol.VerdictInvalid || !contains(verdict.ReasonCodes, protocol.ReasonFaultNotBracketed) {
		t.Fatalf("verdict = %+v, want invalid fault boundary", verdict)
	}
}

func TestOracleRejectsAuthorityStateThatDisagreesWithEvents(t *testing.T) {
	t.Parallel()

	runDir, err := calibration.Run(context.Background(), calibration.Config{
		Root: t.TempDir(), Case: protocol.CaseSurvivingExecutor, Probe: protocol.ProbeProtected, Trial: 1,
	})
	if err != nil {
		t.Fatalf("run calibration: %v", err)
	}
	authorityPath := filepath.Join(runDir, protocol.AuthorityStateFile)
	authority := readJSON[protocol.AuthorityState](t, authorityPath)
	authority.AcceptedOutcomes = append(authority.AcceptedOutcomes, protocol.AcceptedAction{Kind: "outcome", Generation: 1, Sequence: 999})
	rewriteAndRehash(t, runDir, protocol.AuthorityStateFile, authority)

	verdict, err := oracle.EvaluateAndWrite(context.Background(), runDir)
	if err != nil {
		t.Fatalf("evaluate inconsistent evidence: %v", err)
	}
	if verdict.Class != protocol.VerdictInvalid || !contains(verdict.ReasonCodes, protocol.ReasonEvidenceInconsistent) {
		t.Fatalf("verdict = %+v, want invalid inconsistent evidence", verdict)
	}
}

func TestOracleRejectsWrongFaultProcessIdentity(t *testing.T) {
	t.Parallel()

	runDir, err := calibration.Run(context.Background(), calibration.Config{
		Root: t.TempDir(), Case: protocol.CaseAmbiguousEffect, Probe: protocol.ProbeProtected, Trial: 1,
	})
	if err != nil {
		t.Fatalf("run calibration: %v", err)
	}
	faultPath := filepath.Join(runDir, protocol.FaultBoundaryFile)
	fault := readJSON[protocol.FaultBoundary](t, faultPath)
	fault.ProcessIdentity = "pid:999:start:wrong"
	rewriteAndRehash(t, runDir, protocol.FaultBoundaryFile, fault)

	verdict, err := oracle.EvaluateAndWrite(context.Background(), runDir)
	if err != nil {
		t.Fatalf("evaluate wrong identity: %v", err)
	}
	if verdict.Class != protocol.VerdictInvalid || !contains(verdict.ReasonCodes, protocol.ReasonWrongProcessIdentity) {
		t.Fatalf("verdict = %+v, want invalid wrong process identity", verdict)
	}
}

func TestOracleRejectsWrongNamedFaultBoundary(t *testing.T) {
	t.Parallel()

	runDir, err := calibration.Run(context.Background(), calibration.Config{
		Root: t.TempDir(), Case: protocol.CaseSurvivingExecutor, Probe: protocol.ProbeProtected, Trial: 1,
	})
	if err != nil {
		t.Fatalf("run calibration: %v", err)
	}
	faultPath := filepath.Join(runDir, protocol.FaultBoundaryFile)
	fault := readJSON[protocol.FaultBoundary](t, faultPath)
	fault.Point = "unrelated-boundary"
	fault.TriggeredAt = "not-a-timestamp"
	rewriteAndRehash(t, runDir, protocol.FaultBoundaryFile, fault)

	verdict, err := oracle.EvaluateAndWrite(context.Background(), runDir)
	if err != nil {
		t.Fatalf("evaluate wrong boundary: %v", err)
	}
	if verdict.Class != protocol.VerdictInvalid || !contains(verdict.ReasonCodes, protocol.ReasonFaultNotBracketed) {
		t.Fatalf("verdict = %+v, want invalid wrong boundary", verdict)
	}
}

func TestOracleRejectsAcceptedEventOmittedFromAuthorityState(t *testing.T) {
	t.Parallel()

	runDir, err := calibration.Run(context.Background(), calibration.Config{
		Root: t.TempDir(), Case: protocol.CaseStaleGeneration, Probe: protocol.ProbeProtected, Trial: 1,
	})
	if err != nil {
		t.Fatalf("run calibration: %v", err)
	}
	manifest := readJSON[protocol.Manifest](t, filepath.Join(runDir, protocol.ManifestFile))
	appendJSONLEvent(t, runDir, protocol.Event{
		Sequence: 7, Time: "2026-08-07T00:01:00Z", Kind: protocol.EventStaleCompletion,
		SessionID: manifest.SessionID, ActorID: "agent-1", Generation: 1,
		ProcessIdentity: "pid:101:start:fixture", Decision: "accepted",
	})
	rehashFile(t, runDir, protocol.CommonEventsFile)

	verdict, err := oracle.EvaluateAndWrite(context.Background(), runDir)
	if err != nil {
		t.Fatalf("evaluate omitted action: %v", err)
	}
	if verdict.Class != protocol.VerdictInvalid || !contains(verdict.ReasonCodes, protocol.ReasonEvidenceInconsistent) {
		t.Fatalf("verdict = %+v, want invalid inconsistent evidence", verdict)
	}
}

func TestOracleRequiresCancellationCommitForCancellationCase(t *testing.T) {
	t.Parallel()

	runDir, err := calibration.Run(context.Background(), calibration.Config{
		Root: t.TempDir(), Case: protocol.CaseCancellationUnreachable, Probe: protocol.ProbeProtected, Trial: 1,
	})
	if err != nil {
		t.Fatalf("run calibration: %v", err)
	}
	authorityPath := filepath.Join(runDir, protocol.AuthorityStateFile)
	authority := readJSON[protocol.AuthorityState](t, authorityPath)
	authority.CancellationCommitted = false
	authority.CancellationSequence = 0
	rewriteAndRehash(t, runDir, protocol.AuthorityStateFile, authority)

	verdict, err := oracle.EvaluateAndWrite(context.Background(), runDir)
	if err != nil {
		t.Fatalf("evaluate absent cancellation: %v", err)
	}
	if verdict.Class != protocol.VerdictInvalid || !contains(verdict.ReasonCodes, protocol.ReasonCasePreconditionMissing) {
		t.Fatalf("verdict = %+v, want invalid missing precondition", verdict)
	}
}

func TestOracleRejectsChangedFixedProtocols(t *testing.T) {
	t.Parallel()

	runDir, err := calibration.Run(context.Background(), calibration.Config{
		Root: t.TempDir(), Case: protocol.CaseAmbiguousEffect, Probe: protocol.ProbeProtected, Trial: 1,
	})
	if err != nil {
		t.Fatalf("run calibration: %v", err)
	}
	inputPath := filepath.Join(runDir, protocol.EffectiveInputFile)
	input := readJSON[protocol.EffectiveInput](t, inputPath)
	input.AgentProtocol = "different-agent"
	input.AuthorityProtocol = "different-authority"
	input.DestinationProtocol = "different-destination"
	rewriteAndRehash(t, runDir, protocol.EffectiveInputFile, input)

	verdict, err := oracle.EvaluateAndWrite(context.Background(), runDir)
	if err != nil {
		t.Fatalf("evaluate changed protocols: %v", err)
	}
	if verdict.Class != protocol.VerdictInvalid || !contains(verdict.ReasonCodes, protocol.ReasonEvidenceMalformed) {
		t.Fatalf("verdict = %+v, want invalid malformed input", verdict)
	}
}

func TestOracleRejectsEmptyRequiredRecords(t *testing.T) {
	t.Parallel()

	runDir, err := calibration.Run(context.Background(), calibration.Config{
		Root: t.TempDir(), Case: protocol.CaseAmbiguousEffect, Probe: protocol.ProbeUnfaulted, Trial: 1,
	})
	if err != nil {
		t.Fatalf("run calibration: %v", err)
	}
	rewriteAndRehash(t, runDir, protocol.ProcessObservationsFile, []protocol.ProcessObservation{{}})
	rewriteAndRehash(t, runDir, protocol.NativeJournalFile, []protocol.NativeRecord{{}})

	verdict, err := oracle.EvaluateAndWrite(context.Background(), runDir)
	if err != nil {
		t.Fatalf("evaluate empty records: %v", err)
	}
	if verdict.Class != protocol.VerdictInvalid || !contains(verdict.ReasonCodes, protocol.ReasonEvidenceMalformed) {
		t.Fatalf("verdict = %+v, want invalid malformed records", verdict)
	}
}

func TestCalibrationRefusesToOverwriteEvidence(t *testing.T) {
	t.Parallel()

	config := calibration.Config{
		Root: t.TempDir(), Case: protocol.CaseAmbiguousEffect, Probe: protocol.ProbeUnsafe, Trial: 1,
	}
	if _, err := calibration.Run(context.Background(), config); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if _, err := calibration.Run(context.Background(), config); !errors.Is(err, protocol.ErrEvidenceExists) {
		t.Fatalf("second run error = %v, want ErrEvidenceExists", err)
	}
}

func assertRawEvidenceExistsWithoutVerdict(t *testing.T, runDir string) {
	t.Helper()
	for _, name := range protocol.RawEvidenceFiles() {
		if _, err := os.Stat(filepath.Join(runDir, name)); err != nil {
			t.Errorf("raw evidence %s: %v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(runDir, protocol.VerdictFile)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("adapter emitted oracle-owned verdict: %v", err)
	}
}

func assertVerdictFile(t *testing.T, runDir string, want protocol.Verdict) {
	t.Helper()
	got := readJSON[protocol.Verdict](t, filepath.Join(runDir, protocol.VerdictFile))
	if got.Class != want.Class || got.Case != want.Case || got.Probe != want.Probe {
		t.Errorf("persisted verdict = %+v, want %+v", got, want)
	}
}

func readJSON[T any](t *testing.T, path string) T {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var value T
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return value
}

func rewriteAndRehash(t *testing.T, runDir, evidenceName string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatalf("encode %s: %v", evidenceName, err)
	}
	evidencePath := filepath.Join(runDir, evidenceName)
	if err := os.WriteFile(evidencePath, append(data, '\n'), 0o600); err != nil {
		t.Fatalf("rewrite %s: %v", evidenceName, err)
	}
	manifestPath := filepath.Join(runDir, protocol.ManifestFile)
	manifest := readJSON[protocol.Manifest](t, manifestPath)
	manifest.EvidenceSHA256[evidenceName], err = protocol.FileSHA256(evidencePath)
	if err != nil {
		t.Fatalf("rehash %s: %v", evidenceName, err)
	}
	if evidenceName == protocol.EffectiveInputFile {
		manifest.InputSHA256 = manifest.EvidenceSHA256[evidenceName]
	}
	data, err = json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatalf("encode manifest: %v", err)
	}
	if err := os.WriteFile(manifestPath, append(data, '\n'), 0o600); err != nil {
		t.Fatalf("rewrite manifest: %v", err)
	}
}

func appendJSONLEvent(t *testing.T, runDir string, event protocol.Event) {
	t.Helper()
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("encode event: %v", err)
	}
	file, err := os.OpenFile(filepath.Join(runDir, protocol.CommonEventsFile), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open events: %v", err)
	}
	if _, err := file.Write(append(data, '\n')); err != nil {
		_ = file.Close()
		t.Fatalf("append event: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close events: %v", err)
	}
}

func rehashFile(t *testing.T, runDir, evidenceName string) {
	t.Helper()
	manifestPath := filepath.Join(runDir, protocol.ManifestFile)
	manifest := readJSON[protocol.Manifest](t, manifestPath)
	var err error
	manifest.EvidenceSHA256[evidenceName], err = protocol.FileSHA256(filepath.Join(runDir, evidenceName))
	if err != nil {
		t.Fatalf("rehash %s: %v", evidenceName, err)
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatalf("encode manifest: %v", err)
	}
	if err := os.WriteFile(manifestPath, append(data, '\n'), 0o600); err != nil {
		t.Fatalf("rewrite manifest: %v", err)
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
