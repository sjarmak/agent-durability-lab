package v2_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/calibration"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/oracle"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
)

func TestABACalibrationProducesDistinguishingIndependentVerdicts(t *testing.T) {
	t.Parallel()

	for _, probe := range []protocol.Probe{protocol.ProbeUnfaulted, protocol.ProbeUnsafe, protocol.ProbeProtected} {
		probe := probe
		t.Run(string(probe), func(t *testing.T) {
			t.Parallel()
			for trial := 1; trial <= 3; trial++ {
				runDir, err := calibration.Run(context.Background(), calibration.Config{
					Root: t.TempDir(), Case: protocol.CaseABAReacquisition, Probe: probe, Trial: trial,
				})
				if err != nil {
					t.Fatalf("run trial %d: %v", trial, err)
				}
				for _, name := range protocol.RawEvidenceFiles() {
					if _, err := os.Stat(filepath.Join(runDir, name)); err != nil {
						t.Errorf("raw evidence %s: %v", name, err)
					}
				}
				if _, err := os.Stat(filepath.Join(runDir, protocol.VerdictFile)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("adapter wrote verdict: %v", err)
				}

				verdict, err := oracle.EvaluateAndWrite(context.Background(), runDir)
				if err != nil {
					t.Fatalf("evaluate trial %d: %v", trial, err)
				}
				if verdict.Admission != protocol.AdmissionValid || verdict.Correctness != protocol.OutcomePass || verdict.Diagnosability != protocol.OutcomePass {
					t.Errorf("trial %d base outcomes = %+v", trial, verdict)
				}
				if probe == protocol.ProbeUnsafe {
					if verdict.Safety != protocol.OutcomeFail || verdict.Liveness != protocol.OutcomeFail || verdict.EfficiencyEligible ||
						verdict.Metrics.StaleActionAcceptCount < 1 {
						t.Errorf("unsafe trial %d did not distinguish label-only authority: %+v", trial, verdict)
					}
				} else if verdict.Safety != protocol.OutcomePass || verdict.Liveness != protocol.OutcomePass || !verdict.EfficiencyEligible ||
					verdict.Metrics.StaleActionAcceptCount != 0 {
					t.Errorf("safe trial %d outcomes = %+v", trial, verdict)
				}
			}
		})
	}
}

func TestABAOracleRejectsRehashedCausalContradictionAsInvalid(t *testing.T) {
	t.Parallel()

	runDir, err := calibration.Run(context.Background(), calibration.Config{
		Root: t.TempDir(), Case: protocol.CaseABAReacquisition, Probe: protocol.ProbeProtected, Trial: 1,
	})
	if err != nil {
		t.Fatalf("run calibration: %v", err)
	}
	eventsPath := filepath.Join(runDir, protocol.CausalEventsFile)
	events := readJSONL[protocol.CausalEvent](t, eventsPath)
	events[len(events)-1].Generation = 7
	writeJSONL(t, eventsPath, events)
	rehash(t, runDir, protocol.CausalEventsFile)

	verdict, err := oracle.EvaluateAndWrite(context.Background(), runDir)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if verdict.Admission != protocol.AdmissionInvalid || verdict.Correctness != protocol.OutcomeNotApplicable ||
		!hasReason(verdict.ReasonCodes, protocol.ReasonEvidenceInconsistent) {
		t.Fatalf("verdict = %+v, want invalid inconsistent evidence", verdict)
	}
}

func TestRecoveryDynamicsCalibrationsCoverEveryProfileAndDistinguishControls(t *testing.T) {
	t.Parallel()

	cases := []protocol.CaseID{
		protocol.CaseLayeredRetryAmplification,
		protocol.CaseOutageBacklogRecovery,
		protocol.CaseBackpressureOverload,
		protocol.CasePoisonWorkIsolation,
		protocol.CaseSilentProgress,
	}
	for _, benchmarkCase := range cases {
		benchmarkCase := benchmarkCase
		t.Run(string(benchmarkCase), func(t *testing.T) {
			t.Parallel()
			for _, probe := range []protocol.Probe{protocol.ProbeUnfaulted, protocol.ProbeUnsafe, protocol.ProbeProtected} {
				for trial := 1; trial <= 3; trial++ {
					runDir, err := calibration.Run(context.Background(), calibration.Config{
						Root: t.TempDir(), Case: benchmarkCase, Probe: probe, Trial: trial,
					})
					if err != nil {
						t.Fatalf("%s trial %d: %v", probe, trial, err)
					}
					verdict, err := oracle.EvaluateAndWrite(context.Background(), runDir)
					if err != nil {
						t.Fatalf("evaluate %s trial %d: %v", probe, trial, err)
					}
					if verdict.Admission != protocol.AdmissionValid || verdict.Diagnosability != protocol.OutcomePass {
						t.Fatalf("%s trial %d invalid: %+v", probe, trial, verdict)
					}
					if probe == protocol.ProbeUnsafe {
						if verdict.Safety != protocol.OutcomeFail && verdict.Liveness != protocol.OutcomeFail && verdict.Correctness != protocol.OutcomeFail {
							t.Errorf("unsafe trial %d did not distinguish: %+v", trial, verdict)
						}
						if verdict.EfficiencyEligible {
							t.Errorf("unsafe trial %d passed parity gate: %+v", trial, verdict)
						}
					} else if verdict.Correctness != protocol.OutcomePass || verdict.Safety != protocol.OutcomePass ||
						verdict.Liveness != protocol.OutcomePass || !verdict.EfficiencyEligible {
						t.Errorf("%s trial %d failed: %+v", probe, trial, verdict)
					}
					if verdict.Metrics.PhysicalRequestCount < 1 || verdict.Metrics.DurableRecordCount < 1 || verdict.Metrics.DurableBytes < 1 {
						t.Errorf("%s trial %d lacks workload metrics: %+v", probe, trial, verdict.Metrics)
					}
				}
			}
		})
	}
}

func TestRecoveryOracleFailsClosedOnRehashedBoundaryAndIdentityContradictions(t *testing.T) {
	t.Parallel()

	t.Run("fault boundary", func(t *testing.T) {
		runDir, err := calibration.Run(context.Background(), calibration.Config{
			Root: t.TempDir(), Case: protocol.CaseOutageBacklogRecovery, Probe: protocol.ProbeProtected, Trial: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(runDir, protocol.FaultBoundaryFile)
		fault := readJSONFile[protocol.FaultBoundary](t, path)
		events := readJSONL[protocol.CausalEvent](t, filepath.Join(runDir, protocol.CausalEventsFile))
		fault.TriggeredAt = events[fault.AfterSequence-1].Time
		writeJSONFile(t, path, fault)
		rehash(t, runDir, protocol.FaultBoundaryFile)
		verdict, err := oracle.EvaluateAndWrite(context.Background(), runDir)
		if err != nil {
			t.Fatal(err)
		}
		if verdict.Admission != protocol.AdmissionInvalid || !hasReason(verdict.ReasonCodes, protocol.ReasonEvidenceMalformed) {
			t.Fatalf("verdict = %+v", verdict)
		}
	})

	t.Run("process identity", func(t *testing.T) {
		runDir, err := calibration.Run(context.Background(), calibration.Config{
			Root: t.TempDir(), Case: protocol.CasePoisonWorkIsolation, Probe: protocol.ProbeProtected, Trial: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(runDir, protocol.ProcessObservationsFile)
		processes := readJSONFile[[]protocol.ProcessObservation](t, path)
		processes[0].ProcessIdentity = "pid:99999:start:contradiction"
		writeJSONFile(t, path, processes)
		rehash(t, runDir, protocol.ProcessObservationsFile)
		verdict, err := oracle.EvaluateAndWrite(context.Background(), runDir)
		if err != nil {
			t.Fatal(err)
		}
		if verdict.Admission != protocol.AdmissionInvalid || !hasReason(verdict.ReasonCodes, protocol.ReasonEvidenceInconsistent) {
			t.Fatalf("verdict = %+v", verdict)
		}
	})
}

func readJSONL[T any](t *testing.T, path string) []T {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	var values []T
	for decoder.More() {
		var value T
		if err := decoder.Decode(&value); err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
		values = append(values, value)
	}
	return values
}

func writeJSONL[T any](t *testing.T, path string, values []T) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	encoder := json.NewEncoder(file)
	for _, value := range values {
		if err := encoder.Encode(value); err != nil {
			_ = file.Close()
			t.Fatalf("encode %s: %v", path, err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close %s: %v", path, err)
	}
}

func writeJSONFile[T any](t *testing.T, path string, value T) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

func rehash(t *testing.T, runDir, name string) {
	t.Helper()
	manifestPath := filepath.Join(runDir, protocol.ManifestFile)
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var manifest protocol.Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	manifest.EvidenceSHA256[name], err = protocol.FileSHA256(filepath.Join(runDir, name))
	if err != nil {
		t.Fatalf("hash %s: %v", name, err)
	}
	data, err = json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatalf("encode manifest: %v", err)
	}
	if err := os.WriteFile(manifestPath, append(data, '\n'), 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
}

func hasReason(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
