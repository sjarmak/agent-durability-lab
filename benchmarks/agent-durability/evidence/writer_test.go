package evidence_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/evidence"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

func TestWriteRunPublishesCompleteRawEvidenceWithoutVerdict(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	bundle := validBundle()
	runDir, err := evidence.WriteRun(context.Background(), root, bundle)
	if err != nil {
		t.Fatalf("write run: %v", err)
	}
	if runDir != filepath.Join(root, bundle.Identity.RunID) {
		t.Fatalf("run directory = %q, want %q", runDir, filepath.Join(root, bundle.Identity.RunID))
	}

	for _, name := range protocol.RawEvidenceFiles() {
		info, statErr := os.Stat(filepath.Join(runDir, name))
		if statErr != nil {
			t.Errorf("stat %s: %v", name, statErr)
			continue
		}
		if info.Mode().Perm() != 0o600 {
			t.Errorf("%s mode = %o, want 600", name, info.Mode().Perm())
		}
	}
	if _, err := os.Stat(filepath.Join(runDir, protocol.VerdictFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("verdict should not be written by evidence adapter: %v", err)
	}

	manifest := readJSON[protocol.Manifest](t, filepath.Join(runDir, protocol.ManifestFile))
	if manifest.RunID != bundle.Identity.RunID || manifest.SessionID != bundle.Identity.SessionID {
		t.Fatalf("manifest identity = %+v, want %+v", manifest, bundle.Identity)
	}
	for name, wantHash := range manifest.EvidenceSHA256 {
		gotHash, hashErr := protocol.FileSHA256(filepath.Join(runDir, name))
		if hashErr != nil {
			t.Fatalf("hash %s: %v", name, hashErr)
		}
		if gotHash != wantHash {
			t.Errorf("%s hash = %q, want %q", name, gotHash, wantHash)
		}
	}
	if manifest.InputSHA256 != manifest.EvidenceSHA256[protocol.EffectiveInputFile] {
		t.Fatalf("input hash = %q, want effective input hash", manifest.InputSHA256)
	}
}

func TestWriteRunRefusesToOverwritePublishedEvidence(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	bundle := validBundle()
	runDir, err := evidence.WriteRun(context.Background(), root, bundle)
	if err != nil {
		t.Fatalf("first write: %v", err)
	}
	wantHash, err := protocol.FileSHA256(filepath.Join(runDir, protocol.NativeJournalFile))
	if err != nil {
		t.Fatalf("hash original native journal: %v", err)
	}

	bundle.Native[0].Detail = "replacement"
	if _, err := evidence.WriteRun(context.Background(), root, bundle); !errors.Is(err, protocol.ErrEvidenceExists) {
		t.Fatalf("second write error = %v, want ErrEvidenceExists", err)
	}
	gotHash, err := protocol.FileSHA256(filepath.Join(runDir, protocol.NativeJournalFile))
	if err != nil {
		t.Fatalf("hash preserved native journal: %v", err)
	}
	if gotHash != wantHash {
		t.Fatalf("published evidence changed: hash = %q, want %q", gotHash, wantHash)
	}
}

func TestWriteRunRejectsInvalidBundleBeforePublishing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*evidence.Bundle)
	}{
		{name: "event session mismatch", mutate: func(bundle *evidence.Bundle) { bundle.Events[0].SessionID = "other" }},
		{name: "event sequence gap", mutate: func(bundle *evidence.Bundle) { bundle.Events[0].Sequence = 2 }},
		{name: "event time not increasing", mutate: func(bundle *evidence.Bundle) {
			first := bundle.Events[0]
			bundle.Events = append(bundle.Events, protocol.Event{
				Sequence: 2, Time: first.Time, Kind: protocol.EventBarrierReached,
				SessionID: first.SessionID, ActorID: first.ActorID, Generation: first.Generation,
				ProcessIdentity: first.ProcessIdentity, Decision: "blocked",
			})
		}},
		{name: "invalid native record", mutate: func(bundle *evidence.Bundle) { bundle.Native[0].Detail = "" }},
		{name: "native sequence gap", mutate: func(bundle *evidence.Bundle) { bundle.Native[0].Sequence = 2 }},
		{name: "invalid process observation", mutate: func(bundle *evidence.Bundle) { bundle.Processes[0].ProcessIdentity = "" }},
		{name: "process observation outside event stream", mutate: func(bundle *evidence.Bundle) { bundle.Processes[0].Sequence = 2 }},
		{name: "invalid effective input", mutate: func(bundle *evidence.Bundle) { bundle.Input.AdapterID = "" }},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			bundle := validBundle()
			test.mutate(&bundle)
			if _, err := evidence.WriteRun(context.Background(), root, bundle); !errors.Is(err, protocol.ErrInvalidEvidence) {
				t.Fatalf("write error = %v, want ErrInvalidEvidence", err)
			}
			if _, err := os.Stat(filepath.Join(root, bundle.Identity.RunID)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("invalid run was published: %v", err)
			}
		})
	}
}

func TestWriteRunRejectsRunIdentifiersThatAliasOrEscapeRoot(t *testing.T) {
	t.Parallel()

	for _, runID := range []string{".", "..", "nested/run"} {
		t.Run(runID, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "evidence")
			bundle := validBundle()
			bundle.Identity.RunID = runID
			if _, err := evidence.WriteRun(context.Background(), root, bundle); !errors.Is(err, protocol.ErrInvalidEvidence) {
				t.Fatalf("write %q error = %v, want ErrInvalidEvidence", runID, err)
			}
			if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("unsafe run ID %q created evidence root: %v", runID, err)
			}
		})
	}
}

func TestWriteRunHonorsCanceledContextBeforePublishing(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	root := t.TempDir()
	bundle := validBundle()
	if _, err := evidence.WriteRun(ctx, root, bundle); !errors.Is(err, context.Canceled) {
		t.Fatalf("write error = %v, want context.Canceled", err)
	}
	if _, err := os.Stat(filepath.Join(root, bundle.Identity.RunID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canceled run was published: %v", err)
	}
}

func validBundle() evidence.Bundle {
	now := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)
	sessionID := "session-run-1"
	processIdentity := "pid:101:start:fixture"
	return evidence.Bundle{
		Identity: evidence.RunIdentity{
			RunID: "run-1", Case: protocol.CaseSurvivingExecutor,
			Probe: protocol.ProbeUnfaulted, Trial: 1, SessionID: sessionID,
		},
		Events: []protocol.Event{{
			Sequence: 1, Time: now.Format(time.RFC3339Nano), Kind: protocol.EventExecutorRegistered,
			SessionID: sessionID, ActorID: "agent-1", Generation: 1,
			ProcessIdentity: processIdentity, Decision: "observed",
		}},
		Authority: protocol.AuthorityState{
			SessionID: sessionID, ActiveGeneration: 1, ConcurrentOwnerCount: 1, CurrentOwnerAlive: true,
		},
		Destination: protocol.DestinationState{DestinationID: "destination-1"},
		Fault:       protocol.FaultBoundary{},
		Processes: []protocol.ProcessObservation{{
			Sequence: 1, ActorID: "agent-1", Generation: 1,
			ProcessIdentity: processIdentity, State: "running",
		}},
		Native: []protocol.NativeRecord{{Sequence: 1, Kind: "start", Detail: "live fixture"}},
		Input: protocol.EffectiveInput{
			AdapterID: "live-common", AdapterVersion: "v1", AgentProtocol: protocol.AgentProtocol,
			AgentBinarySHA256:   sha256Hex("live-agent"),
			AuthorityProtocol:   protocol.AuthorityProtocol,
			DestinationProtocol: protocol.DestinationProtocol, DestinationID: "destination-1",
			FailureProtocol: protocol.FailureProtocol, OracleProtocol: protocol.OracleProtocol,
			OracleVisibility: []string{
				protocol.AuthorityStateFile, protocol.DestinationStateFile,
				protocol.FaultBoundaryFile, protocol.ProcessObservationsFile,
			},
			Runtime: "linux/amd64", Settings: map[string]string{"mode": "unfaulted"},
		},
	}
}

func sha256Hex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func readJSON[T any](t *testing.T, path string) T {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", filepath.Base(path), err)
	}
	var value T
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatalf("decode %s: %v", filepath.Base(path), err)
	}
	return value
}
