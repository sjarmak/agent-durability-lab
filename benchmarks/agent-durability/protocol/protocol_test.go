package protocol

import "testing"

func TestManifestValidationRejectsUnknownCaseAndIncompleteIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		manifest Manifest
	}{
		{name: "empty", manifest: Manifest{}},
		{name: "unknown case", manifest: validManifest(CaseID("unknown"))},
		{name: "zero trial", manifest: func() Manifest { value := validManifest(CaseAmbiguousEffect); value.Trial = 0; return value }()},
		{name: "missing inventory hash", manifest: func() Manifest { value := validManifest(CaseAmbiguousEffect); value.InputSHA256 = ""; return value }()},
		{name: "unexpected evidence path", manifest: func() Manifest {
			value := validManifest(CaseAmbiguousEffect)
			value.EvidenceSHA256["../outside"] = "hash"
			return value
		}()},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if err := test.manifest.Validate(); err == nil {
				t.Fatal("Validate() succeeded, want error")
			}
		})
	}
}

func TestEventValidationRequiresStableIdentity(t *testing.T) {
	t.Parallel()

	event := Event{Sequence: 1, Time: "2026-08-07T00:00:00Z", Kind: EventExecutorRegistered, SessionID: "session-1", ActorID: "agent-1", Generation: 1, Decision: "observed"}
	if err := event.Validate(); err == nil {
		t.Fatal("Validate() succeeded without process identity")
	}
	event.ProcessIdentity = "pid:101:start:fixture"
	if err := event.Validate(); err != nil {
		t.Fatalf("Validate() failed: %v", err)
	}
}

func validManifest(benchmarkCase CaseID) Manifest {
	hashes := make(map[string]string)
	for _, name := range RawEvidenceFiles()[1:] {
		hashes[name] = "hash"
	}
	return Manifest{
		ContractVersion: ContractVersion, RunID: "run-1", Case: benchmarkCase,
		Probe: ProbeProtected, Trial: 1, SessionID: "session-1", InputSHA256: "abc",
		EvidenceSHA256: hashes,
	}
}
