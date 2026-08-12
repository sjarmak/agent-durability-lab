package codingagent

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestDecodeTransitionRecordConsumesSharedValidFixtures(t *testing.T) {
	root := fixtureRoot(t)
	paths, err := filepath.Glob(filepath.Join(root, "valid", "transition", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	resultPaths, err := filepath.Glob(filepath.Join(root, "valid-result", "transition", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	paths = append(paths, resultPaths...)
	if len(paths) != 15 {
		t.Fatalf("fixture count = %d", len(paths))
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			record, err := DecodeTransitionRecord(data)
			if err != nil {
				t.Fatal(err)
			}
			if !record.Operation.Valid() || record.OccurredAt.IsZero() {
				t.Fatalf("invalid decoded record: %#v", record)
			}
		})
	}
}

func TestStrictDecodersRejectDuplicateKeysUnknownFieldsAndInvalidUTC(t *testing.T) {
	valid, err := os.ReadFile(filepath.Join(fixtureRoot(t), "valid", "transition", "01-claim.json"))
	if err != nil {
		t.Fatal(err)
	}
	duplicate := strings.Replace(string(valid), `"operation":"claim"`, `"operation":"claim","operation":"claim"`, 1)
	if _, err := DecodeTransitionRecord([]byte(duplicate)); err == nil {
		t.Fatal("duplicate transition key accepted")
	}
	unknown := strings.Replace(string(valid), `"schema_version":"1.0.0"`, `"schema_version":"1.0.0","surprise":true`, 1)
	if _, err := DecodeTransitionRecord([]byte(unknown)); err == nil {
		t.Fatal("unknown transition field accepted")
	}
	badTime := strings.Replace(string(valid), "2026-08-11T12:00:00Z", "2026-99-99T99:99:99Z", 1)
	if _, err := DecodeTransitionRecord([]byte(badTime)); err == nil {
		t.Fatal("impossible timestamp accepted")
	}
}

func TestEventAndArtifactValuesRejectSecretsAndUnsafePaths(t *testing.T) {
	event := EventEnvelope{
		SchemaVersion: SchemaVersion, EventID: "event:1", EventType: EventTransitionRecorded,
		SessionID: "session:test", TurnID: "turn:test", OperationID: "operation:test",
		Generation: 1, AuthorityStatus: AuthorityCurrent, Sequence: 1, OccurredAt: testTime,
		Source:    EventSource{Layer: SourceApplication, Component: "binding"},
		Reference: EventReference{TransitionID: "transition:test"},
	}
	if err := event.Validate(); err != nil {
		t.Fatal(err)
	}
	event.Source.Component = ""
	if err := event.Validate(); err == nil {
		t.Fatal("empty event component accepted")
	}
	if err := (ArtifactReference{Path: "evidence/run.json", Digest: Digest("sha256:" + repeat("a", 64))}).Validate(); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"../secret", "/tmp/secret", `dir\\secret`, ""} {
		if err := (ArtifactReference{Path: path, Digest: Digest("sha256:" + repeat("a", 64))}).Validate(); err == nil {
			t.Fatalf("unsafe artifact path %q accepted", path)
		}
	}
}

func TestEventDecoderConsumesSharedFixture(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(fixtureRoot(t), "valid", "event", "event.json"))
	if err != nil {
		t.Fatal(err)
	}
	event, err := DecodeEventEnvelope(data)
	if err != nil {
		t.Fatal(err)
	}
	if event.EventType != EventTransitionRecorded || event.Generation != 2 || event.Reference.TransitionID != "transition:7" {
		t.Fatalf("decoded event drifted from fixture: %#v", event)
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	corpus, err := LoadSchemaCorpus(filepath.Join(filepath.Dir(fixtureRoot(t)), "schema"))
	if err != nil {
		t.Fatal(err)
	}
	if err := corpus.Validate("event", encoded); err != nil {
		t.Fatalf("binding event did not round-trip through wire schema: %v", err)
	}
	for _, name := range []string{"duplicate-key.json", "impossible-timestamp.json", "secret-bearing.json"} {
		data, err := os.ReadFile(filepath.Join(fixtureRoot(t), "invalid", "event", name))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodeEventEnvelope(data); err == nil {
			t.Fatalf("invalid event fixture %s accepted", name)
		}
	}
}

func TestSchemaCorpusAcceptsAndRejectsEverySharedFixture(t *testing.T) {
	root := filepath.Dir(fixtureRoot(t))
	corpus, err := LoadSchemaCorpus(filepath.Join(root, "schema"))
	if err != nil {
		t.Fatal(err)
	}
	validations := []struct {
		validity, kind string
		wantValid      bool
	}{
		{"valid", "identity", true}, {"valid", "transition", true}, {"valid", "event", true}, {"valid", "evidence", true},
		{"valid-result", "transition", true}, {"invalid", "identity", false}, {"invalid", "transition", false}, {"invalid", "event", false},
	}
	for _, validation := range validations {
		paths, err := filepath.Glob(filepath.Join(fixtureRoot(t), validation.validity, validation.kind, "*.json"))
		if err != nil || len(paths) == 0 {
			t.Fatalf("fixtures %s/%s: %v (%d)", validation.validity, validation.kind, err, len(paths))
		}
		for _, fixture := range paths {
			data, err := os.ReadFile(fixture)
			if err != nil {
				t.Fatal(err)
			}
			err = corpus.Validate(validation.kind, data)
			if validation.wantValid && err != nil {
				t.Errorf("valid fixture %s rejected: %v", fixture, err)
			}
			if !validation.wantValid && err == nil {
				t.Errorf("invalid fixture %s accepted", fixture)
			}
		}
	}
}

func TestDestinationCapabilitiesMatchWireContract(t *testing.T) {
	want := []DestinationCapability{
		"atomic_idempotency_key", "transactional_unique_effect_identity", "stable_message_identity",
		"serialized_correlation_lookup", "conditional_versioned_git_mutation", "content_addressed_blob", "manual_reconciliation",
	}
	got := []DestinationCapability{AtomicIdempotencyKey, TransactionalUnique, StableMessageID, SerializedLookup, ConditionalMutation, ContentAddressed, ManualReconciliation}
	for index := range want {
		if got[index] != want[index] {
			t.Errorf("capability %d = %q; want %q", index, got[index], want[index])
		}
	}
	if !(EffectResult{EffectID: "effect:1", DestinationNamespace: "destination:test", Capability: ManualReconciliation, Outcome: EffectUnresolved}).valid() {
		t.Fatal("manual unresolved result without destination receipt was rejected")
	}
}

func TestProviderExecutorUsesWireSessionID(t *testing.T) {
	encoded, err := json.Marshal(ProviderExecutor("provider:test", "session:provider"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "provider_session_id") || !strings.Contains(string(encoded), `"session_id":"session:provider"`) {
		t.Fatalf("provider identity drifted from wire schema: %s", encoded)
	}
}

func TestSchemaCorpusRejectsUnsafeEvidencePathsAndResourceExhaustion(t *testing.T) {
	root := filepath.Dir(fixtureRoot(t))
	corpus, err := LoadSchemaCorpus(filepath.Join(root, "schema"))
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := os.ReadFile(filepath.Join(fixtureRoot(t), "valid", "evidence", "evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	unsafe := strings.Replace(string(evidence), "evidence/episode-protected-001/events.jsonl", "../../etc/passwd", 1)
	if err := corpus.Validate("evidence", []byte(unsafe)); err == nil {
		t.Fatal("traversal artifact path accepted")
	}
	unsafe = strings.Replace(string(evidence), "evidence/episode-protected-001/history.json", "/etc/passwd", 1)
	if err := corpus.Validate("evidence", []byte(unsafe)); err == nil {
		t.Fatal("absolute history path accepted")
	}
	unsafe = strings.Replace(string(evidence), "evidence/episode-protected-001/history.json", "C:/Windows/System32", 1)
	if err := corpus.Validate("evidence", []byte(unsafe)); err == nil {
		t.Fatal("Windows drive-absolute history path accepted")
	}
	if err := corpus.Validate("identity", append([]byte{'"'}, append(make([]byte, MaxJSONDocumentBytes), '"')...)); err == nil {
		t.Fatal("oversized JSON document accepted")
	}
	deep := []byte(strings.Repeat("[", MaxJSONDepth+1) + "0" + strings.Repeat("]", MaxJSONDepth+1))
	if err := corpus.Validate("identity", deep); err == nil {
		t.Fatal("excessively deep JSON accepted")
	}
	if err := corpus.Validate("identity", []byte{'{', '"', 'x', '"', ':', '"', 0xff, '"', '}'}); err == nil {
		t.Fatal("invalid UTF-8 accepted")
	}
}

func fixtureRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "../../../specs/coding-agent-durability/v1/fixtures"))
}
