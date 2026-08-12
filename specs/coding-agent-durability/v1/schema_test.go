package durabilityspec

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

const schemaDirectory = "schema"

var schemaFiles = map[string]string{
	"identity":   "identity.schema.json",
	"transition": "transition.schema.json",
	"event":      "event.schema.json",
	"evidence":   "evidence.schema.json",
}

func TestSchemasCompile(t *testing.T) {
	for schemaName, schemaFile := range schemaFiles {
		t.Run(schemaName, func(t *testing.T) {
			compileSchema(t, schemaFile)
		})
	}
}

func TestValidFixtures(t *testing.T) {
	for schemaName, schemaFile := range schemaFiles {
		t.Run(schemaName, func(t *testing.T) {
			fixtures := fixturePaths(t, "valid", schemaName)
			schema := compileSchema(t, schemaFile)
			for _, fixture := range fixtures {
				t.Run(filepath.Base(fixture), func(t *testing.T) {
					if err := validateFixture(schema, fixture); err != nil {
						t.Fatalf("valid fixture %s was rejected: %v", fixture, err)
					}
				})
			}
		})
	}
}

func TestInvalidFixtures(t *testing.T) {
	tests := []struct {
		name       string
		schemaName string
		fixture    string
		wantErrors []string
	}{
		{name: "malformed", schemaName: "identity", fixture: "malformed.json", wantErrors: []string{"EOF"}},
		{name: "conflicting", schemaName: "transition", fixture: "conflicting-operation.json", wantErrors: []string{"/decision", "false"}},
		{name: "stale authority", schemaName: "transition", fixture: "stale-authority-accepted.json", wantErrors: []string{"/decision", "stale"}},
		{name: "illegal transition", schemaName: "transition", fixture: "illegal-transition.json", wantErrors: []string{"/before/lifecycle", "claimed"}},
		{name: "secret bearing", schemaName: "event", fixture: "secret-bearing.json", wantErrors: []string{"owner_capability", "additional properties"}},
		{name: "missing session identity", schemaName: "identity", fixture: "missing-session-id.json", wantErrors: []string{"missing property", "session_id"}},
		{name: "effect missing effect identity", schemaName: "transition", fixture: "effect-missing-effect-id.json", wantErrors: []string{"effect_id"}},
		{name: "registration missing executor identity", schemaName: "transition", fixture: "register-missing-executor-identity.json", wantErrors: []string{"executor_identity"}},
		{name: "stop missing target", schemaName: "transition", fixture: "stop-missing-target.json", wantErrors: []string{"target"}},
		{name: "stale result carrying success receipt", schemaName: "transition", fixture: "stale-success-receipt.json", wantErrors: []string{"rejection"}},
		{name: "executor cancellation", schemaName: "transition", fixture: "executor-cancel.json", wantErrors: []string{"coordinator"}},
		{name: "current check with stale decision", schemaName: "transition", fixture: "current-stale-decision.json", wantErrors: []string{"decision"}},
		{name: "accepted decision with replay identity", schemaName: "transition", fixture: "accepted-original-transition.json", wantErrors: []string{"original_transition_id"}},
		{name: "duplicate object key", schemaName: "event", fixture: "duplicate-key.json", wantErrors: []string{"duplicate object key", "event_id"}},
		{name: "impossible timestamp shape", schemaName: "event", fixture: "impossible-timestamp.json", wantErrors: []string{"occurred_at", "pattern"}},
		{name: "revoked decision with canceled result", schemaName: "transition", fixture: "revoked-canceled-mismatch.json", wantErrors: []string{"rejection", "revoked"}},
		{name: "canceled decision with revoked result", schemaName: "transition", fixture: "canceled-revoked-mismatch.json", wantErrors: []string{"rejection", "canceled"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schema := compileSchema(t, schemaFiles[test.schemaName])
			fixture := filepath.Join("fixtures", "invalid", test.schemaName, test.fixture)
			if _, err := os.Stat(fixture); err != nil {
				t.Fatalf("required invalid fixture %s is unavailable: %v", fixture, err)
			}
			err := validateFixture(schema, fixture)
			if err == nil {
				t.Fatalf("invalid fixture %s was accepted", fixture)
			}
			for _, wantError := range test.wantErrors {
				if !strings.Contains(err.Error(), wantError) {
					t.Errorf("invalid fixture %s failed with %q; want error containing %q", fixture, err, wantError)
				}
			}
		})
	}
}

func TestValidTypedRejectionsAndStopTargets(t *testing.T) {
	schema := compileSchema(t, schemaFiles["transition"])
	fixtures := fixturePaths(t, "valid-result", "transition")
	for _, fixture := range fixtures {
		t.Run(filepath.Base(fixture), func(t *testing.T) {
			if err := validateFixture(schema, fixture); err != nil {
				t.Fatalf("valid transition result %s was rejected: %v", fixture, err)
			}
		})
	}
}

func TestEvidenceVariants(t *testing.T) {
	schema := compileSchema(t, schemaFiles["evidence"])
	for _, variant := range []string{"unsafe", "protected", "unfaulted"} {
		t.Run(variant, func(t *testing.T) {
			instance := loadFixture(t, filepath.Join("fixtures", "valid", "evidence", "evidence.json"))
			evidence, ok := instance.(map[string]any)
			if !ok {
				t.Fatalf("evidence fixture decoded as %T; want object", instance)
			}
			evidence["variant"] = variant
			if err := schema.Validate(evidence); err != nil {
				t.Fatalf("evidence variant %q was rejected: %v", variant, err)
			}
		})
	}
}

func TestEveryTransitionOperationHasAValidFixture(t *testing.T) {
	wantReceipts := map[string]string{
		"acknowledge_stop":       "stop_ack",
		"attach":                 "attachment",
		"begin_start":            "start",
		"cancel":                 "cancellation",
		"claim":                  "claim",
		"complete":               "completion",
		"mark_unresolved":        "unresolved",
		"observe_progress":       "progress",
		"publish_effect_receipt": "effect",
		"publish_result":         "result",
		"record_stop_delivery":   "stop_delivery",
		"register":               "registration",
		"replace":                "replacement",
	}
	wantAuthorityChecks := map[string]string{
		"acknowledge_stop":       "coordinator",
		"attach":                 "current",
		"begin_start":            "current",
		"cancel":                 "coordinator",
		"claim":                  "coordinator",
		"complete":               "current",
		"mark_unresolved":        "coordinator",
		"observe_progress":       "current",
		"publish_effect_receipt": "current",
		"publish_result":         "current",
		"record_stop_delivery":   "coordinator",
		"register":               "current",
		"replace":                "coordinator",
	}

	paths := fixturePaths(t, "valid", "transition")
	got := make([]string, 0, len(paths))
	for _, path := range paths {
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read fixture %s: %v", path, err)
		}
		var fixture struct {
			SessionID      string `json:"session_id"`
			Operation      string `json:"operation"`
			AuthorityCheck string `json:"authority_check"`
			Receipt        struct {
				Type string `json:"receipt_type"`
			} `json:"receipt"`
		}
		if err := json.Unmarshal(contents, &fixture); err != nil {
			t.Fatalf("decode fixture %s: %v", path, err)
		}
		if fixture.SessionID == "" {
			t.Errorf("fixture %s does not name session_id", path)
		}
		wantReceipt, ok := wantReceipts[fixture.Operation]
		if !ok {
			t.Errorf("fixture %s uses unknown operation %q", path, fixture.Operation)
		} else if fixture.Receipt.Type != wantReceipt {
			t.Errorf("fixture %s receipt type = %q; want %q", path, fixture.Receipt.Type, wantReceipt)
		}
		if wantAuthority := wantAuthorityChecks[fixture.Operation]; fixture.AuthorityCheck != wantAuthority {
			t.Errorf("fixture %s authority check = %q; want %q", path, fixture.AuthorityCheck, wantAuthority)
		}
		got = append(got, fixture.Operation)
	}
	want := make([]string, 0, len(wantReceipts))
	for operation := range wantReceipts {
		want = append(want, operation)
	}
	sort.Strings(want)
	sort.Strings(got)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("valid transition operations = %v; want exactly %v", got, want)
	}
}

func TestSchemaManifestMatchesFiles(t *testing.T) {
	contents, err := os.ReadFile(filepath.Join(schemaDirectory, "schema-manifest.json"))
	if err != nil {
		t.Fatalf("read schema manifest: %v", err)
	}
	var manifest struct {
		SchemaVersion string            `json:"schema_version"`
		Files         map[string]string `json:"files"`
	}
	if err := json.Unmarshal(contents, &manifest); err != nil {
		t.Fatalf("decode schema manifest: %v", err)
	}
	if manifest.SchemaVersion != "1.0.0" {
		t.Fatalf("schema manifest version = %q; want 1.0.0", manifest.SchemaVersion)
	}
	if len(manifest.Files) != len(schemaFiles) {
		t.Fatalf("schema manifest has %d files; want %d", len(manifest.Files), len(schemaFiles))
	}
	for _, schemaFile := range schemaFiles {
		contents, err := os.ReadFile(filepath.Join(schemaDirectory, schemaFile))
		if err != nil {
			t.Fatalf("read schema %s: %v", schemaFile, err)
		}
		sum := sha256.Sum256(contents)
		got := manifest.Files[schemaFile]
		want := "sha256:" + hex.EncodeToString(sum[:])
		if got != want {
			t.Errorf("manifest hash for %s = %q; want %q", schemaFile, got, want)
		}
	}
}

func compileSchema(t *testing.T, schemaFile string) *jsonschema.Schema {
	t.Helper()
	compiler := jsonschema.NewCompiler()
	schema, err := compiler.Compile(filepath.Join(schemaDirectory, schemaFile))
	if err != nil {
		t.Fatalf("compile schema %s: %v", schemaFile, err)
	}
	return schema
}

func fixturePaths(t *testing.T, validity, schemaName string) []string {
	t.Helper()
	pattern := filepath.Join("fixtures", validity, schemaName, "*.json")
	paths, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatalf("find fixtures matching %s: %v", pattern, err)
	}
	if len(paths) == 0 {
		t.Fatalf("no %s fixtures for %s", validity, schemaName)
	}
	return paths
}

func validateFixture(schema *jsonschema.Schema, path string) error {
	instance, err := decodeFixture(path)
	if err != nil {
		return err
	}
	if err := schema.Validate(instance); err != nil {
		return fmt.Errorf("validate fixture: %w", err)
	}
	return nil
}

func loadFixture(t *testing.T, path string) any {
	t.Helper()
	instance, err := decodeFixture(path)
	if err != nil {
		t.Fatalf("decode fixture %s: %v", path, err)
	}
	return instance
}

func decodeFixture(path string) (any, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("open fixture: %w", err)
	}
	if err := rejectDuplicateObjectKeys(bytes.NewReader(contents)); err != nil {
		return nil, fmt.Errorf("decode fixture: %w", err)
	}
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(contents))
	if err != nil {
		return nil, fmt.Errorf("decode fixture: %w", err)
	}
	return instance, nil
}

func rejectDuplicateObjectKeys(reader io.Reader) error {
	decoder := json.NewDecoder(reader)
	if err := scanJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return err
	}
	return nil
}

func scanJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		keys := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("object key decoded as %T", keyToken)
			}
			if _, exists := keys[key]; exists {
				return fmt.Errorf("duplicate object key %q", key)
			}
			keys[key] = struct{}{}
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unexpected delimiter %q", delimiter)
	}
	_, err = decoder.Token()
	return err
}
