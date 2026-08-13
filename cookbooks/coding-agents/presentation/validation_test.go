package presentation_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/sjarmak/temporal_projects/cookbooks/coding-agents/presentation"
)

func TestValidateRejectsClaimAndEvidenceDrift(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*presentation.Catalog)
		want   string
	}{
		{name: "schema", mutate: func(value *presentation.Catalog) { value.SchemaVersion = "presentation-v0" }, want: "schema version"},
		{name: "anti-goal", mutate: func(value *presentation.Catalog) { value.Product.AntiGoals = value.Product.AntiGoals[:3] }, want: "anti-goals"},
		{name: "section order", mutate: func(value *presentation.Catalog) {
			value.Sections[0], value.Sections[1] = value.Sections[1], value.Sections[0]
		}, want: "section 0"},
		{name: "unsafe false green", mutate: func(value *presentation.Catalog) {
			value.Scenarios[0].Episodes[1].Verdict = presentation.VerdictValidPass
		}, want: "unsafe"},
		{name: "protected false failure", mutate: func(value *presentation.Catalog) {
			value.Scenarios[0].Episodes[2].Verdict = presentation.VerdictValidFail
		}, want: "protected"},
		{name: "path traversal", mutate: func(value *presentation.Catalog) {
			value.Scenarios[0].Episodes[2].NativeHistory.Path = "../history.json"
		}, want: "confined"},
		{name: "Windows path", mutate: func(value *presentation.Catalog) {
			value.Scenarios[0].Episodes[2].NativeHistory.Path = "C:/history.json"
		}, want: "confined"},
		{name: "digest", mutate: func(value *presentation.Catalog) { value.Scenarios[0].Episodes[2].NativeHistory.SHA256 = "sha256:nope" }, want: "SHA-256"},
		{name: "non UTC event", mutate: func(value *presentation.Catalog) {
			value.Scenarios[0].Episodes[2].Events[0].OccurredAt = "2026-08-11T08:00:00-04:00"
		}, want: "UTC"},
		{name: "event order", mutate: func(value *presentation.Catalog) { value.Scenarios[0].Episodes[2].Events[1].Sequence = 1 }, want: "sequence"},
		{name: "raw capability", mutate: func(value *presentation.Catalog) {
			value.Scenarios[0].Episodes[2].Authority[0].CapabilityDigest = "owner-capability-secret"
		}, want: "capability digest"},
		{name: "native history", mutate: func(value *presentation.Catalog) {
			value.Scenarios[0].Episodes[2].NativeHistory = presentation.ArtifactLink{}
		}, want: "native history"},
		{name: "replay", mutate: func(value *presentation.Catalog) {
			value.Scenarios[0].Episodes[2].Provenance.ReplayStatus = presentation.ReplayFailed
		}, want: "replay"},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			value := validCatalog()
			test.mutate(&value)
			err := presentation.Validate(value)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate() error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestValidateCancellationChronology(t *testing.T) {
	t.Parallel()
	value := validCatalog()
	episode := &value.Scenarios[0].Episodes[2]
	episode.Outcome.TerminalState = presentation.TerminalCanceled
	episode.Cancellation = &presentation.CancellationView{
		RequestedAt:        "2026-08-11T12:00:03Z",
		AuthorityRevokedAt: "2026-08-11T12:00:04Z",
		StopDeliveredAt:    "2026-08-11T12:00:05Z",
		AcknowledgedAt:     "2026-08-11T12:00:06Z",
		Disposition:        presentation.StopExited,
	}
	if err := presentation.Validate(value); err != nil {
		t.Fatalf("valid cancellation: %v", err)
	}
	episode.Cancellation.AuthorityRevokedAt = "2026-08-11T12:00:07Z"
	if err := presentation.Validate(value); err == nil || !strings.Contains(err.Error(), "revocation before stop") {
		t.Fatalf("cancellation order error = %v", err)
	}
}

func TestValidatePreservesAuthorityFailuresAsObservations(t *testing.T) {
	t.Parallel()
	value := validCatalog()
	unsafe := &value.Scenarios[0].Episodes[1]
	unsafe.Authority = []presentation.AuthorityView{{
		Sequence: 1, Generation: 0, Status: presentation.AuthorityAbsent,
		Actor: "unsafe-direct-launch", OccurredAt: "2026-08-11T12:00:00Z",
	}}
	protected := &value.Scenarios[0].Episodes[2]
	protected.Authority[len(protected.Authority)-1].Status = presentation.AuthorityCurrent

	if err := presentation.Validate(value); err != nil {
		t.Fatalf("Validate() rejected faithfully observed authority state: %v", err)
	}
}

func TestValidateRejectsIncompletePresentationBoundaries(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*presentation.Catalog)
		want   string
	}{
		{name: "blank product", mutate: func(value *presentation.Catalog) { value.Product.Audience = " " }, want: "product fields"},
		{name: "duplicate scenario", mutate: func(value *presentation.Catalog) {
			value.Scenarios = append(value.Scenarios, value.Scenarios[0])
		}, want: "duplicates ID"},
		{name: "no scenarios", mutate: func(value *presentation.Catalog) { value.Scenarios = nil }, want: "contain scenarios"},
		{name: "bad scenario slug", mutate: func(value *presentation.Catalog) { value.Scenarios[0].Slug = "different" }, want: "ID and slug"},
		{name: "missing narrative", mutate: func(value *presentation.Catalog) { value.Scenarios[0].Question = "" }, want: "narrative"},
		{name: "bad claim maturity", mutate: func(value *presentation.Catalog) { value.Scenarios[0].Claim.Maturity = "settled" }, want: "maturity"},
		{name: "claim without evidence", mutate: func(value *presentation.Catalog) { value.Scenarios[0].Claim.Evidence = nil }, want: "link evidence"},
		{name: "missing responsibility", mutate: func(value *presentation.Catalog) { value.Scenarios[0].Responsibility.Destination = "" }, want: "responsibility split"},
		{name: "wrong episode count", mutate: func(value *presentation.Catalog) { value.Scenarios[0].Episodes = value.Scenarios[0].Episodes[:2] }, want: "episodes must contain"},
		{name: "missing episode ID", mutate: func(value *presentation.Catalog) { value.Scenarios[0].Episodes[0].ID = "" }, want: "episode ID"},
		{name: "invalid terminal", mutate: func(value *presentation.Catalog) { value.Scenarios[0].Episodes[0].Outcome.TerminalState = "running" }, want: "terminal state"},
		{name: "negative effect count", mutate: func(value *presentation.Catalog) { value.Scenarios[0].Episodes[0].Outcome.PhysicalEffectCount = -1 }, want: "outcome"},
		{name: "missing accepted result", mutate: func(value *presentation.Catalog) { value.Scenarios[0].Episodes[0].Outcome.AcceptedResultID = "" }, want: "accepted result"},
		{name: "missing delivery identity", mutate: func(value *presentation.Catalog) { value.Scenarios[0].Episodes[0].Identities.Delivery.RunID = "" }, want: "identities"},
		{name: "missing authority", mutate: func(value *presentation.Catalog) { value.Scenarios[0].Episodes[0].Authority = nil }, want: "authority history"},
		{name: "invalid authority status", mutate: func(value *presentation.Catalog) { value.Scenarios[0].Episodes[0].Authority[0].Status = "pending" }, want: "status"},
		{name: "missing effects", mutate: func(value *presentation.Catalog) { value.Scenarios[0].Episodes[0].Effects = nil }, want: "effect observations"},
		{name: "wrong effect identity", mutate: func(value *presentation.Catalog) { value.Scenarios[0].Episodes[0].Effects[0].EffectID = "effect:other" }, want: "identity or receipt"},
		{name: "invalid effect disposition", mutate: func(value *presentation.Catalog) { value.Scenarios[0].Episodes[0].Effects[0].Disposition = "maybe" }, want: "disposition"},
		{name: "canceled without chronology", mutate: func(value *presentation.Catalog) {
			value.Scenarios[0].Episodes[0].Outcome.TerminalState = presentation.TerminalCanceled
		}, want: "lacks cancellation"},
		{name: "cancellation on success", mutate: func(value *presentation.Catalog) {
			value.Scenarios[0].Episodes[0].Cancellation = &presentation.CancellationView{}
		}, want: "requires canceled"},
		{name: "missing events", mutate: func(value *presentation.Catalog) { value.Scenarios[0].Episodes[0].Events = nil }, want: "events are required"},
		{name: "invalid event lane", mutate: func(value *presentation.Catalog) { value.Scenarios[0].Episodes[0].Events[0].Lane = "provider" }, want: "metadata"},
		{name: "missing raw evidence", mutate: func(value *presentation.Catalog) { value.Scenarios[0].Episodes[0].RawEvidence = nil }, want: "raw evidence"},
		{name: "invalid evidence root", mutate: func(value *presentation.Catalog) {
			value.Scenarios[0].Episodes[0].Provenance.EvidenceRoot = "/tmp/evidence"
		}, want: "provenance root"},
		{name: "missing artifact label", mutate: func(value *presentation.Catalog) { value.Scenarios[0].Claim.Evidence[0].Label = "" }, want: "label"},
		{name: "backslash path", mutate: func(value *presentation.Catalog) { value.Scenarios[0].Claim.Evidence[0].Path = `evidence\\report.json` }, want: "confined"},
		{name: "script link", mutate: func(value *presentation.Catalog) { value.Sections[0].Href = "javascript:alert(1)" }, want: "metadata"},
		{name: "query link", mutate: func(value *presentation.Catalog) { value.Scenarios[0].RecipeHref = "patterns/?next=/outside" }, want: "recipe link"},
		{name: "fragment link", mutate: func(value *presentation.Catalog) { value.Scenarios[0].RecipeHref = "patterns/#outside" }, want: "recipe link"},
		{name: "encoded path", mutate: func(value *presentation.Catalog) { value.Scenarios[0].RecipeHref = "patterns/%2e%2e/outside" }, want: "recipe link"},
		{name: "archive member traversal", mutate: func(value *presentation.Catalog) {
			value.Scenarios[0].Episodes[0].NativeHistory.ArchiveMember = "../history.json"
		}, want: "archive member"},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			value := validCatalog()
			test.mutate(&value)
			err := presentation.Validate(value)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate() error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestDecodeJSONRejectsUntrustedDocuments(t *testing.T) {
	t.Parallel()
	encoded, err := json.Marshal(validCatalog())
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		data []byte
	}{
		{name: "empty", data: nil},
		{name: "unknown field", data: []byte(`{"schema_version":"coding-agent-presentation-v1","unknown":true}`)},
		{name: "duplicate key", data: []byte(`{"schema_version":"coding-agent-presentation-v1","schema_version":"coding-agent-presentation-v1"}`)},
		{name: "invalid UTF-8", data: []byte{'{', '"', 'x', '"', ':', '"', 0xff, '"', '}'}},
		{name: "trailing JSON", data: append(append([]byte{}, encoded...), []byte(` {}`)...)},
		{name: "excessive depth", data: []byte(strings.Repeat("[", 65) + "null" + strings.Repeat("]", 65))},
		{name: "excessive items", data: []byte("[" + strings.Repeat("null,", 10_000) + "null]")},
		{name: "oversized", data: []byte(`{"schema_version":"` + strings.Repeat("x", presentation.MaxJSONDocumentBytes) + `"}`)},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := presentation.DecodeJSON(test.data); err == nil {
				t.Fatal("DecodeJSON() accepted invalid input")
			}
		})
	}
}
