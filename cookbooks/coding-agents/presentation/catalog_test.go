package presentation_test

import (
	"encoding/json"
	"testing"

	"github.com/sjarmak/temporal_projects/cookbooks/coding-agents/presentation"
)

func TestCatalogSupportsFirstTrustworthyRecoveryJourney(t *testing.T) {
	t.Parallel()
	catalog := validCatalog()

	if err := presentation.Validate(catalog); err != nil {
		t.Fatalf("validate catalog: %v", err)
	}
	encoded, err := json.Marshal(catalog)
	if err != nil {
		t.Fatalf("marshal catalog: %v", err)
	}
	decoded, err := presentation.DecodeJSON(encoded)
	if err != nil {
		t.Fatalf("decode catalog: %v", err)
	}
	if err := presentation.Validate(decoded); err != nil {
		t.Fatalf("validate decoded catalog: %v", err)
	}

	scenario := decoded.Scenarios[0]
	if len(scenario.Episodes) != 3 {
		t.Fatalf("episodes = %d, want 3", len(scenario.Episodes))
	}
	if scenario.Episodes[1].Variant != presentation.VariantUnsafe ||
		scenario.Episodes[1].Verdict != presentation.VerdictValidFail {
		t.Fatalf("unsafe episode = %#v", scenario.Episodes[1])
	}
	if scenario.Episodes[2].NativeHistory.Path == "" ||
		scenario.Episodes[2].Provenance.ReplayStatus != presentation.ReplayPassed {
		t.Fatalf("protected evidence is incomplete: %#v", scenario.Episodes[2])
	}
}

func validCatalog() presentation.Catalog {
	return presentation.Catalog{
		SchemaVersion: presentation.SchemaVersion,
		Product: presentation.Product{
			Name:           "Fault-Tested Durability Patterns for Coding Agents",
			Positioning:    "Evidence-backed recovery patterns, not a successful Workflow demo.",
			Audience:       "Backend and platform engineers accountable for long-running coding agents.",
			PrimaryOutcome: "First trustworthy recovery from an exact declared fault.",
			AntiGoals: []presentation.AntiGoal{
				presentation.AntiGoalGenericRuntimeParity,
				presentation.AntiGoalExactlyOnce,
				presentation.AntiGoalControlledComputePerformance,
				presentation.AntiGoalUnadmittedProviderCompatibility,
			},
		},
		Sections: []presentation.Section{
			{ID: presentation.SectionStart, Title: "Start", Purpose: "Run one trustworthy recovery.", Href: "quickstart/"},
			{ID: presentation.SectionPatterns, Title: "Patterns", Purpose: "Apply the six durability patterns.", Href: "patterns/"},
			{ID: presentation.SectionScenarios, Title: "Scenarios", Purpose: "Compare unsafe and protected fault outcomes.", Href: "scenarios/"},
			{ID: presentation.SectionEvidence, Title: "Evidence", Purpose: "Inspect raw observations and replay.", Href: "evidence/"},
			{ID: presentation.SectionProtocol, Title: "Protocol", Purpose: "Integrate stable identity and authority.", Href: "protocol/"},
			{ID: presentation.SectionResearch, Title: "Research", Purpose: "Review findings, limits, and open questions.", Href: "research/"},
		},
		Scenarios: []presentation.Scenario{validScenario()},
	}
}

func validScenario() presentation.Scenario {
	return presentation.Scenario{
		ID:              "effect-after-commit",
		Slug:            "effect-after-commit",
		Title:           "Effect commits before Activity completion",
		Summary:         "A stable effect identity lets the destination return the committed receipt after redelivery.",
		Question:        "Does Worker recovery preserve application effect cardinality?",
		Invariant:       "One logical operation produces at most one committed destination effect.",
		FailureBoundary: "The Worker exits after the destination commit and before Activity completion.",
		Falsifier:       "The protected arm commits twice, loses the receipt, or cannot replay its history.",
		RecipeHref:      "patterns/effect-safe-tools/",
		Claim: presentation.Claim{
			Statement: "Temporal redelivers procedure; stable effect identity plus a destination protocol preserves one committed effect at this boundary.",
			Scope:     "Pinned local destination and recorded fault boundary.",
			Maturity:  presentation.MaturityNormative,
			Evidence: []presentation.ArtifactLink{
				artifact("docs/findings/0004-one-temporal-completion-can-hide-two-effects.md", "finding"),
			},
		},
		Responsibility: presentation.ResponsibilitySplit{
			Temporal:    "Records and redelivers the Activity procedure.",
			Application: "Reuses the stable operation and effect identities.",
			Destination: "Atomically returns the existing receipt for the repeated effect identity.",
			Executor:    "Reports delivery and process observations without becoming logical authority.",
		},
		Episodes: []presentation.Episode{
			episode("unfaulted-1", presentation.VariantUnfaulted, presentation.VerdictValidPass, 1),
			episode("unsafe-1", presentation.VariantUnsafe, presentation.VerdictValidFail, 2),
			episode("protected-1", presentation.VariantProtected, presentation.VerdictValidPass, 1),
		},
	}
}

func episode(id string, variant presentation.Variant, verdict presentation.Verdict, physicalEffects int) presentation.Episode {
	value := presentation.Episode{
		ID:      id,
		Variant: variant,
		Verdict: verdict,
		Outcome: presentation.OutcomeView{
			TerminalState:       presentation.TerminalSucceeded,
			Summary:             "Workflow completed after destination reconciliation.",
			AcceptedResultID:    "result:tool-call-7",
			PhysicalEffectCount: physicalEffects,
		},
		Identities: presentation.IdentityView{
			SessionID:   "session:reviewer",
			TurnID:      "turn:42",
			OperationID: "operation:tool-call-7",
			EffectID:    "effect:tool-call-7",
			Delivery: presentation.DeliveryIdentity{
				WorkflowID:      "workflow:reviewer",
				RunID:           "run:001",
				ActivityID:      "activity:tool-call-7",
				ActivityAttempt: 2,
				WorkerIdentity:  "worker:replacement",
				ProcessIdentity: "process:replacement",
			},
		},
		Authority: []presentation.AuthorityView{
			{Sequence: 1, Generation: 1, Status: presentation.AuthorityRevoked, CapabilityDigest: digest('1'), Actor: "coordinator", OccurredAt: "2026-08-11T12:00:00Z"},
			{Sequence: 2, Generation: 2, Status: presentation.AuthorityCurrent, CapabilityDigest: digest('2'), Actor: "executor", OccurredAt: "2026-08-11T12:00:01Z"},
			{Sequence: 3, Generation: 2, Status: presentation.AuthorityRevoked, CapabilityDigest: digest('2'), Actor: "executor", OccurredAt: "2026-08-11T12:00:03Z"},
		},
		Effects: []presentation.EffectView{
			{EffectID: "effect:tool-call-7", Disposition: presentation.EffectCommitted, ReceiptID: "receipt:tool-call-7", Summary: "Destination committed the stable effect."},
		},
		Events: []presentation.EventView{
			{Sequence: 1, OccurredAt: "2026-08-11T12:00:00Z", Lane: presentation.LaneTemporal, EventType: "activity_redelivered", Summary: "Temporal scheduled another Activity attempt.", References: []string{"activity:tool-call-7"}},
			{Sequence: 2, OccurredAt: "2026-08-11T12:00:01Z", Lane: presentation.LaneApplication, EventType: "authority_checked", Summary: "The replacement generation was current.", References: []string{"operation:tool-call-7"}},
			{Sequence: 3, OccurredAt: "2026-08-11T12:00:02Z", Lane: presentation.LaneDestination, EventType: "effect_receipt_recorded", Summary: "The destination returned the durable receipt.", References: []string{"receipt:tool-call-7"}},
			{Sequence: 4, OccurredAt: "2026-08-11T12:00:03Z", Lane: presentation.LaneApplication, EventType: "turn_completed", Summary: "The current generation completed and authority was revoked.", References: []string{"result:tool-call-7"}},
		},
		NativeHistory: artifact("evidence/effect-after-commit/history.json", "Temporal Event History"),
		RawEvidence: []presentation.ArtifactLink{
			artifact("evidence/effect-after-commit/events.jsonl", "raw events"),
		},
		Provenance: presentation.ProvenanceView{
			EvidenceRoot:   "evidence/effect-after-commit",
			Manifest:       artifact("evidence/effect-after-commit/manifest.json", "manifest"),
			Report:         artifact("evidence/effect-after-commit/report.json", "report"),
			SourceRevision: "0123456789abcdef",
			AuditCommand:   "./cookbooks/coding-agents/run-all.sh check",
			ReplayStatus:   presentation.ReplayPassed,
			CorrectionLineage: []presentation.ArtifactLink{
				artifact("evidence/effect-after-commit-v0/report.json", "superseded report"),
			},
		},
	}
	if variant == presentation.VariantUnsafe {
		value.Effects = append(value.Effects, presentation.EffectView{
			EffectID: "effect:tool-call-7", Disposition: presentation.EffectCommitted,
			ReceiptID: "receipt:tool-call-7-duplicate", Summary: "Unsafe redelivery committed a second physical effect.",
		})
	}
	return value
}

func artifact(path, label string) presentation.ArtifactLink {
	return presentation.ArtifactLink{Path: path, SHA256: digest('a'), MediaType: "application/json", Label: label}
}

func digest(value byte) string {
	data := make([]byte, 64)
	for index := range data {
		data[index] = value
	}
	return "sha256:" + string(data)
}
