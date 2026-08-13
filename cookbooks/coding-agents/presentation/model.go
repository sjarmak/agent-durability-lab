// Package presentation defines the read-only, non-authoritative view model used
// to explain verified coding-agent durability evidence. It does not write,
// admit, or independently verify evidence.
package presentation

const (
	SchemaVersion        = "coding-agent-presentation-v1"
	MaxJSONDocumentBytes = 1 << 20
)

type AntiGoal string

const (
	AntiGoalGenericRuntimeParity            AntiGoal = "generic-runtime-parity"
	AntiGoalExactlyOnce                     AntiGoal = "exactly-once"
	AntiGoalControlledComputePerformance    AntiGoal = "controlled-compute-performance"
	AntiGoalUnadmittedProviderCompatibility AntiGoal = "unadmitted-provider-compatibility"
)

type SectionID string

const (
	SectionStart     SectionID = "start"
	SectionPatterns  SectionID = "patterns"
	SectionScenarios SectionID = "scenarios"
	SectionEvidence  SectionID = "evidence"
	SectionProtocol  SectionID = "protocol"
	SectionResearch  SectionID = "research"
)

type Maturity string

const (
	MaturityNormative    Maturity = "normative"
	MaturityExperimental Maturity = "experimental"
)

type Variant string

const (
	VariantUnfaulted Variant = "unfaulted"
	VariantUnsafe    Variant = "unsafe"
	VariantProtected Variant = "protected"
)

type Verdict string

const (
	VerdictValidPass Verdict = "valid-pass"
	VerdictValidFail Verdict = "valid-fail"
)

type AuthorityStatus string

const (
	AuthorityAbsent  AuthorityStatus = "absent"
	AuthorityCurrent AuthorityStatus = "current"
	AuthorityRevoked AuthorityStatus = "revoked"
)

type EffectDisposition string

const (
	EffectCommitted    EffectDisposition = "committed"
	EffectDeduplicated EffectDisposition = "deduplicated"
	EffectRejected     EffectDisposition = "rejected"
	EffectReconciled   EffectDisposition = "reconciled"
	EffectUnresolved   EffectDisposition = "unresolved"
)

type TerminalState string

const (
	TerminalSucceeded  TerminalState = "succeeded"
	TerminalCanceled   TerminalState = "canceled"
	TerminalUnresolved TerminalState = "unresolved"
)

type EventLane string

const (
	LaneTemporal    EventLane = "temporal"
	LaneApplication EventLane = "application"
	LaneDestination EventLane = "destination"
	LaneExecutor    EventLane = "executor"
	LaneEvidence    EventLane = "evidence"
)

type ReplayStatus string

const (
	ReplayPassed ReplayStatus = "passed"
	ReplayFailed ReplayStatus = "failed"
)

type StopDisposition string

const (
	StopExited     StopDisposition = "exited"
	StopNotFound   StopDisposition = "not-found"
	StopUnresolved StopDisposition = "unresolved"
)

type Catalog struct {
	SchemaVersion string     `json:"schema_version"`
	Product       Product    `json:"product"`
	Sections      []Section  `json:"sections"`
	Scenarios     []Scenario `json:"scenarios"`
}

type Product struct {
	Name           string     `json:"name"`
	Positioning    string     `json:"positioning"`
	Audience       string     `json:"audience"`
	PrimaryOutcome string     `json:"primary_outcome"`
	AntiGoals      []AntiGoal `json:"anti_goals"`
}

type Section struct {
	ID      SectionID `json:"id"`
	Title   string    `json:"title"`
	Purpose string    `json:"purpose"`
	Href    string    `json:"href"`
}

type Scenario struct {
	ID              string              `json:"id"`
	Slug            string              `json:"slug"`
	Title           string              `json:"title"`
	Summary         string              `json:"summary"`
	Question        string              `json:"question"`
	Invariant       string              `json:"invariant"`
	FailureBoundary string              `json:"failure_boundary"`
	Falsifier       string              `json:"falsifier"`
	RecipeHref      string              `json:"recipe_href"`
	Claim           Claim               `json:"claim"`
	Responsibility  ResponsibilitySplit `json:"responsibility"`
	Episodes        []Episode           `json:"episodes"`
}

type Claim struct {
	Statement string         `json:"statement"`
	Scope     string         `json:"scope"`
	Maturity  Maturity       `json:"maturity"`
	Evidence  []ArtifactLink `json:"evidence"`
}

type ResponsibilitySplit struct {
	Temporal    string `json:"temporal"`
	Application string `json:"application"`
	Destination string `json:"destination"`
	Executor    string `json:"executor"`
}

type Episode struct {
	ID            string            `json:"id"`
	Variant       Variant           `json:"variant"`
	Verdict       Verdict           `json:"verdict"`
	Outcome       OutcomeView       `json:"outcome"`
	Identities    IdentityView      `json:"identities"`
	Authority     []AuthorityView   `json:"authority"`
	Effects       []EffectView      `json:"effects"`
	Cancellation  *CancellationView `json:"cancellation,omitempty"`
	Events        []EventView       `json:"events"`
	NativeHistory ArtifactLink      `json:"native_history"`
	RawEvidence   []ArtifactLink    `json:"raw_evidence"`
	Provenance    ProvenanceView    `json:"provenance"`
}

type OutcomeView struct {
	TerminalState       TerminalState `json:"terminal_state"`
	Summary             string        `json:"summary"`
	AcceptedResultID    string        `json:"accepted_result_id"`
	PhysicalEffectCount int           `json:"physical_effect_count"`
}

type IdentityView struct {
	SessionID   string           `json:"session_id"`
	TurnID      string           `json:"turn_id"`
	OperationID string           `json:"operation_id"`
	EffectID    string           `json:"effect_id"`
	Delivery    DeliveryIdentity `json:"delivery"`
}

type DeliveryIdentity struct {
	WorkflowID       string `json:"workflow_id"`
	RunID            string `json:"run_id"`
	ActivityID       string `json:"activity_id"`
	ActivityAttempt  int    `json:"activity_attempt"`
	WorkerIdentity   string `json:"worker_identity"`
	ProcessIdentity  string `json:"process_identity"`
	ProviderIdentity string `json:"provider_identity,omitempty"`
}

type AuthorityView struct {
	Sequence         int             `json:"sequence"`
	Generation       int             `json:"generation"`
	Status           AuthorityStatus `json:"status"`
	CapabilityDigest string          `json:"capability_digest"`
	Actor            string          `json:"actor"`
	OccurredAt       string          `json:"occurred_at"`
}

type EffectView struct {
	EffectID    string            `json:"effect_id"`
	Disposition EffectDisposition `json:"disposition"`
	ReceiptID   string            `json:"receipt_id"`
	Summary     string            `json:"summary"`
}

type CancellationView struct {
	RequestedAt        string          `json:"requested_at"`
	AuthorityRevokedAt string          `json:"authority_revoked_at"`
	StopDeliveredAt    string          `json:"stop_delivered_at"`
	AcknowledgedAt     string          `json:"acknowledged_at"`
	Disposition        StopDisposition `json:"disposition"`
}

type EventView struct {
	Sequence   int       `json:"sequence"`
	OccurredAt string    `json:"occurred_at"`
	Lane       EventLane `json:"lane"`
	EventType  string    `json:"event_type"`
	Summary    string    `json:"summary"`
	References []string  `json:"references"`
}

type ArtifactLink struct {
	Path          string `json:"path"`
	ArchiveMember string `json:"archive_member,omitempty"`
	SHA256        string `json:"sha256"`
	MediaType     string `json:"media_type"`
	Label         string `json:"label"`
}

type ProvenanceView struct {
	EvidenceRoot      string         `json:"evidence_root"`
	Manifest          ArtifactLink   `json:"manifest"`
	Report            ArtifactLink   `json:"report"`
	SourceRevision    string         `json:"source_revision"`
	AuditCommand      string         `json:"audit_command"`
	ReplayStatus      ReplayStatus   `json:"replay_status"`
	CorrectionLineage []ArtifactLink `json:"correction_lineage"`
}
