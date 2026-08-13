package presentation

import (
	"errors"
	"fmt"
	"path"
	"regexp"
	"strings"
	"time"
)

var (
	digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	slugPattern   = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*$`)
)

func Validate(catalog Catalog) error {
	if catalog.SchemaVersion != SchemaVersion {
		return fmt.Errorf("schema version must be %q", SchemaVersion)
	}
	if err := validateProduct(catalog.Product); err != nil {
		return err
	}
	if err := validateSections(catalog.Sections); err != nil {
		return err
	}
	if len(catalog.Scenarios) == 0 {
		return errors.New("catalog must contain scenarios")
	}
	seen := make(map[string]struct{}, len(catalog.Scenarios))
	for index, scenario := range catalog.Scenarios {
		if _, exists := seen[scenario.ID]; exists {
			return fmt.Errorf("scenario %d duplicates ID %q", index, scenario.ID)
		}
		seen[scenario.ID] = struct{}{}
		if err := validateScenario(scenario); err != nil {
			return fmt.Errorf("scenario %q: %w", scenario.ID, err)
		}
	}
	return nil
}

func validateProduct(product Product) error {
	if blank(product.Name, product.Positioning, product.Audience, product.PrimaryOutcome) {
		return errors.New("product fields must be present")
	}
	required := [...]AntiGoal{
		AntiGoalGenericRuntimeParity, AntiGoalExactlyOnce,
		AntiGoalControlledComputePerformance, AntiGoalUnadmittedProviderCompatibility,
	}
	if len(product.AntiGoals) != len(required) {
		return errors.New("product anti-goals must contain the exact required set")
	}
	seen := make(map[AntiGoal]struct{}, len(product.AntiGoals))
	for _, value := range product.AntiGoals {
		seen[value] = struct{}{}
	}
	for _, value := range required {
		if _, exists := seen[value]; !exists {
			return fmt.Errorf("product anti-goals omit %q", value)
		}
	}
	return nil
}

func validateSections(sections []Section) error {
	required := [...]SectionID{
		SectionStart, SectionPatterns, SectionScenarios,
		SectionEvidence, SectionProtocol, SectionResearch,
	}
	if len(sections) != len(required) {
		return fmt.Errorf("sections = %d, want %d", len(sections), len(required))
	}
	for index, expected := range required {
		section := sections[index]
		if section.ID != expected {
			return fmt.Errorf("section %d = %q, want %q", index, section.ID, expected)
		}
		if blank(section.Title, section.Purpose) || !confinedPath(section.Href) {
			return fmt.Errorf("section %d metadata is invalid", index)
		}
	}
	return nil
}

func validateScenario(scenario Scenario) error {
	if !slugPattern.MatchString(scenario.ID) || scenario.Slug != scenario.ID {
		return errors.New("ID and slug must be the same confined kebab-case value")
	}
	if blank(scenario.Title, scenario.Summary, scenario.Question, scenario.Invariant,
		scenario.FailureBoundary, scenario.Falsifier) || !confinedPath(scenario.RecipeHref) {
		return errors.New("narrative fields or recipe link are invalid")
	}
	if err := validateClaim(scenario.Claim); err != nil {
		return err
	}
	if blank(scenario.Responsibility.Temporal, scenario.Responsibility.Application,
		scenario.Responsibility.Destination, scenario.Responsibility.Executor) {
		return errors.New("responsibility split must name every layer")
	}
	return validateEpisodes(scenario.Episodes)
}

func validateClaim(claim Claim) error {
	if blank(claim.Statement, claim.Scope) ||
		(claim.Maturity != MaturityNormative && claim.Maturity != MaturityExperimental) {
		return errors.New("claim statement, scope, or maturity is invalid")
	}
	if len(claim.Evidence) == 0 {
		return errors.New("claim must link evidence")
	}
	for index, evidence := range claim.Evidence {
		if err := validateArtifact(evidence); err != nil {
			return fmt.Errorf("claim evidence %d: %w", index, err)
		}
	}
	return nil
}

func validateEpisodes(episodes []Episode) error {
	required := [...]struct {
		variant Variant
		verdict Verdict
	}{{VariantUnfaulted, VerdictValidPass}, {VariantUnsafe, VerdictValidFail}, {VariantProtected, VerdictValidPass}}
	if len(episodes) != len(required) {
		return errors.New("episodes must contain unfaulted, unsafe, and protected variants")
	}
	for index, expected := range required {
		episode := episodes[index]
		if episode.Variant != expected.variant || episode.Verdict != expected.verdict {
			return fmt.Errorf("%s episode must be %s", expected.variant, expected.verdict)
		}
		if err := validateEpisode(episode); err != nil {
			return fmt.Errorf("%s episode: %w", episode.Variant, err)
		}
	}
	return nil
}

func validateEpisode(episode Episode) error {
	if episode.ID == "" {
		return errors.New("episode ID is required")
	}
	if err := validateOutcome(episode); err != nil {
		return err
	}
	if err := validateIdentities(episode.Identities); err != nil {
		return err
	}
	if err := validateAuthority(episode.Authority); err != nil {
		return err
	}
	if err := validateEffects(episode.Effects, episode.Identities.EffectID); err != nil {
		return err
	}
	if err := validateCancellation(episode.Cancellation, episode.Outcome.TerminalState); err != nil {
		return err
	}
	if err := validateEvents(episode.Events); err != nil {
		return err
	}
	if err := validateArtifact(episode.NativeHistory); err != nil {
		return fmt.Errorf("native history: %w", err)
	}
	if len(episode.RawEvidence) == 0 {
		return errors.New("raw evidence is required")
	}
	for index, artifact := range episode.RawEvidence {
		if err := validateArtifact(artifact); err != nil {
			return fmt.Errorf("raw evidence %d: %w", index, err)
		}
	}
	return validateProvenance(episode.Provenance)
}

func validateOutcome(episode Episode) error {
	state := episode.Outcome.TerminalState
	if state != TerminalSucceeded && state != TerminalCanceled && state != TerminalUnresolved {
		return errors.New("terminal state is invalid")
	}
	if episode.Outcome.Summary == "" || episode.Outcome.PhysicalEffectCount < 0 {
		return errors.New("outcome is invalid")
	}
	if state == TerminalSucceeded && episode.Outcome.AcceptedResultID == "" {
		return errors.New("succeeded outcome lacks accepted result")
	}
	return nil
}

func validateIdentities(value IdentityView) error {
	if blank(value.SessionID, value.TurnID, value.OperationID, value.EffectID,
		value.Delivery.WorkflowID, value.Delivery.RunID, value.Delivery.ActivityID,
		value.Delivery.WorkerIdentity, value.Delivery.ProcessIdentity) || value.Delivery.ActivityAttempt < 1 {
		return errors.New("logical or delivery identities are incomplete")
	}
	return nil
}

func validateAuthority(values []AuthorityView) error {
	if len(values) == 0 {
		return errors.New("authority history is required")
	}
	lastSequence := 0
	for index, value := range values {
		if value.Sequence <= lastSequence {
			return fmt.Errorf("authority %d sequence is invalid", index)
		}
		lastSequence = value.Sequence
		switch value.Status {
		case AuthorityAbsent:
			if value.Generation != 0 || value.CapabilityDigest != "" {
				return fmt.Errorf("authority %d absent state is invalid", index)
			}
		case AuthorityCurrent, AuthorityRevoked:
			if value.Generation < 1 || !digestPattern.MatchString(value.CapabilityDigest) {
				return fmt.Errorf("authority %d generation or capability digest is invalid", index)
			}
		default:
			return fmt.Errorf("authority %d status is invalid", index)
		}
		if value.Actor == "" {
			return fmt.Errorf("authority %d actor is invalid", index)
		}
		if _, err := parseUTC(value.OccurredAt); err != nil {
			return fmt.Errorf("authority %d: %w", index, err)
		}
	}
	return nil
}

func validateEffects(values []EffectView, effectID string) error {
	if len(values) == 0 {
		return errors.New("effect observations are required")
	}
	for index, value := range values {
		if value.EffectID != effectID || blank(value.ReceiptID, value.Summary) {
			return fmt.Errorf("effect %d identity or receipt is invalid", index)
		}
		switch value.Disposition {
		case EffectCommitted, EffectDeduplicated, EffectRejected, EffectReconciled, EffectUnresolved:
		default:
			return fmt.Errorf("effect %d disposition is invalid", index)
		}
	}
	return nil
}

func validateCancellation(value *CancellationView, terminal TerminalState) error {
	if value == nil {
		if terminal == TerminalCanceled {
			return errors.New("canceled outcome lacks cancellation evidence")
		}
		return nil
	}
	if terminal != TerminalCanceled {
		return errors.New("cancellation evidence requires canceled terminal state")
	}
	requested, err := parseUTC(value.RequestedAt)
	if err != nil {
		return fmt.Errorf("cancellation request: %w", err)
	}
	revoked, err := parseUTC(value.AuthorityRevokedAt)
	if err != nil {
		return fmt.Errorf("cancellation revocation: %w", err)
	}
	delivered, err := parseUTC(value.StopDeliveredAt)
	if err != nil {
		return fmt.Errorf("stop delivery: %w", err)
	}
	acknowledged, err := parseUTC(value.AcknowledgedAt)
	if err != nil {
		return fmt.Errorf("stop acknowledgement: %w", err)
	}
	if revoked.Before(requested) || delivered.Before(revoked) {
		return errors.New("cancellation requires request then revocation before stop delivery")
	}
	if acknowledged.Before(delivered) {
		return errors.New("stop acknowledgement precedes delivery")
	}
	if value.Disposition != StopExited && value.Disposition != StopNotFound && value.Disposition != StopUnresolved {
		return errors.New("stop disposition is invalid")
	}
	return nil
}

func validateEvents(values []EventView) error {
	if len(values) == 0 {
		return errors.New("normalized events are required")
	}
	lastSequence := 0
	for index, value := range values {
		if value.Sequence <= lastSequence {
			return fmt.Errorf("event %d sequence is not increasing", index)
		}
		lastSequence = value.Sequence
		if _, err := parseUTC(value.OccurredAt); err != nil {
			return fmt.Errorf("event %d: %w", index, err)
		}
		if !validLane(value.Lane) || blank(value.EventType, value.Summary) {
			return fmt.Errorf("event %d metadata is invalid", index)
		}
	}
	return nil
}

func validateProvenance(value ProvenanceView) error {
	if !confinedPath(value.EvidenceRoot) || blank(value.SourceRevision, value.AuditCommand) {
		return errors.New("provenance root, source revision, or audit command is invalid")
	}
	if err := validateArtifact(value.Manifest); err != nil {
		return fmt.Errorf("provenance manifest: %w", err)
	}
	if err := validateArtifact(value.Report); err != nil {
		return fmt.Errorf("provenance report: %w", err)
	}
	if value.ReplayStatus != ReplayPassed {
		return errors.New("provenance replay must have passed")
	}
	for index, artifact := range value.CorrectionLineage {
		if err := validateArtifact(artifact); err != nil {
			return fmt.Errorf("correction lineage %d: %w", index, err)
		}
	}
	return nil
}

func validateArtifact(value ArtifactLink) error {
	if !confinedPath(value.Path) {
		return errors.New("artifact path is not confined")
	}
	if value.ArchiveMember != "" && !confinedPath(value.ArchiveMember) {
		return errors.New("artifact archive member is not confined")
	}
	if !digestPattern.MatchString(value.SHA256) {
		return errors.New("artifact SHA-256 is invalid")
	}
	if blank(value.MediaType, value.Label) {
		return errors.New("artifact media type or label is missing")
	}
	return nil
}

func confinedPath(value string) bool {
	if value == "" || strings.ContainsAny(value, "\\\x00:?#%") || strings.HasPrefix(value, "/") {
		return false
	}
	cleaned := path.Clean(value)
	return cleaned != "." && cleaned != ".." && !strings.HasPrefix(cleaned, "../")
}

func parseUTC(value string) (time.Time, error) {
	if !strings.HasSuffix(value, "Z") {
		return time.Time{}, errors.New("timestamp must use UTC Z notation")
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, errors.New("timestamp must be a real RFC3339 UTC instant")
	}
	return parsed, nil
}

func validLane(value EventLane) bool {
	switch value {
	case LaneTemporal, LaneApplication, LaneDestination, LaneExecutor, LaneEvidence:
		return true
	default:
		return false
	}
}

func blank(values ...string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return true
		}
	}
	return false
}
