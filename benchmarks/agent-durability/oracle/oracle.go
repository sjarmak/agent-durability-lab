package oracle

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

const oracleID = "adl-independent-oracle-v1"

type evidence struct {
	manifest    protocol.Manifest
	events      []protocol.Event
	authority   protocol.AuthorityState
	destination protocol.DestinationState
	fault       protocol.FaultBoundary
	processes   []protocol.ProcessObservation
	native      []protocol.NativeRecord
	input       protocol.EffectiveInput
}

func EvaluateAndWrite(ctx context.Context, runDir string) (protocol.Verdict, error) {
	verdict := Evaluate(ctx, runDir)
	if err := writeVerdict(ctx, filepath.Join(runDir, protocol.VerdictFile), verdict); err != nil {
		return protocol.Verdict{}, err
	}
	return verdict, nil
}

// Evaluate reconstructs a verdict from sealed raw evidence without writing to
// the run directory. It is intended for independent post-run audits.
func Evaluate(ctx context.Context, runDir string) protocol.Verdict {
	return evaluate(ctx, runDir)
}

func evaluate(ctx context.Context, runDir string) protocol.Verdict {
	result := protocol.Verdict{ContractVersion: protocol.ContractVersion, Class: protocol.VerdictInvalid, Oracle: oracleID}
	if err := ctx.Err(); err != nil {
		result.ReasonCodes = []string{protocol.ReasonEvidenceMalformed}
		return result
	}
	loaded, reasons := loadEvidence(runDir)
	result.RunID = loaded.manifest.RunID
	result.Case = loaded.manifest.Case
	result.Probe = loaded.manifest.Probe
	result.Trial = loaded.manifest.Trial
	if len(reasons) != 0 {
		result.ReasonCodes = uniqueSorted(reasons)
		return result
	}

	reasons = validateEvidence(loaded)
	if len(reasons) != 0 {
		result.ReasonCodes = uniqueSorted(reasons)
		return result
	}
	result.Metrics = calculateMetrics(loaded)
	result.ReasonCodes = caseFailures(loaded, result.Metrics)
	if len(result.ReasonCodes) == 0 {
		result.Class = protocol.VerdictValidPass
	} else {
		result.Class = protocol.VerdictValidFail
	}
	return result
}

func loadEvidence(runDir string) (evidence, []string) {
	var loaded evidence
	if err := readJSON(filepath.Join(runDir, protocol.ManifestFile), &loaded.manifest); err != nil {
		return loaded, []string{reasonForReadError(err)}
	}
	if err := loaded.manifest.Validate(); err != nil {
		return loaded, []string{protocol.ReasonEvidenceMalformed}
	}
	var reasons []string
	for name, expected := range loaded.manifest.EvidenceSHA256 {
		actual, err := protocol.FileSHA256(filepath.Join(runDir, name))
		if err != nil {
			reasons = append(reasons, reasonForReadError(err))
			continue
		}
		if actual != expected {
			reasons = append(reasons, protocol.ReasonEvidenceHashMismatch)
		}
	}
	for _, name := range protocol.RawEvidenceFiles()[1:] {
		if _, found := loaded.manifest.EvidenceSHA256[name]; !found {
			reasons = append(reasons, protocol.ReasonEvidenceMissing)
		}
	}
	if len(reasons) != 0 {
		return loaded, reasons
	}
	readers := []struct {
		name  string
		value any
	}{
		{protocol.AuthorityStateFile, &loaded.authority},
		{protocol.DestinationStateFile, &loaded.destination},
		{protocol.FaultBoundaryFile, &loaded.fault},
		{protocol.ProcessObservationsFile, &loaded.processes},
		{protocol.NativeJournalFile, &loaded.native},
		{protocol.EffectiveInputFile, &loaded.input},
	}
	for _, reader := range readers {
		if err := readJSON(filepath.Join(runDir, reader.name), reader.value); err != nil {
			reasons = append(reasons, reasonForReadError(err))
		}
	}
	if err := readEvents(filepath.Join(runDir, protocol.CommonEventsFile), &loaded.events); err != nil {
		reasons = append(reasons, reasonForReadError(err))
	}
	return loaded, reasons
}

func validateEvidence(loaded evidence) []string {
	var reasons []string
	if loaded.manifest.InputSHA256 != loaded.manifest.EvidenceSHA256[protocol.EffectiveInputFile] {
		reasons = append(reasons, protocol.ReasonEvidenceHashMismatch)
	}
	if err := loaded.input.Validate(); err != nil {
		reasons = append(reasons, protocol.ReasonEvidenceMalformed)
	}
	if err := loaded.authority.Validate(loaded.manifest.SessionID); err != nil {
		reasons = append(reasons, protocol.ReasonEvidenceMalformed)
	}
	if err := loaded.destination.Validate(); err != nil {
		reasons = append(reasons, protocol.ReasonEvidenceMalformed)
	}
	var previousEventTime time.Time
	for index, event := range loaded.events {
		if err := event.Validate(); err != nil || event.Sequence != uint64(index+1) || event.SessionID != loaded.manifest.SessionID {
			reasons = append(reasons, protocol.ReasonEvidenceMalformed)
		}
		eventTime, err := time.Parse(time.RFC3339Nano, event.Time)
		if err == nil && !previousEventTime.IsZero() && !eventTime.After(previousEventTime) {
			reasons = append(reasons, protocol.ReasonEvidenceMalformed)
		}
		previousEventTime = eventTime
	}
	for _, observation := range loaded.processes {
		if err := observation.Validate(); err != nil || observation.Sequence > uint64(len(loaded.events)) {
			reasons = append(reasons, protocol.ReasonEvidenceMalformed)
			continue
		}
		event := loaded.events[observation.Sequence-1]
		if event.ActorID != observation.ActorID || event.Generation != observation.Generation || event.ProcessIdentity != observation.ProcessIdentity {
			reasons = append(reasons, protocol.ReasonEvidenceInconsistent)
		}
	}
	for index, record := range loaded.native {
		if err := record.Validate(); err != nil || record.Sequence != uint64(index+1) {
			reasons = append(reasons, protocol.ReasonEvidenceMalformed)
		}
	}
	if loaded.manifest.Probe == protocol.ProbeUnfaulted {
		if loaded.fault.Triggered || loaded.fault.AfterSequence != 0 || loaded.fault.BeforeSequence != 0 {
			reasons = append(reasons, protocol.ReasonFaultNotBracketed)
		}
	} else if !faultIsBracketed(loaded) || !faultTargetsCase(loaded) {
		reasons = append(reasons, protocol.ReasonFaultNotBracketed)
	}
	if loaded.fault.Triggered && !observedIdentity(loaded.processes, loaded.fault.ActorID, loaded.fault.ProcessIdentity) {
		reasons = append(reasons, protocol.ReasonWrongProcessIdentity)
	}
	if len(loaded.native) == 0 || len(loaded.processes) == 0 {
		reasons = append(reasons, protocol.ReasonEvidenceMissing)
	}
	if loaded.manifest.Case == protocol.CaseCancellationUnreachable && loaded.manifest.Probe != protocol.ProbeUnfaulted && !loaded.authority.CancellationCommitted {
		reasons = append(reasons, protocol.ReasonCasePreconditionMissing)
	}
	if !evidenceConsistent(loaded) {
		reasons = append(reasons, protocol.ReasonEvidenceInconsistent)
	}
	return uniqueSorted(reasons)
}

func evidenceConsistent(loaded evidence) bool {
	events := make(map[uint64]protocol.Event, len(loaded.events))
	acceptedOutcomes := 0
	acceptedActions := make(map[uint64]protocol.Event)
	effectEvents := make(map[uint64]protocol.Event)
	cancellationEvents := 0
	replacementAfterCancellation := false
	activeGeneration := uint64(0)
	registeredOwners := make(map[string]bool)
	for _, event := range loaded.events {
		events[event.Sequence] = event
		if event.Kind == protocol.EventOutcomeAccepted && event.Decision == "accepted" {
			acceptedOutcomes++
		}
		if acceptedActionEvent(event.Kind) && event.Decision == "accepted" {
			acceptedActions[event.Sequence] = event
		}
		if event.Kind == protocol.EventEffectAccepted || event.Kind == protocol.EventEffectRejected {
			effectEvents[event.Sequence] = event
		}
		if event.Kind == protocol.EventCancellationCommitted && event.Decision == "accepted" {
			cancellationEvents++
		}
		if loaded.authority.CancellationCommitted && event.Sequence > loaded.authority.CancellationSequence && event.Decision == "accepted" &&
			(event.Kind == protocol.EventExecutorRegistered || event.Kind == protocol.EventOwnerReplaced) {
			replacementAfterCancellation = true
		}
		if authoritativeGenerationEvent(event) && event.Generation > activeGeneration {
			activeGeneration = event.Generation
		}
		if event.Kind == protocol.EventExecutorRegistered && (event.Decision == "observed" || event.Decision == "accepted") {
			registeredOwners[event.ActorID+"\x00"+event.ProcessIdentity] = true
		}
	}
	if activeGeneration != loaded.authority.ActiveGeneration {
		return false
	}
	if loaded.manifest.Case == protocol.CaseSurvivingExecutor && len(registeredOwners) != loaded.authority.ConcurrentOwnerCount {
		return false
	}
	if acceptedOutcomes != len(loaded.authority.AcceptedOutcomes) {
		return false
	}
	for _, outcome := range loaded.authority.AcceptedOutcomes {
		event, found := events[outcome.Sequence]
		if !found || event.Kind != protocol.EventOutcomeAccepted || event.Decision != "accepted" || event.Generation != outcome.Generation || outcome.Kind != "outcome" {
			return false
		}
	}
	for _, action := range loaded.authority.AcceptedActions {
		event, found := events[action.Sequence]
		if !found || event.Generation != action.Generation || event.Decision != "accepted" || !acceptedActionEvent(event.Kind) || action.Kind != acceptedActionKind(event.Kind) {
			return false
		}
		delete(acceptedActions, action.Sequence)
	}
	if len(acceptedActions) != 0 {
		return false
	}
	for _, attempt := range loaded.destination.Attempts {
		event, found := events[attempt.Sequence]
		if !found || event.Generation != attempt.Generation || event.LogicalEffectID != attempt.LogicalEffectID || event.PhysicalAttemptID != attempt.PhysicalAttemptID {
			return false
		}
		if attempt.Applied != (event.Kind == protocol.EventEffectAccepted && event.Decision == "accepted") {
			return false
		}
		delete(effectEvents, attempt.Sequence)
	}
	if len(effectEvents) != 0 {
		return false
	}
	if loaded.authority.CancellationCommitted {
		event, found := events[loaded.authority.CancellationSequence]
		if !found || event.Kind != protocol.EventCancellationCommitted || event.Decision != "accepted" || cancellationEvents != 1 {
			return false
		}
	} else if cancellationEvents != 0 {
		return false
	}
	if replacementAfterCancellation != loaded.authority.ReplacementAfterCancellation {
		return false
	}
	return true
}

func authoritativeGenerationEvent(event protocol.Event) bool {
	switch event.Kind {
	case protocol.EventExecutorRegistered:
		return event.Decision == "observed" || event.Decision == "accepted"
	case protocol.EventOwnerReplaced, protocol.EventOutcomeAccepted, protocol.EventEffectAccepted:
		return event.Decision == "accepted"
	default:
		return false
	}
}

func acceptedActionEvent(kind string) bool {
	return kind == protocol.EventEffectAccepted || kind == protocol.EventStaleCompletion || kind == protocol.EventStaleStop
}

func acceptedActionKind(eventKind string) string {
	switch eventKind {
	case protocol.EventEffectAccepted:
		return "effect"
	case protocol.EventStaleCompletion:
		return "stale-completion"
	case protocol.EventStaleStop:
		return "stale-stop"
	default:
		return ""
	}
}

func faultIsBracketed(loaded evidence) bool {
	fault := loaded.fault
	if !fault.Triggered || fault.Point == "" || fault.TriggeredAt == "" || fault.AfterSequence == 0 || fault.BeforeSequence == 0 || fault.AfterSequence >= fault.BeforeSequence {
		return false
	}
	if fault.AfterSequence > uint64(len(loaded.events)) || fault.BeforeSequence > uint64(len(loaded.events)) {
		return false
	}
	after := loaded.events[fault.AfterSequence-1]
	before := loaded.events[fault.BeforeSequence-1]
	expectedPoint, afterKind, beforeKinds := expectedBoundary(loaded)
	if fault.Point != expectedPoint || after.Kind != afterKind || !slices.Contains(beforeKinds, before.Kind) {
		return false
	}
	afterTime, afterErr := time.Parse(time.RFC3339Nano, after.Time)
	beforeTime, beforeErr := time.Parse(time.RFC3339Nano, before.Time)
	faultTime, faultErr := time.Parse(time.RFC3339Nano, fault.TriggeredAt)
	return afterErr == nil && beforeErr == nil && faultErr == nil && faultTime.After(afterTime) && faultTime.Before(beforeTime)
}

func expectedBoundary(loaded evidence) (string, string, []string) {
	switch loaded.manifest.Case {
	case protocol.CaseSurvivingExecutor:
		return "worker-died-after-agent-registration", protocol.EventBarrierReached, []string{protocol.EventExecutorAttached, protocol.EventExecutorRegistered}
	case protocol.CaseAmbiguousEffect:
		switch boundary := loaded.input.Settings["fault_boundary"]; boundary {
		case "claim-committed-before-process-exec":
			return boundary, protocol.EventBarrierReached, []string{protocol.EventExecutorAttached}
		case protocol.FaultPointProcessCreatedBeforeVendorRegistration:
			return boundary, protocol.EventBarrierReached, []string{
				protocol.EventExecutorAttached, protocol.EventEffectAccepted,
			}
		case protocol.FaultPointToolEffectBeforeActivityCompletion,
			protocol.FaultPointFinalOutputBeforeActivityCompletion:
			return boundary, protocol.EventBarrierReached, []string{
				protocol.EventExecutorAttached, protocol.EventExecutorRegistered,
			}
		case "", "unfaulted":
			return "effect-confirmed-before-step-completion", protocol.EventEffectAccepted,
				[]string{protocol.EventEffectAccepted, protocol.EventEffectRejected}
		default:
			return "", "", nil
		}
	case protocol.CaseStaleGeneration:
		return "replacement-committed-before-stale-actions", protocol.EventOwnerReplaced, []string{protocol.EventEffectAccepted, protocol.EventEffectRejected}
	case protocol.CaseCancellationUnreachable:
		return "process-frozen-before-cancellation", protocol.EventExecutorRegistered, []string{protocol.EventCancellationCommitted}
	default:
		return "", "", nil
	}
}

func faultTargetsCase(loaded evidence) bool {
	fault := loaded.fault
	if loaded.manifest.Case == protocol.CaseStaleGeneration {
		for _, observation := range loaded.processes {
			if observation.ActorID == fault.ActorID && observation.ProcessIdentity == fault.ProcessIdentity {
				return observation.Generation < loaded.authority.ActiveGeneration && observation.State == "running"
			}
		}
		return false
	}
	after := loaded.events[fault.AfterSequence-1]
	if fault.ActorID != after.ActorID || fault.ProcessIdentity != after.ProcessIdentity {
		return false
	}
	wantState := "running"
	if loaded.manifest.Case == protocol.CaseCancellationUnreachable {
		wantState = "frozen"
	}
	for _, observation := range loaded.processes {
		if observation.ActorID == fault.ActorID && observation.ProcessIdentity == fault.ProcessIdentity && observation.State == wantState {
			return true
		}
	}
	return false
}

func observedIdentity(observations []protocol.ProcessObservation, actor, process string) bool {
	for _, observation := range observations {
		if observation.ActorID == actor && observation.ProcessIdentity == process && observation.Generation > 0 && observation.Sequence > 0 {
			return true
		}
	}
	return false
}

func calculateMetrics(loaded evidence) protocol.Metrics {
	metrics := protocol.Metrics{
		AcceptedOutcomeCount: len(loaded.authority.AcceptedOutcomes),
		PhysicalAttemptCount: len(loaded.destination.Attempts),
		ConcurrentOwnerCount: loaded.authority.ConcurrentOwnerCount,
	}
	for _, attempt := range loaded.destination.Attempts {
		if attempt.Applied {
			metrics.PhysicalEffectCount++
		}
	}
	for _, action := range loaded.authority.AcceptedActions {
		if action.Generation < loaded.authority.ActiveGeneration {
			metrics.StaleActionAcceptCount++
		}
		if loaded.authority.CancellationCommitted && action.Sequence > loaded.authority.CancellationSequence {
			metrics.PostCancelAcceptCount++
		}
	}
	for _, outcome := range loaded.authority.AcceptedOutcomes {
		if loaded.authority.CancellationCommitted && outcome.Sequence > loaded.authority.CancellationSequence {
			metrics.PostCancelAcceptCount++
		}
	}
	return metrics
}

func caseFailures(loaded evidence, metrics protocol.Metrics) []string {
	var reasons []string
	switch loaded.manifest.Case {
	case protocol.CaseSurvivingExecutor:
		if metrics.ConcurrentOwnerCount > 1 {
			reasons = append(reasons, protocol.ReasonCompetingOwner)
		}
		if metrics.AcceptedOutcomeCount != 1 {
			reasons = append(reasons, protocol.ReasonMultipleOutcomes)
		}
	case protocol.CaseAmbiguousEffect:
		if metrics.PhysicalEffectCount > 1 {
			reasons = append(reasons, protocol.ReasonDuplicateEffect)
		}
		if metrics.AcceptedOutcomeCount != 1 {
			reasons = append(reasons, protocol.ReasonMultipleOutcomes)
		}
	case protocol.CaseStaleGeneration:
		if metrics.StaleActionAcceptCount > 0 {
			reasons = append(reasons, protocol.ReasonStaleActionAccepted)
		}
		if !loaded.authority.CurrentOwnerAlive {
			reasons = append(reasons, protocol.ReasonCurrentOwnerStopped)
		}
		if metrics.AcceptedOutcomeCount != 1 {
			reasons = append(reasons, protocol.ReasonMultipleOutcomes)
		}
	case protocol.CaseCancellationUnreachable:
		if metrics.PostCancelAcceptCount > 0 {
			reasons = append(reasons, protocol.ReasonPostCancelMutation)
		}
		if loaded.authority.ReplacementAfterCancellation {
			reasons = append(reasons, protocol.ReasonPostCancelReplacement)
		}
	}
	return uniqueSorted(reasons)
}

func readJSON(path string, destination any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := rejectDuplicateObjectKeys(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON content")
	}
	return nil
}

func readEvents(path string, destination *[]protocol.Event) (returnErr error) {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() {
		returnErr = errors.Join(returnErr, file.Close())
	}()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var event protocol.Event
		if err := rejectDuplicateObjectKeys(scanner.Bytes()); err != nil {
			return err
		}
		decoder := json.NewDecoder(bytes.NewReader(scanner.Bytes()))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&event); err != nil {
			return err
		}
		*destination = append(*destination, event)
	}
	return scanner.Err()
}

func rejectDuplicateObjectKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := inspectJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON content")
		}
		return err
	}
	return nil
}

func inspectJSONValue(decoder *json.Decoder) error {
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
			token, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := token.(string)
			if !ok {
				return errors.New("JSON object key is not a string")
			}
			if _, exists := keys[key]; exists {
				return fmt.Errorf("duplicate JSON object key %q", key)
			}
			keys[key] = struct{}{}
			if err := inspectJSONValue(decoder); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := inspectJSONValue(decoder); err != nil {
				return err
			}
		}
	default:
		return errors.New("unsupported JSON delimiter")
	}
	closing, err := decoder.Token()
	if err != nil {
		return err
	}
	if closing != json.Delim(map[json.Delim]json.Delim{'{': '}', '[': ']'}[delimiter]) {
		return errors.New("mismatched JSON delimiter")
	}
	return nil
}

func writeVerdict(ctx context.Context, path string, verdict protocol.Verdict) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(verdict, "", "  ")
	if err != nil {
		return fmt.Errorf("encode verdict: %w", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("%w: %s", protocol.ErrEvidenceExists, path)
		}
		return fmt.Errorf("create verdict: %w", err)
	}
	if _, err := file.Write(append(data, '\n')); err != nil {
		_ = file.Close()
		return fmt.Errorf("write verdict: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync verdict: %w", err)
	}
	return file.Close()
}

func reasonForReadError(err error) string {
	if errors.Is(err, os.ErrNotExist) {
		return protocol.ReasonEvidenceMissing
	}
	return protocol.ReasonEvidenceMalformed
}

func uniqueSorted(values []string) []string {
	seen := make(map[string]bool, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	sort.Strings(result)
	return result
}
