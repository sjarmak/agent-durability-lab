package lab

import (
	"fmt"
	"runtime"
	"sort"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/evidence"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

const evidenceAdapterID = "temporal-claude-direct-cli"

type ClaudeAttemptCapture struct {
	TemporalAttempt   int32
	ActorID           string
	ProcessIdentity   string
	VendorSessionID   string
	PhysicalAttemptID string
	StartedAt         time.Time
	AppliedAt         time.Time
}

type NativeCapture struct {
	Kind   string
	Detail string
}

type BoundaryCapture struct {
	Point           FaultBoundary
	ActorID         string
	ProcessIdentity string
	ReachedAt       time.Time
}

type AttachmentCapture struct {
	TemporalAttempt int32
	ActorID         string
	ProcessIdentity string
	Generation      uint64
	AttachedAt      time.Time
}

type EvidenceCapture struct {
	AdapterVersion          string
	ClaudeBinarySHA256      string
	ClaudeVersion           string
	Model                   string
	Runtime                 string
	Probe                   protocol.Probe
	FaultBoundary           FaultBoundary
	Trial                   int
	LogicalSessionID        string
	LogicalTurnID           string
	LogicalEffectID         string
	RecoveryMode            RecoveryMode
	SelectedVendorSessionID string
	DestinationID           string
	StartedAt               time.Time
	Attempts                []ClaudeAttemptCapture
	Attachments             []AttachmentCapture
	Boundary                BoundaryCapture
	FaultAt                 time.Time
	CompletedAt             time.Time
	Settings                map[string]string
	Native                  []NativeCapture
}

func BuildEvidenceBundle(capture EvidenceCapture) (evidence.Bundle, error) {
	if err := validateEvidenceCapture(capture); err != nil {
		return evidence.Bundle{}, err
	}
	events, processes, destinationAttempts, actions, boundarySequence, err := buildAttemptEvidence(capture)
	if err != nil {
		return evidence.Bundle{}, err
	}
	outcomeSequence := uint64(len(events) + 1)
	lastAttempt := capture.Attempts[len(capture.Attempts)-1]
	events = append(events, protocol.Event{
		Sequence: outcomeSequence, Time: capture.CompletedAt.Format(time.RFC3339Nano),
		Kind: protocol.EventOutcomeAccepted, SessionID: capture.LogicalSessionID,
		ActorID: lastAttempt.ActorID, Generation: 1, ProcessIdentity: lastAttempt.ProcessIdentity,
		Decision: "accepted",
	})
	return evidence.Bundle{
		Identity: evidence.RunIdentity{
			RunID: captureRunID(capture),
			Case:  protocol.CaseAmbiguousEffect, Probe: capture.Probe, Trial: capture.Trial,
			SessionID: capture.LogicalSessionID,
		},
		Events: events,
		Authority: protocol.AuthorityState{
			SessionID: capture.LogicalSessionID, ActiveGeneration: 1,
			ConcurrentOwnerCount: concurrentOwnerCount(capture), CurrentOwnerAlive: false,
			AcceptedOutcomes: []protocol.AcceptedAction{{Kind: "outcome", Generation: 1, Sequence: outcomeSequence}},
			AcceptedActions:  actions,
		},
		Destination: protocol.DestinationState{
			DestinationID: capture.DestinationID, Attempts: destinationAttempts,
		},
		Fault:     captureFault(capture, boundarySequence, events),
		Processes: processes, Native: nativeRecordsForCapture(capture.Native),
		Input: effectiveInput(capture),
	}, nil
}

func captureFault(capture EvidenceCapture, boundarySequence uint64, events []protocol.Event) protocol.FaultBoundary {
	if capture.Probe == protocol.ProbeUnfaulted {
		return protocol.FaultBoundary{}
	}
	beforeSequence := uint64(0)
	for _, event := range events {
		eventTime, _ := time.Parse(time.RFC3339Nano, event.Time)
		if eventTime.After(capture.FaultAt) {
			beforeSequence = event.Sequence
			break
		}
	}
	return protocol.FaultBoundary{
		Point: string(capture.FaultBoundary), Triggered: true,
		AfterSequence: boundarySequence, BeforeSequence: beforeSequence, ActorID: capture.Boundary.ActorID,
		ProcessIdentity: capture.Boundary.ProcessIdentity,
		TriggeredAt:     capture.FaultAt.Format(time.RFC3339Nano),
	}
}

func effectiveInput(capture EvidenceCapture) protocol.EffectiveInput {
	settings := cloneSettings(capture.Settings)
	settings["claude_version"] = capture.ClaudeVersion
	settings["model"] = capture.Model
	settings["logical_turn_id"] = capture.LogicalTurnID
	settings["probe"] = string(capture.Probe)
	settings["fault_boundary"] = string(capture.FaultBoundary)
	settings["recovery_mode"] = string(capture.RecoveryMode.normalized())
	if capture.SelectedVendorSessionID != "" {
		settings["selected_vendor_session_id"] = capture.SelectedVendorSessionID
	}
	return protocol.EffectiveInput{
		AdapterID: evidenceAdapterID, AdapterVersion: capture.AdapterVersion,
		AgentProtocol: protocol.AgentProtocol, AgentBinarySHA256: capture.ClaudeBinarySHA256,
		AuthorityProtocol: protocol.AuthorityProtocol, DestinationProtocol: protocol.DestinationProtocol,
		DestinationID: capture.DestinationID, FailureProtocol: protocol.FailureProtocol,
		OracleProtocol: protocol.OracleProtocol,
		OracleVisibility: []string{
			protocol.AuthorityStateFile, protocol.DestinationStateFile,
			protocol.FaultBoundaryFile, protocol.ProcessObservationsFile,
		},
		Runtime: capture.Runtime + "; adapter=" + runtime.GOOS + "/" + runtime.GOARCH, Settings: settings,
	}
}

type eventCandidate struct {
	event        protocol.Event
	time         time.Time
	processState string
	destination  bool
}

func buildAttemptEvidence(capture EvidenceCapture) (
	events []protocol.Event, processes []protocol.ProcessObservation,
	destination []protocol.DestinationAttempt, actions []protocol.AcceptedAction,
	boundarySequence uint64, err error,
) {
	candidates := make([]eventCandidate, 0, len(capture.Attempts)*2+len(capture.Attachments)+1)
	for _, attempt := range capture.Attempts {
		candidates = append(candidates, eventCandidate{event: protocol.Event{
			Time: attempt.StartedAt.Format(time.RFC3339Nano),
			Kind: protocol.EventExecutorRegistered, SessionID: capture.LogicalSessionID,
			ActorID: attempt.ActorID, Generation: 1, ProcessIdentity: attempt.ProcessIdentity,
			Decision: "observed",
		}, time: attempt.StartedAt, processState: "running"})
		candidates = append(candidates, eventCandidate{event: protocol.Event{
			Time: attempt.AppliedAt.Format(time.RFC3339Nano),
			Kind: protocol.EventEffectAccepted, SessionID: capture.LogicalSessionID,
			ActorID: attempt.ActorID, Generation: 1, ProcessIdentity: attempt.ProcessIdentity,
			LogicalEffectID: capture.LogicalEffectID, PhysicalAttemptID: attempt.PhysicalAttemptID,
			Decision: "accepted",
		}, time: attempt.AppliedAt, destination: true})
	}
	for _, attachment := range capture.Attachments {
		candidates = append(candidates, eventCandidate{event: protocol.Event{
			Time: attachment.AttachedAt.Format(time.RFC3339Nano), Kind: protocol.EventExecutorAttached,
			SessionID: capture.LogicalSessionID, ActorID: attachment.ActorID,
			Generation: attachment.Generation, ProcessIdentity: attachment.ProcessIdentity,
			Decision: "observed",
		}, time: attachment.AttachedAt, processState: "running"})
	}
	if capture.Probe != protocol.ProbeUnfaulted {
		candidates = append(candidates, eventCandidate{event: protocol.Event{
			Time: capture.Boundary.ReachedAt.Format(time.RFC3339Nano), Kind: protocol.EventBarrierReached,
			SessionID: capture.LogicalSessionID, ActorID: capture.Boundary.ActorID, Generation: 1,
			ProcessIdentity: capture.Boundary.ProcessIdentity, Decision: "blocked",
		}, time: capture.Boundary.ReachedAt, processState: "running"})
	}
	sort.Slice(candidates, func(left, right int) bool { return candidates[left].time.Before(candidates[right].time) })
	return materializeCandidates(candidates, capture.LogicalEffectID)
}

func materializeCandidates(candidates []eventCandidate, logicalEffectID string) (
	events []protocol.Event, processes []protocol.ProcessObservation,
	destination []protocol.DestinationAttempt, actions []protocol.AcceptedAction,
	boundarySequence uint64, err error,
) {
	for index, candidate := range candidates {
		sequence := uint64(index + 1)
		candidate.event.Sequence = sequence
		events = append(events, candidate.event)
		if candidate.processState != "" {
			processes = append(processes, protocol.ProcessObservation{
				Sequence: sequence, ActorID: candidate.event.ActorID, Generation: 1,
				ProcessIdentity: candidate.event.ProcessIdentity, State: candidate.processState,
			})
		}
		if candidate.event.Kind == protocol.EventBarrierReached {
			boundarySequence = sequence
		}
		if candidate.destination {
			destination = append(destination, protocol.DestinationAttempt{
				LogicalEffectID: logicalEffectID, PhysicalAttemptID: candidate.event.PhysicalAttemptID,
				Generation: 1, Sequence: sequence, Applied: true,
			})
			actions = append(actions, protocol.AcceptedAction{Kind: "effect", Generation: 1, Sequence: sequence})
		}
	}
	if len(events) == 0 {
		return nil, nil, nil, nil, 0, fmt.Errorf("%w: no Claude evidence events", protocol.ErrInvalidEvidence)
	}
	return events, processes, destination, actions, boundarySequence, nil
}

func concurrentOwnerCount(capture EvidenceCapture) int {
	if capture.Probe == protocol.ProbeProtected {
		return 1
	}
	if capture.FaultBoundary == FaultAfterFinalOutput {
		return 1
	}
	return len(capture.Attempts)
}

func validateEvidenceCapture(capture EvidenceCapture) error {
	if err := validateCaptureShape(capture); err != nil {
		return err
	}
	seenPhysical := make(map[string]bool, len(capture.Attempts))
	previous := capture.StartedAt
	for _, attempt := range capture.Attempts {
		if attempt.TemporalAttempt < 1 || attempt.ActorID == "" || attempt.ProcessIdentity == "" ||
			attempt.VendorSessionID == "" || attempt.PhysicalAttemptID == "" || seenPhysical[attempt.PhysicalAttemptID] ||
			!previous.Before(attempt.StartedAt) || !attempt.StartedAt.Before(attempt.AppliedAt) {
			return fmt.Errorf("%w: invalid or unordered Claude attempt capture", protocol.ErrInvalidEvidence)
		}
		seenPhysical[attempt.PhysicalAttemptID] = true
		previous = attempt.AppliedAt
	}
	if !previous.Before(capture.CompletedAt) {
		return fmt.Errorf("%w: completion does not follow physical attempts", protocol.ErrInvalidEvidence)
	}
	return validateCaptureFault(capture)
}

func validateCaptureShape(capture EvidenceCapture) error {
	wantAttempts := 0
	switch capture.Probe {
	case protocol.ProbeUnfaulted:
		wantAttempts = 1
	case protocol.ProbeUnsafe:
		wantAttempts = 2
	case protocol.ProbeProtected:
		wantAttempts = 1
	default:
		return fmt.Errorf("%w: unsupported Claude direct probe", protocol.ErrInvalidEvidence)
	}
	if capture.AdapterVersion == "" || capture.ClaudeBinarySHA256 == "" || capture.ClaudeVersion == "" ||
		capture.Model == "" || capture.Runtime == "" || capture.Trial < 1 || capture.LogicalSessionID == "" ||
		capture.LogicalTurnID == "" || capture.LogicalEffectID == "" || capture.DestinationID == "" ||
		capture.StartedAt.IsZero() || len(capture.Attempts) != wantAttempts || capture.CompletedAt.IsZero() ||
		len(capture.Settings) == 0 || len(capture.Native) == 0 {
		return fmt.Errorf("%w: incomplete Claude direct capture", protocol.ErrInvalidEvidence)
	}
	if !capture.RecoveryMode.valid() ||
		(capture.RecoveryMode.usesSelectedSession() && !validVendorSessionID(capture.SelectedVendorSessionID)) ||
		(capture.RecoveryMode.normalized() == RecoveryModeUnsafeFresh && capture.SelectedVendorSessionID != "") {
		return fmt.Errorf("%w: invalid Claude recovery mode or selected session", protocol.ErrInvalidEvidence)
	}
	if capture.RecoveryMode.usesSelectedSession() {
		for _, attempt := range capture.Attempts {
			if attempt.VendorSessionID != capture.SelectedVendorSessionID {
				return fmt.Errorf("%w: selected Claude session %q observed as %q",
					protocol.ErrInvalidEvidence, capture.SelectedVendorSessionID, attempt.VendorSessionID)
			}
		}
	}
	if !capture.FaultBoundary.valid() ||
		(capture.Probe == protocol.ProbeUnfaulted && capture.FaultBoundary != FaultNone) ||
		(capture.Probe != protocol.ProbeUnfaulted && capture.FaultBoundary == FaultNone) {
		return fmt.Errorf("%w: probe and fault boundary disagree", protocol.ErrInvalidEvidence)
	}
	if capture.Probe != protocol.ProbeUnfaulted &&
		(capture.Boundary.Point != capture.FaultBoundary || capture.Boundary.ActorID == "" ||
			capture.Boundary.ProcessIdentity == "" || capture.Boundary.ReachedAt.IsZero()) {
		return fmt.Errorf("%w: faulted capture lacks its Worker fault boundary", protocol.ErrInvalidEvidence)
	}
	return nil
}

func captureRunID(capture EvidenceCapture) string {
	return trialStorageID(capture.RecoveryMode, capture.Probe, capture.FaultBoundary, capture.Trial)
}

func validateCaptureFault(capture EvidenceCapture) error {
	if capture.Probe == protocol.ProbeUnfaulted && !capture.FaultAt.IsZero() {
		return fmt.Errorf("%w: unfaulted capture contains a fault", protocol.ErrInvalidEvidence)
	}
	if capture.Probe == protocol.ProbeUnsafe {
		if err := validateFaultOrder(capture); err != nil {
			return err
		}
	}
	if capture.Probe == protocol.ProbeProtected {
		if err := validateProtectedFaultOrder(capture); err != nil {
			return err
		}
	}
	for _, record := range capture.Native {
		if record.Kind == "" || record.Detail == "" {
			return fmt.Errorf("%w: invalid native capture", protocol.ErrInvalidEvidence)
		}
	}
	return nil
}

func validateProtectedFaultOrder(capture EvidenceCapture) error {
	if len(capture.Attempts) != 1 || len(capture.Attachments) != 1 {
		return fmt.Errorf("%w: protected fault lacks one execution and a recovery attachment", protocol.ErrInvalidEvidence)
	}
	attempt := capture.Attempts[0]
	attachment := capture.Attachments[0]
	valid := attachment.TemporalAttempt == 2 && attachment.ActorID != "" &&
		!attachment.AttachedAt.IsZero() && capture.Boundary.ReachedAt.Before(capture.FaultAt) &&
		capture.FaultAt.Before(attachment.AttachedAt) && attachment.Generation == 1 &&
		attachment.ProcessIdentity == attempt.ProcessIdentity
	if capture.FaultBoundary == FaultBeforeVendorRegistration {
		valid = valid && attempt.StartedAt.Before(capture.Boundary.ReachedAt) &&
			attachment.AttachedAt.Before(attempt.AppliedAt)
	} else if capture.FaultBoundary == FaultAfterClaimBeforeExec {
		valid = valid && attachment.AttachedAt.Before(attempt.StartedAt) &&
			attempt.StartedAt.Before(attempt.AppliedAt)
	} else {
		valid = valid && attempt.AppliedAt.Before(capture.Boundary.ReachedAt)
	}
	if !valid {
		return fmt.Errorf("%w: protected fault is not exactly ordered for %s", protocol.ErrInvalidEvidence, capture.FaultBoundary)
	}
	return nil
}

func validateFaultOrder(capture EvidenceCapture) error {
	first := capture.Attempts[0]
	second := capture.Attempts[1]
	valid := capture.Boundary.ReachedAt.Before(capture.FaultAt) &&
		first.AppliedAt.Before(capture.Boundary.ReachedAt) && capture.FaultAt.Before(second.StartedAt)
	if capture.FaultBoundary == FaultBeforeVendorRegistration {
		valid = first.StartedAt.Before(capture.Boundary.ReachedAt) &&
			capture.Boundary.ReachedAt.Before(capture.FaultAt) && capture.FaultAt.Before(first.AppliedAt) &&
			first.AppliedAt.Before(second.StartedAt)
	}
	if !valid {
		return fmt.Errorf("%w: unsafe fault is not exactly ordered for %s",
			protocol.ErrInvalidEvidence, capture.FaultBoundary)
	}
	return nil
}

func nativeRecordsForCapture(captures []NativeCapture) []protocol.NativeRecord {
	records := make([]protocol.NativeRecord, 0, len(captures))
	for index, capture := range captures {
		records = append(records, protocol.NativeRecord{
			Sequence: uint64(index + 1), Kind: capture.Kind, Detail: capture.Detail,
		})
	}
	return records
}

func cloneSettings(settings map[string]string) map[string]string {
	copyOfSettings := make(map[string]string, len(settings)+4)
	for key, value := range settings {
		copyOfSettings[key] = value
	}
	return copyOfSettings
}
