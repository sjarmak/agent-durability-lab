package evidenceadapter

import (
	"fmt"
	"runtime"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/evidence"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

type AttachedWriterCapture struct {
	AdapterVersion    string            `json:"adapter_version"`
	AgentSourceSHA256 string            `json:"agent_source_sha256"`
	Trial             int               `json:"trial"`
	Probe             protocol.Probe    `json:"probe"`
	SessionID         string            `json:"session_id"`
	DestinationID     string            `json:"destination_id"`
	LogicalEffectID   string            `json:"logical_effect_id"`
	Runtime           string            `json:"runtime"`
	StartedAt         time.Time         `json:"started_at"`
	OldOwner          ProcessCapture    `json:"old_owner"`
	CurrentOwner      ProcessCapture    `json:"current_owner"`
	ReplacementAt     time.Time         `json:"replacement_at"`
	BoundaryAt        time.Time         `json:"boundary_at"`
	StaleAttempt      AttemptCapture    `json:"stale_attempt"`
	CompletedAt       time.Time         `json:"completed_at"`
	Native            []NativeCapture   `json:"native"`
	Settings          map[string]string `json:"settings"`
}

func BuildAttachedWriterBundle(capture AttachedWriterCapture) (evidence.Bundle, error) {
	if err := validateAttachedWriterCapture(capture); err != nil {
		return evidence.Bundle{}, err
	}
	staleKind, staleDecision := protocol.EventEffectRejected, "stale_generation"
	acceptedActions := []protocol.AcceptedAction{}
	if capture.StaleAttempt.Applied {
		staleKind, staleDecision = protocol.EventEffectAccepted, "accepted"
		acceptedActions = append(acceptedActions, protocol.AcceptedAction{Kind: "effect", Generation: 1, Sequence: 3})
	}
	events := []protocol.Event{
		attachedEvent(1, capture.StartedAt, protocol.EventExecutorRegistered, capture, capture.OldOwner, 1, "", "", "observed"),
		attachedEvent(2, capture.ReplacementAt, protocol.EventOwnerReplaced, capture, capture.CurrentOwner, 2, "", "", "accepted"),
		attachedEvent(3, capture.StaleAttempt.ObservedAt, staleKind, capture, capture.OldOwner, 1, capture.LogicalEffectID, capture.StaleAttempt.PhysicalAttemptID, staleDecision),
		attachedEvent(4, capture.CompletedAt, protocol.EventOutcomeAccepted, capture, capture.CurrentOwner, 2, "", "", "accepted"),
	}
	settings := map[string]string{"probe": string(capture.Probe), "attached_reference": "opaque-routing-reference"}
	for key, value := range capture.Settings {
		settings[key] = value
	}
	return evidence.Bundle{
		Identity: evidence.RunIdentity{
			RunID: fmt.Sprintf("sandbox-harness-attached-writer-%s-trial-%d", capture.Probe, capture.Trial),
			Case:  protocol.CaseStaleGeneration, Probe: capture.Probe, Trial: capture.Trial, SessionID: capture.SessionID,
		},
		Events: events,
		Authority: protocol.AuthorityState{
			SessionID: capture.SessionID, ActiveGeneration: 2, ConcurrentOwnerCount: 1,
			CurrentOwnerAlive: true, AcceptedOutcomes: []protocol.AcceptedAction{{Kind: "outcome", Generation: 2, Sequence: 4}},
			AcceptedActions: acceptedActions,
		},
		Destination: protocol.DestinationState{
			DestinationID: capture.DestinationID,
			Attempts: []protocol.DestinationAttempt{{
				LogicalEffectID: capture.LogicalEffectID, PhysicalAttemptID: capture.StaleAttempt.PhysicalAttemptID,
				Generation: 1, Sequence: 3, Applied: capture.StaleAttempt.Applied,
			}},
		},
		Fault: protocol.FaultBoundary{
			Point: "replacement-committed-before-stale-actions", Triggered: true,
			AfterSequence: 2, BeforeSequence: 3, ActorID: capture.OldOwner.ActorID,
			ProcessIdentity: capture.OldOwner.Identity, TriggeredAt: capture.BoundaryAt.Format(time.RFC3339Nano),
		},
		Processes: []protocol.ProcessObservation{
			{Sequence: 1, ActorID: capture.OldOwner.ActorID, Generation: 1, ProcessIdentity: capture.OldOwner.Identity, State: "running"},
			{Sequence: 2, ActorID: capture.CurrentOwner.ActorID, Generation: 2, ProcessIdentity: capture.CurrentOwner.Identity, State: "running"},
		},
		Native: nativeRecords(capture.Native),
		Input: protocol.EffectiveInput{
			AdapterID: adapterID, AdapterVersion: capture.AdapterVersion,
			AgentProtocol: protocol.AgentProtocol, AgentBinarySHA256: capture.AgentSourceSHA256,
			AuthorityProtocol: protocol.AuthorityProtocol, DestinationProtocol: protocol.DestinationProtocol,
			DestinationID: capture.DestinationID, FailureProtocol: protocol.FailureProtocol,
			OracleProtocol: protocol.OracleProtocol,
			OracleVisibility: []string{
				protocol.AuthorityStateFile, protocol.DestinationStateFile,
				protocol.FaultBoundaryFile, protocol.ProcessObservationsFile,
			},
			Runtime: capture.Runtime + "; adapter=" + runtime.GOOS + "/" + runtime.GOARCH, Settings: settings,
		},
	}, nil
}

func validateAttachedWriterCapture(capture AttachedWriterCapture) error {
	if capture.AdapterVersion == "" || capture.AgentSourceSHA256 == "" || capture.Trial < 1 ||
		capture.SessionID == "" || capture.DestinationID == "" || capture.LogicalEffectID == "" ||
		capture.Runtime == "" || capture.OldOwner.ActorID == "" || capture.OldOwner.Identity == "" ||
		capture.CurrentOwner.ActorID == "" || capture.CurrentOwner.Identity == "" ||
		capture.StaleAttempt.PhysicalAttemptID == "" || len(capture.Native) == 0 {
		return fmt.Errorf("%w: incomplete attached-writer capture", protocol.ErrInvalidEvidence)
	}
	if capture.Probe != protocol.ProbeUnsafe && capture.Probe != protocol.ProbeProtected {
		return fmt.Errorf("%w: unsupported probe", protocol.ErrInvalidEvidence)
	}
	if (capture.Probe == protocol.ProbeUnsafe) != capture.StaleAttempt.Applied {
		return fmt.Errorf("%w: capture does not distinguish stale-writer arms", protocol.ErrInvalidEvidence)
	}
	ordered := capture.StartedAt.Before(capture.ReplacementAt) &&
		capture.ReplacementAt.Before(capture.BoundaryAt) &&
		capture.BoundaryAt.Before(capture.StaleAttempt.ObservedAt) &&
		capture.StaleAttempt.ObservedAt.Before(capture.CompletedAt)
	if !ordered {
		return fmt.Errorf("%w: authority replacement is not exactly bracketed", protocol.ErrInvalidEvidence)
	}
	return nil
}

func attachedEvent(
	sequence uint64,
	at time.Time,
	kind string,
	capture AttachedWriterCapture,
	process ProcessCapture,
	generation uint64,
	effectID string,
	attemptID string,
	decision string,
) protocol.Event {
	return protocol.Event{
		Sequence: sequence, Time: at.Format(time.RFC3339Nano), Kind: kind, SessionID: capture.SessionID,
		ActorID: process.ActorID, Generation: generation, ProcessIdentity: process.Identity,
		LogicalEffectID: effectID, PhysicalAttemptID: attemptID, Decision: decision,
	}
}
