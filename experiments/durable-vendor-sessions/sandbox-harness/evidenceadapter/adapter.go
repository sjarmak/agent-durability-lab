package evidenceadapter

import (
	"fmt"
	"runtime"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/evidence"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

const adapterID = "temporal-sandbox-orchestration-harness"

type ProcessCapture struct {
	ActorID  string `json:"actor_id"`
	Identity string `json:"identity"`
}

type AttemptCapture struct {
	PhysicalAttemptID string    `json:"physical_attempt_id"`
	Applied           bool      `json:"applied"`
	ObservedAt        time.Time `json:"observed_at"`
}

type FaultCapture struct {
	Point       string    `json:"point"`
	TriggeredAt time.Time `json:"triggered_at"`
}

type NativeCapture struct {
	Kind   string `json:"kind"`
	Detail string `json:"detail"`
}

type AmbiguousEffectCapture struct {
	AdapterVersion    string            `json:"adapter_version"`
	AgentSourceSHA256 string            `json:"agent_source_sha256"`
	Operation         string            `json:"operation"`
	Trial             int               `json:"trial"`
	Probe             protocol.Probe    `json:"probe"`
	SessionID         string            `json:"session_id"`
	DestinationID     string            `json:"destination_id"`
	LogicalEffectID   string            `json:"logical_effect_id"`
	Generation        uint64            `json:"generation"`
	Runtime           string            `json:"runtime"`
	StartedAt         time.Time         `json:"started_at"`
	FirstWorker       ProcessCapture    `json:"first_worker"`
	RecoveryWorker    ProcessCapture    `json:"recovery_worker"`
	Attempts          []AttemptCapture  `json:"attempts"`
	Fault             FaultCapture      `json:"fault"`
	CompletedAt       time.Time         `json:"completed_at"`
	Native            []NativeCapture   `json:"native"`
	Settings          map[string]string `json:"settings"`
}

func BuildAmbiguousEffectBundle(capture AmbiguousEffectCapture) (evidence.Bundle, error) {
	if err := validateAmbiguousEffectCapture(capture); err != nil {
		return evidence.Bundle{}, err
	}
	runID := fmt.Sprintf(
		"sandbox-harness-%s-ambiguous-effect-%s-trial-%d",
		capture.Operation, capture.Probe, capture.Trial,
	)
	events := buildEvents(capture)
	actions := []protocol.AcceptedAction{{Kind: "effect", Generation: capture.Generation, Sequence: 2}}
	if capture.Attempts[1].Applied {
		actions = append(actions, protocol.AcceptedAction{Kind: "effect", Generation: capture.Generation, Sequence: 3})
	}
	return evidence.Bundle{
		Identity: evidence.RunIdentity{
			RunID: runID, Case: protocol.CaseAmbiguousEffect, Probe: capture.Probe,
			Trial: capture.Trial, SessionID: capture.SessionID,
		},
		Events: events,
		Authority: protocol.AuthorityState{
			SessionID: capture.SessionID, ActiveGeneration: capture.Generation,
			ConcurrentOwnerCount: 1, CurrentOwnerAlive: true,
			AcceptedOutcomes: []protocol.AcceptedAction{{Kind: "outcome", Generation: capture.Generation, Sequence: 4}},
			AcceptedActions:  actions,
		},
		Destination: destinationState(capture),
		Fault: protocol.FaultBoundary{
			Point: "effect-confirmed-before-step-completion", Triggered: true,
			AfterSequence: 2, BeforeSequence: 3, ActorID: capture.FirstWorker.ActorID,
			ProcessIdentity: capture.FirstWorker.Identity,
			TriggeredAt:     capture.Fault.TriggeredAt.Format(time.RFC3339Nano),
		},
		Processes: []protocol.ProcessObservation{
			{Sequence: 1, ActorID: capture.FirstWorker.ActorID, Generation: capture.Generation, ProcessIdentity: capture.FirstWorker.Identity, State: "running"},
			{Sequence: 3, ActorID: capture.RecoveryWorker.ActorID, Generation: capture.Generation, ProcessIdentity: capture.RecoveryWorker.Identity, State: "running"},
		},
		Native: nativeRecords(capture.Native),
		Input:  effectiveInput(capture),
	}, nil
}

func buildEvents(capture AmbiguousEffectCapture) []protocol.Event {
	events := []protocol.Event{
		event(1, capture.StartedAt, protocol.EventExecutorRegistered, capture, capture.FirstWorker, "", "", "observed"),
		event(2, capture.Attempts[0].ObservedAt, protocol.EventEffectAccepted, capture, capture.FirstWorker, capture.LogicalEffectID, capture.Attempts[0].PhysicalAttemptID, "accepted"),
	}
	secondKind, secondDecision := protocol.EventEffectAccepted, "accepted"
	if !capture.Attempts[1].Applied {
		secondKind, secondDecision = protocol.EventEffectRejected, "duplicate"
	}
	return append(events,
		event(3, capture.Attempts[1].ObservedAt, secondKind, capture, capture.RecoveryWorker, capture.LogicalEffectID, capture.Attempts[1].PhysicalAttemptID, secondDecision),
		event(4, capture.CompletedAt, protocol.EventOutcomeAccepted, capture, capture.RecoveryWorker, "", "", "accepted"),
	)
}

func destinationState(capture AmbiguousEffectCapture) protocol.DestinationState {
	attempts := make([]protocol.DestinationAttempt, 0, len(capture.Attempts))
	for index, attempt := range capture.Attempts {
		attempts = append(attempts, protocol.DestinationAttempt{
			LogicalEffectID:   capture.LogicalEffectID,
			PhysicalAttemptID: attempt.PhysicalAttemptID,
			Generation:        capture.Generation, Sequence: uint64(index + 2), Applied: attempt.Applied,
		})
	}
	return protocol.DestinationState{DestinationID: capture.DestinationID, Attempts: attempts}
}

func nativeRecords(captures []NativeCapture) []protocol.NativeRecord {
	records := make([]protocol.NativeRecord, 0, len(captures))
	for index, capture := range captures {
		records = append(records, protocol.NativeRecord{
			Sequence: uint64(index + 1), Kind: capture.Kind, Detail: capture.Detail,
		})
	}
	return records
}

func effectiveInput(capture AmbiguousEffectCapture) protocol.EffectiveInput {
	settings := map[string]string{
		"fault_selection": "named-barrier", "operation": capture.Operation,
		"probe": string(capture.Probe), "upstream_update_identity": "stable",
	}
	for key, value := range capture.Settings {
		settings[key] = value
	}
	return protocol.EffectiveInput{
		AdapterID: adapterID, AdapterVersion: capture.AdapterVersion,
		AgentProtocol: protocol.AgentProtocol, AgentBinarySHA256: capture.AgentSourceSHA256,
		AuthorityProtocol: protocol.AuthorityProtocol, DestinationProtocol: protocol.DestinationProtocol,
		DestinationID: capture.DestinationID, FailureProtocol: protocol.FailureProtocol,
		OracleProtocol: protocol.OracleProtocol,
		OracleVisibility: []string{
			protocol.AuthorityStateFile, protocol.DestinationStateFile,
			protocol.FaultBoundaryFile, protocol.ProcessObservationsFile,
		},
		Runtime:  capture.Runtime + "; adapter=" + runtime.GOOS + "/" + runtime.GOARCH,
		Settings: settings,
	}
}

func validateAmbiguousEffectCapture(capture AmbiguousEffectCapture) error {
	if capture.AdapterVersion == "" || capture.AgentSourceSHA256 == "" || capture.Operation == "" ||
		capture.Trial < 1 || capture.SessionID == "" || capture.DestinationID == "" ||
		capture.LogicalEffectID == "" || capture.Generation == 0 || capture.Runtime == "" ||
		capture.FirstWorker.ActorID == "" || capture.FirstWorker.Identity == "" ||
		capture.RecoveryWorker.ActorID == "" || capture.RecoveryWorker.Identity == "" ||
		len(capture.Attempts) != 2 || len(capture.Native) == 0 {
		return fmt.Errorf("%w: incomplete sandbox-harness capture", protocol.ErrInvalidEvidence)
	}
	if capture.Probe != protocol.ProbeUnsafe && capture.Probe != protocol.ProbeProtected {
		return fmt.Errorf("%w: unsupported probe", protocol.ErrInvalidEvidence)
	}
	if capture.Fault.Point != "provider-effect-committed" || !capture.Attempts[0].Applied ||
		(capture.Probe == protocol.ProbeUnsafe) != capture.Attempts[1].Applied {
		return fmt.Errorf("%w: capture does not distinguish unsafe and protected effects", protocol.ErrInvalidEvidence)
	}
	if capture.Attempts[0].PhysicalAttemptID == "" || capture.Attempts[1].PhysicalAttemptID == "" ||
		capture.Attempts[0].PhysicalAttemptID == capture.Attempts[1].PhysicalAttemptID {
		return fmt.Errorf("%w: invalid physical attempt identity", protocol.ErrInvalidEvidence)
	}
	ordered := capture.StartedAt.Before(capture.Attempts[0].ObservedAt) &&
		capture.Attempts[0].ObservedAt.Before(capture.Fault.TriggeredAt) &&
		capture.Fault.TriggeredAt.Before(capture.Attempts[1].ObservedAt) &&
		capture.Attempts[1].ObservedAt.Before(capture.CompletedAt)
	if !ordered {
		return fmt.Errorf("%w: provider fault is not exactly bracketed", protocol.ErrInvalidEvidence)
	}
	return nil
}

func event(
	sequence uint64,
	at time.Time,
	kind string,
	capture AmbiguousEffectCapture,
	process ProcessCapture,
	effectID string,
	attemptID string,
	decision string,
) protocol.Event {
	return protocol.Event{
		Sequence: sequence, Time: at.Format(time.RFC3339Nano), Kind: kind,
		SessionID: capture.SessionID, ActorID: process.ActorID, Generation: capture.Generation,
		ProcessIdentity: process.Identity, LogicalEffectID: effectID,
		PhysicalAttemptID: attemptID, Decision: decision,
	}
}
