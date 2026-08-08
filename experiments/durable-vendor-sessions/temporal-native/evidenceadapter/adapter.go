package evidenceadapter

import (
	"fmt"
	"runtime"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/evidence"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

const adapterID = "temporal-native-openai-agents"

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

type Capture struct {
	AdapterVersion    string            `json:"adapter_version"`
	Trial             int               `json:"trial"`
	Probe             protocol.Probe    `json:"probe"`
	SessionID         string            `json:"session_id"`
	DestinationID     string            `json:"destination_id"`
	LogicalEffectID   string            `json:"logical_effect_id"`
	Generation        uint64            `json:"generation"`
	AgentSourceSHA256 string            `json:"agent_source_sha256"`
	Runtime           string            `json:"runtime"`
	StartedAt         time.Time         `json:"started_at"`
	FirstWorker       ProcessCapture    `json:"first_worker"`
	RecoveryWorker    ProcessCapture    `json:"recovery_worker"`
	Attempts          []AttemptCapture  `json:"attempts"`
	Fault             FaultCapture      `json:"fault"`
	CompletedAt       time.Time         `json:"completed_at"`
	History           []NativeCapture   `json:"history"`
	Settings          map[string]string `json:"settings"`
}

func BuildBundle(capture Capture) (evidence.Bundle, error) {
	if err := validateCapture(capture); err != nil {
		return evidence.Bundle{}, err
	}
	runID := fmt.Sprintf("temporal-native-ambiguous-effect-%s-trial-%d", capture.Probe, capture.Trial)
	events := []protocol.Event{
		event(1, capture.StartedAt, protocol.EventExecutorRegistered, capture, capture.FirstWorker, "", "", "observed"),
		event(2, capture.Attempts[0].ObservedAt, protocol.EventEffectAccepted, capture, capture.FirstWorker, capture.LogicalEffectID, capture.Attempts[0].PhysicalAttemptID, "accepted"),
	}
	secondKind, secondDecision := protocol.EventEffectAccepted, "accepted"
	if !capture.Attempts[1].Applied {
		secondKind, secondDecision = protocol.EventEffectRejected, "duplicate"
	}
	events = append(events,
		event(3, capture.Attempts[1].ObservedAt, secondKind, capture, capture.RecoveryWorker, capture.LogicalEffectID, capture.Attempts[1].PhysicalAttemptID, secondDecision),
		event(4, capture.CompletedAt, protocol.EventOutcomeAccepted, capture, capture.RecoveryWorker, "", "", "accepted"),
	)
	actions := []protocol.AcceptedAction{{Kind: "effect", Generation: capture.Generation, Sequence: 2}}
	if capture.Attempts[1].Applied {
		actions = append(actions, protocol.AcceptedAction{Kind: "effect", Generation: capture.Generation, Sequence: 3})
	}
	destinationAttempts := make([]protocol.DestinationAttempt, 0, len(capture.Attempts))
	for index, attempt := range capture.Attempts {
		destinationAttempts = append(destinationAttempts, protocol.DestinationAttempt{
			LogicalEffectID: capture.LogicalEffectID, PhysicalAttemptID: attempt.PhysicalAttemptID,
			Generation: capture.Generation, Sequence: uint64(index + 2), Applied: attempt.Applied,
		})
	}
	native := make([]protocol.NativeRecord, 0, len(capture.History))
	for index, record := range capture.History {
		native = append(native, protocol.NativeRecord{Sequence: uint64(index + 1), Kind: record.Kind, Detail: record.Detail})
	}
	settings := map[string]string{
		"fault_selection": "named-barrier", "model_dispatch": "temporal-openai-agents-plugin",
		"probe": string(capture.Probe), "worker_recovery": "external-process",
	}
	for key, value := range capture.Settings {
		settings[key] = value
	}
	return evidence.Bundle{
		Identity: evidence.RunIdentity{RunID: runID, Case: protocol.CaseAmbiguousEffect, Probe: capture.Probe, Trial: capture.Trial, SessionID: capture.SessionID},
		Events:   events,
		Authority: protocol.AuthorityState{
			SessionID: capture.SessionID, ActiveGeneration: capture.Generation,
			ConcurrentOwnerCount: 1, CurrentOwnerAlive: true,
			AcceptedOutcomes: []protocol.AcceptedAction{{Kind: "outcome", Generation: capture.Generation, Sequence: 4}},
			AcceptedActions:  actions,
		},
		Destination: protocol.DestinationState{DestinationID: capture.DestinationID, Attempts: destinationAttempts},
		Fault: protocol.FaultBoundary{
			Point: "effect-confirmed-before-step-completion", Triggered: true,
			AfterSequence: 2, BeforeSequence: 3, ActorID: capture.FirstWorker.ActorID,
			ProcessIdentity: capture.FirstWorker.Identity, TriggeredAt: capture.Fault.TriggeredAt.Format(time.RFC3339Nano),
		},
		Processes: []protocol.ProcessObservation{
			{Sequence: 1, ActorID: capture.FirstWorker.ActorID, Generation: capture.Generation, ProcessIdentity: capture.FirstWorker.Identity, State: "running"},
			{Sequence: 3, ActorID: capture.RecoveryWorker.ActorID, Generation: capture.Generation, ProcessIdentity: capture.RecoveryWorker.Identity, State: "running"},
		},
		Native: native,
		Input: protocol.EffectiveInput{
			AdapterID: adapterID, AdapterVersion: capture.AdapterVersion,
			AgentProtocol: protocol.AgentProtocol, AgentBinarySHA256: capture.AgentSourceSHA256,
			AuthorityProtocol: protocol.AuthorityProtocol, DestinationProtocol: protocol.DestinationProtocol,
			DestinationID: capture.DestinationID, FailureProtocol: protocol.FailureProtocol,
			OracleProtocol:   protocol.OracleProtocol,
			OracleVisibility: []string{protocol.AuthorityStateFile, protocol.DestinationStateFile, protocol.FaultBoundaryFile, protocol.ProcessObservationsFile},
			Runtime:          capture.Runtime + "; adapter=" + runtime.GOOS + "/" + runtime.GOARCH,
			Settings:         settings,
		},
	}, nil
}

func validateCapture(capture Capture) error {
	if capture.AdapterVersion == "" || capture.Trial < 1 || capture.SessionID == "" || capture.DestinationID == "" ||
		capture.LogicalEffectID == "" || capture.Generation == 0 || capture.AgentSourceSHA256 == "" || capture.Runtime == "" ||
		capture.FirstWorker.ActorID == "" || capture.FirstWorker.Identity == "" || capture.RecoveryWorker.ActorID == "" ||
		capture.RecoveryWorker.Identity == "" || len(capture.Attempts) != 2 || len(capture.History) == 0 {
		return fmt.Errorf("%w: incomplete Temporal-native capture", protocol.ErrInvalidEvidence)
	}
	if capture.Probe != protocol.ProbeUnsafe && capture.Probe != protocol.ProbeProtected {
		return fmt.Errorf("%w: unsupported probe", protocol.ErrInvalidEvidence)
	}
	if capture.Fault.Point != "tool-effect-committed" || !capture.Attempts[0].Applied ||
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
		return fmt.Errorf("%w: native fault is not exactly bracketed", protocol.ErrInvalidEvidence)
	}
	return nil
}

func event(sequence uint64, at time.Time, kind string, capture Capture, process ProcessCapture, effectID, attemptID, decision string) protocol.Event {
	return protocol.Event{
		Sequence: sequence, Time: at.Format(time.RFC3339Nano), Kind: kind,
		SessionID: capture.SessionID, ActorID: process.ActorID, Generation: capture.Generation,
		ProcessIdentity: process.Identity, LogicalEffectID: effectID,
		PhysicalAttemptID: attemptID, Decision: decision,
	}
}
