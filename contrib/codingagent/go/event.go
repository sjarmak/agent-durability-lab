package codingagent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"
)

type EventType string

const (
	EventTransitionRecorded    EventType = "transition_recorded"
	EventEffectReceiptRecorded EventType = "effect_receipt_recorded"
	EventStopDeliveryRecorded  EventType = "stop_delivery_recorded"
	EventRecoveryObserved      EventType = "recovery_observed"
	EventEvidenceRecorded      EventType = "evidence_recorded"
)

func (eventType EventType) valid() bool {
	switch eventType {
	case EventTransitionRecorded, EventEffectReceiptRecorded, EventStopDeliveryRecorded,
		EventRecoveryObserved, EventEvidenceRecorded:
		return true
	default:
		return false
	}
}

type SourceLayer string

const (
	SourceTemporal    SourceLayer = "temporal"
	SourceApplication SourceLayer = "application"
	SourceDestination SourceLayer = "destination"
	SourceProvider    SourceLayer = "provider"
)

func (layer SourceLayer) valid() bool {
	return layer == SourceTemporal || layer == SourceApplication || layer == SourceDestination || layer == SourceProvider
}

type EventSource struct {
	Layer           SourceLayer `json:"layer"`
	Component       string      `json:"component"`
	TemporalAttempt uint64      `json:"temporal_attempt,omitempty"`
	WorkerIdentity  string      `json:"worker_identity,omitempty"`
	ProcessIdentity string      `json:"process_identity,omitempty"`
}

func (source EventSource) valid() bool {
	return source.Layer.valid() && len(source.Component) >= 1 && len(source.Component) <= 120 &&
		len(source.WorkerIdentity) <= 256 && len(source.ProcessIdentity) <= 256
}

type EventReference struct {
	TransitionID ID     `json:"transition_id,omitempty"`
	ReceiptID    ID     `json:"receipt_id,omitempty"`
	ArtifactPath string `json:"artifact_path,omitempty"`
	ContentHash  Digest `json:"content_hash,omitempty"`
}

func (reference EventReference) valid() bool {
	count := 0
	if reference.TransitionID != "" {
		count++
		if !reference.TransitionID.valid() {
			return false
		}
	}
	if reference.ReceiptID != "" {
		count++
		if !reference.ReceiptID.valid() {
			return false
		}
	}
	if reference.ArtifactPath != "" {
		count++
		if !confinedArtifactPath(reference.ArtifactPath) {
			return false
		}
	}
	if reference.ContentHash != "" {
		count++
		if !reference.ContentHash.valid() {
			return false
		}
	}
	return count > 0
}

type EventEnvelope struct {
	SchemaVersion   string          `json:"schema_version"`
	EventID         ID              `json:"event_id"`
	EventType       EventType       `json:"event_type"`
	SessionID       ID              `json:"session_id"`
	TurnID          ID              `json:"turn_id"`
	OperationID     ID              `json:"operation_id"`
	EffectID        ID              `json:"effect_id,omitempty"`
	Generation      uint64          `json:"generation"`
	AuthorityStatus AuthorityStatus `json:"authority_status"`
	OccurredAt      time.Time       `json:"occurred_at"`
	Source          EventSource     `json:"source"`
	Sequence        uint64          `json:"sequence"`
	Reference       EventReference  `json:"reference"`
	ReasonCode      string          `json:"reason_code,omitempty"`
}

func (event EventEnvelope) Validate() error {
	if event.SchemaVersion != SchemaVersion || !event.EventID.valid() || !event.EventType.valid() ||
		!event.SessionID.valid() || !event.TurnID.valid() || !event.OperationID.valid() ||
		event.Generation == 0 || (event.AuthorityStatus != AuthorityCurrent && event.AuthorityStatus != AuthorityRevoked) ||
		event.Sequence == 0 || event.OccurredAt.IsZero() || event.OccurredAt.Location() != time.UTC ||
		!event.Source.valid() || !event.Reference.valid() {
		return fmt.Errorf("%w: event envelope", ErrInvalidInput)
	}
	if event.EffectID != "" && !event.EffectID.valid() {
		return fmt.Errorf("%w: event effect ID", ErrInvalidInput)
	}
	if event.ReasonCode != "" && !reasonCodePattern.MatchString(event.ReasonCode) {
		return fmt.Errorf("%w: event reason code", ErrInvalidInput)
	}
	return nil
}

type eventWire struct {
	SchemaVersion   string          `json:"schema_version"`
	EventID         ID              `json:"event_id"`
	EventType       EventType       `json:"event_type"`
	SessionID       ID              `json:"session_id"`
	TurnID          ID              `json:"turn_id"`
	OperationID     ID              `json:"operation_id"`
	EffectID        ID              `json:"effect_id,omitempty"`
	Generation      uint64          `json:"generation"`
	AuthorityStatus AuthorityStatus `json:"authority_status"`
	OccurredAt      string          `json:"occurred_at"`
	Source          EventSource     `json:"source"`
	Sequence        uint64          `json:"sequence"`
	Reference       EventReference  `json:"reference"`
	ReasonCode      string          `json:"reason_code,omitempty"`
}

func DecodeEventEnvelope(data []byte) (EventEnvelope, error) {
	if err := rejectDuplicateKeys(data); err != nil {
		return EventEnvelope{}, err
	}
	var wire eventWire
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return EventEnvelope{}, fmt.Errorf("decode event: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return EventEnvelope{}, err
	}
	occurredAt, err := parseUTC(wire.OccurredAt)
	if err != nil {
		return EventEnvelope{}, err
	}
	event := EventEnvelope{
		SchemaVersion: wire.SchemaVersion, EventID: wire.EventID, EventType: wire.EventType,
		SessionID: wire.SessionID, TurnID: wire.TurnID, OperationID: wire.OperationID,
		EffectID: wire.EffectID, Generation: wire.Generation, AuthorityStatus: wire.AuthorityStatus,
		OccurredAt: occurredAt, Source: wire.Source, Sequence: wire.Sequence,
		Reference: wire.Reference, ReasonCode: wire.ReasonCode,
	}
	if err := event.Validate(); err != nil {
		return EventEnvelope{}, err
	}
	return event, nil
}

// ArtifactReference names a confined, content-addressed artifact. Path is
// metadata beneath a caller-owned root; it is never joined without confinement.
type ArtifactReference struct {
	Path   string `json:"path"`
	Digest Digest `json:"digest"`
}

func (reference ArtifactReference) Validate() error {
	if !confinedArtifactPath(reference.Path) || !reference.Digest.valid() {
		return fmt.Errorf("%w: artifact reference", ErrInvalidInput)
	}
	return nil
}

func confinedArtifactPath(value string) bool {
	return value != "" && len(value) <= 512 && value == path.Clean(value) && !path.IsAbs(value) &&
		value != ".." && !strings.HasPrefix(value, "../") && !strings.Contains(value, `\`) &&
		!strings.ContainsRune(value, 0) && !hasWindowsVolume(value)
}

func hasWindowsVolume(value string) bool {
	return len(value) >= 2 && value[1] == ':' &&
		((value[0] >= 'A' && value[0] <= 'Z') || (value[0] >= 'a' && value[0] <= 'z'))
}
