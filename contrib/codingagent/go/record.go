package codingagent

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	MaxJSONDocumentBytes   = 4 << 20
	MaxJSONDepth           = 64
	MaxJSONCollectionItems = 10_000
)

type TransitionRecord struct {
	SchemaVersion  string          `json:"schema_version"`
	TransitionID   ID              `json:"transition_id"`
	SessionID      ID              `json:"session_id"`
	TurnID         ID              `json:"turn_id"`
	OperationID    ID              `json:"operation_id"`
	RequestHash    Digest          `json:"request_hash"`
	Operation      Operation       `json:"operation"`
	OccurredAt     time.Time       `json:"-"`
	Actor          json.RawMessage `json:"actor"`
	AuthorityCheck string          `json:"authority_check"`
	Before         *TurnState      `json:"before"`
	After          *TurnState      `json:"after"`
	Decision       json.RawMessage `json:"decision"`
	Receipt        json.RawMessage `json:"receipt,omitempty"`
	Rejection      json.RawMessage `json:"rejection,omitempty"`
}

type transitionWire struct {
	SchemaVersion  string          `json:"schema_version"`
	TransitionID   ID              `json:"transition_id"`
	SessionID      ID              `json:"session_id"`
	TurnID         ID              `json:"turn_id"`
	OperationID    ID              `json:"operation_id"`
	RequestHash    Digest          `json:"request_hash"`
	Operation      Operation       `json:"operation"`
	OccurredAt     string          `json:"occurred_at"`
	Actor          json.RawMessage `json:"actor"`
	AuthorityCheck string          `json:"authority_check"`
	Before         *TurnState      `json:"before"`
	After          *TurnState      `json:"after"`
	Decision       json.RawMessage `json:"decision"`
	Receipt        json.RawMessage `json:"receipt,omitempty"`
	Rejection      json.RawMessage `json:"rejection,omitempty"`
}

func DecodeTransitionRecord(data []byte) (TransitionRecord, error) {
	if err := rejectDuplicateKeys(data); err != nil {
		return TransitionRecord{}, err
	}
	var wire transitionWire
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return TransitionRecord{}, fmt.Errorf("decode transition record: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return TransitionRecord{}, err
	}
	occurredAt, err := parseUTC(wire.OccurredAt)
	if err != nil {
		return TransitionRecord{}, fmt.Errorf("decode transition record: %w", err)
	}
	if wire.SchemaVersion != SchemaVersion || !wire.TransitionID.valid() || !wire.SessionID.valid() ||
		!wire.TurnID.valid() || !wire.OperationID.valid() || !wire.RequestHash.valid() || !wire.Operation.Valid() ||
		len(wire.Actor) == 0 || len(wire.Decision) == 0 {
		return TransitionRecord{}, fmt.Errorf("%w: transition metadata", ErrInvalidInput)
	}
	return TransitionRecord{
		SchemaVersion: wire.SchemaVersion, TransitionID: wire.TransitionID, SessionID: wire.SessionID,
		TurnID: wire.TurnID, OperationID: wire.OperationID, RequestHash: wire.RequestHash,
		Operation: wire.Operation, OccurredAt: occurredAt, Actor: wire.Actor,
		AuthorityCheck: wire.AuthorityCheck, Before: wire.Before, After: wire.After,
		Decision: wire.Decision, Receipt: wire.Receipt, Rejection: wire.Rejection,
	}, nil
}

func rejectDuplicateKeys(data []byte) error {
	if len(data) > MaxJSONDocumentBytes {
		return fmt.Errorf("JSON document exceeds %d bytes", MaxJSONDocumentBytes)
	}
	if !utf8.Valid(data) {
		return errors.New("JSON document is not valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := scanJSONValue(decoder, 1); err != nil {
		return fmt.Errorf("validate JSON keys: %w", err)
	}
	return requireJSONEOF(decoder)
}

func scanJSONValue(decoder *json.Decoder, depth int) error {
	if depth > MaxJSONDepth {
		return fmt.Errorf("JSON nesting exceeds %d levels", MaxJSONDepth)
	}
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
		seen := make(map[string]struct{})
		items := 0
		for decoder.More() {
			items++
			if items > MaxJSONCollectionItems {
				return fmt.Errorf("JSON object exceeds %d fields", MaxJSONCollectionItems)
			}
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("object key is not a string")
			}
			if _, exists := seen[key]; exists {
				return fmt.Errorf("duplicate object key %q", key)
			}
			seen[key] = struct{}{}
			if err := scanJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
	case '[':
		items := 0
		for decoder.More() {
			items++
			if items > MaxJSONCollectionItems {
				return fmt.Errorf("JSON array exceeds %d items", MaxJSONCollectionItems)
			}
			if err := scanJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
	default:
		return errors.New("unexpected JSON delimiter")
	}
	_, err = decoder.Token()
	return err
}

func requireJSONEOF(decoder *json.Decoder) error {
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("unexpected trailing JSON value")
		}
		return fmt.Errorf("read trailing JSON: %w", err)
	}
	return nil
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
