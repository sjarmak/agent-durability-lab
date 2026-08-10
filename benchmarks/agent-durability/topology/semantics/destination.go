package semantics

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"sync"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
)

type Authority struct {
	Generation     uint64 `json:"generation"`
	CapabilityHash string `json:"capability_hash"`
}

func (a Authority) validate() error {
	decoded, err := hex.DecodeString(a.CapabilityHash)
	if a.Generation == 0 || err != nil || len(decoded) != sha256.Size || a.CapabilityHash != hex.EncodeToString(decoded) {
		return fmt.Errorf("%w: destination authority", protocol.ErrInvalidEvidence)
	}
	return nil
}

type EffectRequest struct {
	EventID         string         `json:"event_id"`
	ItemID          string         `json:"item_id"`
	LogicalEffectID string         `json:"logical_effect_id"`
	Authority       Authority      `json:"authority"`
	Probe           protocol.Probe `json:"probe"`
}

type DestructiveRequest struct {
	EventID         string         `json:"event_id"`
	ItemID          string         `json:"item_id"`
	OperationID     string         `json:"operation_id"`
	Authority       Authority      `json:"authority"`
	ExpectedVersion uint64         `json:"expected_version"`
	Attempt         int            `json:"attempt"`
	Probe           protocol.Probe `json:"probe"`
}

type DestructiveResult struct {
	OperationID      string `json:"operation_id"`
	Decision         string `json:"decision"`
	Applied          bool   `json:"applied"`
	ReceiptID        string `json:"receipt_id,omitempty"`
	PreviousVersion  uint64 `json:"previous_version"`
	ResultingVersion uint64 `json:"resulting_version"`
}

type DestinationSnapshot struct {
	Version               uint64                       `json:"version"`
	DestructiveApplyCount int                          `json:"destructive_apply_count"`
	Actions               []protocol.DestinationAction `json:"actions"`
}

type MemoryDestination struct {
	mu                    sync.Mutex
	authority             map[string]Authority
	effects               map[string]string
	operations            map[string]DestructiveResult
	eventIDs              map[string]bool
	version               uint64
	destructiveApplyCount int
	actions               []protocol.DestinationAction
}

func NewMemoryDestination() *MemoryDestination {
	return &MemoryDestination{
		authority: make(map[string]Authority), effects: make(map[string]string),
		operations: make(map[string]DestructiveResult), eventIDs: make(map[string]bool),
	}
}

func (d *MemoryDestination) SetAuthority(itemID string, authority Authority) error {
	if d == nil || itemID == "" {
		return fmt.Errorf("%w: destination item", protocol.ErrInvalidEvidence)
	}
	if err := authority.validate(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.authority[itemID]; exists {
		return fmt.Errorf("%w: authority already initialized", protocol.ErrInvalidEvidence)
	}
	d.authority[itemID] = authority
	return nil
}

func (d *MemoryDestination) Supersede(itemID string, obsolete, replacement Authority) error {
	if d == nil || itemID == "" {
		return fmt.Errorf("%w: supersession item", protocol.ErrInvalidEvidence)
	}
	if err := obsolete.validate(); err != nil {
		return err
	}
	if err := replacement.validate(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	current, exists := d.authority[itemID]
	if exists && current == replacement {
		return nil
	}
	if !exists || current != obsolete || replacement.Generation <= obsolete.Generation || replacement.CapabilityHash == obsolete.CapabilityHash {
		return fmt.Errorf("%w: atomic supersession", protocol.ErrInvalidEvidence)
	}
	d.authority[itemID] = replacement
	return nil
}

func (d *MemoryDestination) ApplyEffect(request EffectRequest) (protocol.DestinationAction, error) {
	if d == nil || request.EventID == "" || request.ItemID == "" || request.LogicalEffectID == "" || !request.Probe.Valid() {
		return protocol.DestinationAction{}, fmt.Errorf("%w: effect request", protocol.ErrInvalidEvidence)
	}
	if err := request.Authority.validate(); err != nil {
		return protocol.DestinationAction{}, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.eventIDs[request.EventID] {
		return protocol.DestinationAction{}, fmt.Errorf("%w: duplicate destination event", protocol.ErrInvalidEvidence)
	}
	action := protocol.DestinationAction{
		EventID: request.EventID, WorkItemID: request.ItemID, LogicalEffectID: request.LogicalEffectID,
		Generation: request.Authority.Generation, CapabilityHash: request.Authority.CapabilityHash,
	}
	current, exists := d.authority[request.ItemID]
	if request.Probe != protocol.ProbeUnsafe && (!exists || current != request.Authority) {
		action.Decision = protocol.DecisionRejected
		d.recordActionLocked(action)
		return action, nil
	}
	if receipt, exists := d.effects[request.LogicalEffectID]; request.Probe != protocol.ProbeUnsafe && exists {
		action.Decision, action.ReceiptID = protocol.DecisionReconciled, receipt
		d.recordActionLocked(action)
		return action, nil
	}
	action.Decision, action.Applied = protocol.DecisionAccepted, true
	action.ReceiptID = "receipt/effect/" + request.LogicalEffectID
	d.effects[request.LogicalEffectID] = action.ReceiptID
	d.recordActionLocked(action)
	return action, nil
}

func (d *MemoryDestination) ApplyDestructive(request DestructiveRequest) (DestructiveResult, error) {
	if d == nil || request.EventID == "" || request.ItemID == "" || request.OperationID == "" || request.Attempt < 1 || !request.Probe.Valid() {
		return DestructiveResult{}, fmt.Errorf("%w: destructive request", protocol.ErrInvalidEvidence)
	}
	if err := request.Authority.validate(); err != nil {
		return DestructiveResult{}, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.eventIDs[request.EventID] {
		return DestructiveResult{}, fmt.Errorf("%w: duplicate destination event", protocol.ErrInvalidEvidence)
	}
	effectiveOperationID := request.OperationID
	if request.Probe == protocol.ProbeUnsafe {
		effectiveOperationID = fmt.Sprintf("%s/attempt-%d", request.OperationID, request.Attempt)
	}
	current, authorityExists := d.authority[request.ItemID]
	if request.Probe != protocol.ProbeUnsafe && (!authorityExists || current != request.Authority) {
		result := DestructiveResult{OperationID: request.OperationID, Decision: protocol.DecisionRejected, PreviousVersion: d.version, ResultingVersion: d.version}
		d.recordDestructiveActionLocked(request, result)
		return result, nil
	}
	if prior, exists := d.operations[effectiveOperationID]; request.Probe != protocol.ProbeUnsafe && exists {
		result := prior
		result.Decision, result.Applied = protocol.DecisionReconciled, false
		d.recordDestructiveActionLocked(request, result)
		return result, nil
	}
	if request.Probe != protocol.ProbeUnsafe && d.version != request.ExpectedVersion {
		result := DestructiveResult{OperationID: request.OperationID, Decision: protocol.DecisionRejected, PreviousVersion: d.version, ResultingVersion: d.version}
		d.recordDestructiveActionLocked(request, result)
		return result, nil
	}
	previous := d.version
	d.version++
	d.destructiveApplyCount++
	result := DestructiveResult{
		OperationID: request.OperationID, Decision: protocol.DecisionAccepted, Applied: true,
		ReceiptID: "receipt/destructive/" + effectiveOperationID, PreviousVersion: previous, ResultingVersion: d.version,
	}
	d.operations[effectiveOperationID] = result
	d.recordDestructiveActionLocked(request, result)
	return result, nil
}

func (d *MemoryDestination) Snapshot() DestinationSnapshot {
	if d == nil {
		return DestinationSnapshot{}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return DestinationSnapshot{
		Version: d.version, DestructiveApplyCount: d.destructiveApplyCount, Actions: slices.Clone(d.actions),
	}
}

func (d *MemoryDestination) recordDestructiveActionLocked(request DestructiveRequest, result DestructiveResult) {
	action := protocol.DestinationAction{
		EventID: request.EventID, WorkItemID: request.ItemID, LogicalEffectID: request.OperationID,
		ReceiptID: result.ReceiptID, Generation: request.Authority.Generation, CapabilityHash: request.Authority.CapabilityHash,
		Decision: result.Decision, Applied: result.Applied,
	}
	d.recordActionLocked(action)
}

func (d *MemoryDestination) recordActionLocked(action protocol.DestinationAction) {
	d.eventIDs[action.EventID] = true
	d.actions = append(d.actions, action)
}
