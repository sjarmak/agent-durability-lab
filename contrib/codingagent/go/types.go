package codingagent

import (
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const SchemaVersion = "1.0.0"

var (
	ErrInvalidInput            = errors.New("invalid coding-agent command")
	ErrIllegalTransition       = errors.New("illegal coding-agent transition")
	ErrCoordinatorUnauthorized = errors.New("coordinator is not authenticated")
	ErrExecutorNotDiscovered   = errors.New("executor identity lacks authenticated discovery")
	stableIDPattern            = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]*$`)
	reasonCodePattern          = regexp.MustCompile(`^[a-z][a-z0-9_]{0,79}$`)
)

type ID string
type Digest string

func (id ID) valid() bool {
	return len(id) > 0 && len(id) <= 160 && stableIDPattern.MatchString(string(id))
}

func decodeDigest(digest Digest) ([]byte, error) {
	value := string(digest)
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+64 {
		return nil, errors.New("digest must be a lowercase SHA-256 digest")
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	if err != nil {
		return nil, errors.New("digest must be a lowercase SHA-256 digest")
	}
	return decoded, nil
}

func (digest Digest) valid() bool {
	_, err := decodeDigest(digest)
	return err == nil && string(digest) == strings.ToLower(string(digest))
}

type Identity struct {
	SessionID   ID `json:"session_id"`
	TurnID      ID `json:"turn_id"`
	OperationID ID `json:"operation_id"`
}

func (identity Identity) validate() error {
	if !identity.SessionID.valid() || !identity.TurnID.valid() || !identity.OperationID.valid() {
		return fmt.Errorf("%w: session, turn, and operation IDs must be stable IDs", ErrInvalidInput)
	}
	return nil
}

type Operation string

const (
	OperationClaim                Operation = "claim"
	OperationBeginStart           Operation = "begin_start"
	OperationRegister             Operation = "register"
	OperationAttach               Operation = "attach"
	OperationReplace              Operation = "replace"
	OperationObserveProgress      Operation = "observe_progress"
	OperationPublishEffectReceipt Operation = "publish_effect_receipt"
	OperationPublishResult        Operation = "publish_result"
	OperationComplete             Operation = "complete"
	OperationCancel               Operation = "cancel"
	OperationMarkUnresolved       Operation = "mark_unresolved"
	OperationRecordStopDelivery   Operation = "record_stop_delivery"
	OperationAcknowledgeStop      Operation = "acknowledge_stop"
)

func (operation Operation) Valid() bool {
	switch operation {
	case OperationClaim, OperationBeginStart, OperationRegister, OperationAttach,
		OperationReplace, OperationObserveProgress, OperationPublishEffectReceipt,
		OperationPublishResult, OperationComplete, OperationCancel,
		OperationMarkUnresolved, OperationRecordStopDelivery, OperationAcknowledgeStop:
		return true
	default:
		return false
	}
}

func (operation Operation) coordinatorOnly() bool {
	switch operation {
	case OperationClaim, OperationReplace, OperationCancel, OperationMarkUnresolved,
		OperationRecordStopDelivery, OperationAcknowledgeStop:
		return true
	default:
		return false
	}
}

type Lifecycle string

const (
	LifecycleClaimed    Lifecycle = "claimed"
	LifecycleStarting   Lifecycle = "starting"
	LifecycleRunning    Lifecycle = "running"
	LifecycleCompleting Lifecycle = "completing"
	LifecycleSucceeded  Lifecycle = "succeeded"
	LifecycleCanceled   Lifecycle = "canceled"
	LifecycleUnresolved Lifecycle = "unresolved"
)

func (lifecycle Lifecycle) terminal() bool {
	return lifecycle == LifecycleSucceeded || lifecycle == LifecycleCanceled || lifecycle == LifecycleUnresolved
}

type AuthorityStatus string

const (
	AuthorityCurrent AuthorityStatus = "current"
	AuthorityRevoked AuthorityStatus = "revoked"
)

type TurnState struct {
	Lifecycle             Lifecycle       `json:"lifecycle"`
	Generation            uint64          `json:"generation"`
	OwnerCapabilityDigest Digest          `json:"owner_capability_digest"`
	Authority             AuthorityStatus `json:"authority_status"`
}

type ExecutorIdentity struct {
	Kind            string `json:"kind"`
	ProcessID       ID     `json:"process_id,omitempty"`
	StartIdentity   ID     `json:"start_identity,omitempty"`
	Provider        ID     `json:"provider,omitempty"`
	ProviderSession ID     `json:"session_id,omitempty"`
}

func ProcessExecutor(processID, startIdentity ID) ExecutorIdentity {
	return ExecutorIdentity{Kind: "process", ProcessID: processID, StartIdentity: startIdentity}
}

func ProviderExecutor(provider, session ID) ExecutorIdentity {
	return ExecutorIdentity{Kind: "provider", Provider: provider, ProviderSession: session}
}

func (identity ExecutorIdentity) valid() bool {
	switch identity.Kind {
	case "process":
		return identity.ProcessID.valid() && identity.StartIdentity.valid() && identity.Provider == "" && identity.ProviderSession == ""
	case "provider":
		return identity.Provider.valid() && identity.ProviderSession.valid() && identity.ProcessID == "" && identity.StartIdentity == ""
	default:
		return false
	}
}

type DestinationCapability string

const (
	AtomicIdempotencyKey DestinationCapability = "atomic_idempotency_key"
	TransactionalUnique  DestinationCapability = "transactional_unique_effect_identity"
	StableMessageID      DestinationCapability = "stable_message_identity"
	SerializedLookup     DestinationCapability = "serialized_correlation_lookup"
	ConditionalMutation  DestinationCapability = "conditional_versioned_git_mutation"
	ContentAddressed     DestinationCapability = "content_addressed_blob"
	ManualReconciliation DestinationCapability = "manual_reconciliation"
)

func (capability DestinationCapability) valid() bool {
	switch capability {
	case AtomicIdempotencyKey, TransactionalUnique, StableMessageID, SerializedLookup,
		ConditionalMutation, ContentAddressed, ManualReconciliation:
		return true
	default:
		return false
	}
}

type EffectOutcome string

const (
	EffectCommitted  EffectOutcome = "committed"
	EffectReconciled EffectOutcome = "reconciled"
	EffectUnresolved EffectOutcome = "unresolved"
)

type EffectResult struct {
	EffectID             ID
	DestinationNamespace string
	Capability           DestinationCapability
	DestinationReceiptID ID
	Outcome              EffectOutcome
}

func (effect EffectResult) valid() bool {
	return effect.EffectID.valid() && ID(effect.DestinationNamespace).valid() && effect.Capability.valid() &&
		(effect.DestinationReceiptID == "" || len(effect.DestinationReceiptID) <= 512) &&
		(effect.Outcome == EffectCommitted || effect.Outcome == EffectReconciled || effect.Outcome == EffectUnresolved)
}

type CandidateResult struct {
	Hash                Digest
	SystemOfRecordCheck string
}

func (result CandidateResult) valid() bool {
	return result.Hash.valid() && ID(result.SystemOfRecordCheck).valid()
}

type StopTarget struct {
	Generation uint64
	Executor   ExecutorIdentity
}

func (target StopTarget) valid() bool { return target.Generation > 0 && target.Executor.valid() }

type StopStatus string

const (
	StopDelivered    StopStatus = "delivered"
	StopAcknowledged StopStatus = "acknowledged"
)

type Command struct {
	Operation             Operation
	Identity              Identity
	RequestHash           Digest
	ActorGeneration       uint64
	Capability            Capability
	NewCapability         Capability
	CoordinatorAuthorized bool
	ReceiptID             ID
	OccurredAt            time.Time
	Executor              *ExecutorIdentity
	ExecutorDiscovered    bool
	ProgressHash          Digest
	Effect                *EffectResult
	Result                *CandidateResult
	ResultReceiptID       ID
	ReasonCode            string
	Stop                  *StopTarget
	StopStatus            StopStatus
}

func (command Command) AsCoordinator() Command {
	command.CoordinatorAuthorized = true
	return command
}

func (command Command) WithNewCapability(capability Capability) Command {
	command.NewCapability = capability
	return command
}

func (command Command) WithExecutor(identity ExecutorIdentity) Command {
	command.Executor = &identity
	return command
}

func (command Command) WithDiscoveredExecutor(identity ExecutorIdentity) Command {
	command.Executor = &identity
	command.ExecutorDiscovered = true
	return command
}

func (command Command) WithProgress(hash Digest) Command {
	command.ProgressHash = hash
	return command
}

func (command Command) WithEffect(effect EffectResult) Command {
	command.Effect = &effect
	return command
}

func (command Command) WithResult(result CandidateResult) Command {
	command.Result = &result
	return command
}

func (command Command) WithResultReceipt(receiptID ID) Command {
	command.ResultReceiptID = receiptID
	return command
}

func (command Command) WithReason(reason string) Command {
	command.ReasonCode = reason
	return command
}

func (command Command) WithStop(target StopTarget, status StopStatus) Command {
	command.Stop = &target
	command.StopStatus = status
	return command
}

func (command Command) validate() error {
	if !command.Operation.Valid() || command.Identity.validate() != nil || !command.RequestHash.valid() ||
		command.ActorGeneration == 0 || !command.ReceiptID.valid() || command.OccurredAt.IsZero() ||
		command.OccurredAt.Location() != time.UTC {
		return fmt.Errorf("%w: invalid operation metadata", ErrInvalidInput)
	}
	return nil
}
