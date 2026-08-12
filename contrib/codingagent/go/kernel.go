package codingagent

import (
	"context"
	"fmt"
)

type Disposition string

const (
	DispositionAccepted Disposition = "accepted"
	DispositionReplayed Disposition = "replayed"
	DispositionStale    Disposition = "stale"
	DispositionRevoked  Disposition = "revoked"
	DispositionCanceled Disposition = "canceled"
	DispositionConflict Disposition = "conflict"
)

type Receipt struct {
	ID          ID
	Operation   Operation
	RequestHash Digest
	RecordedAt  string
	Subject     ReceiptSubject
}

type ReceiptSubject interface{ receiptSubject() }

type ClaimSubject struct{}
type StartSubject struct{}
type RegistrationSubject struct{ Executor ExecutorIdentity }
type AttachmentSubject struct{ Executor ExecutorIdentity }
type ReplacementSubject struct{ PreviousGeneration, ReplacementGeneration uint64 }
type ProgressSubject struct{ Hash Digest }
type EffectSubject struct{ Result EffectResult }
type ResultSubject struct{ Result CandidateResult }
type CompletionSubject struct{ ResultReceiptID ID }
type CancellationSubject struct{ ReasonCode string }
type UnresolvedSubject struct{ ReasonCode string }
type StopSubject struct {
	Target StopTarget
	Status StopStatus
}

func (ClaimSubject) receiptSubject()        {}
func (StartSubject) receiptSubject()        {}
func (RegistrationSubject) receiptSubject() {}
func (AttachmentSubject) receiptSubject()   {}
func (ReplacementSubject) receiptSubject()  {}
func (ProgressSubject) receiptSubject()     {}
func (EffectSubject) receiptSubject()       {}
func (ResultSubject) receiptSubject()       {}
func (CompletionSubject) receiptSubject()   {}
func (CancellationSubject) receiptSubject() {}
func (UnresolvedSubject) receiptSubject()   {}
func (StopSubject) receiptSubject()         {}

type Rejection struct {
	Type              Disposition
	RequestHash       Digest
	ReasonCode        string
	CurrentGeneration uint64
}

type Outcome struct {
	Disposition Disposition
	Receipt     *Receipt
	Rejection   *Rejection
}

type storedOperation struct {
	hash            Digest
	fingerprint     commandFingerprint
	actorGeneration uint64
	ownerDigest     Digest
	coordinator     bool
	receipt         Receipt
}

type commandFingerprint struct {
	operation          Operation
	actorGeneration    uint64
	newOwnerDigest     Digest
	executor           ExecutorIdentity
	hasExecutor        bool
	executorDiscovered bool
	progressHash       Digest
	effect             EffectResult
	hasEffect          bool
	result             CandidateResult
	hasResult          bool
	resultReceiptID    ID
	reasonCode         string
	stop               StopTarget
	hasStop            bool
	stopStatus         StopStatus
}

type kernelData struct {
	identity         Identity
	state            TurnState
	executor         ExecutorIdentity
	revokedExecutors map[uint64]ExecutorIdentity
	resultReceiptID  ID
	operations       map[ID]storedOperation
	effects          map[ID]struct{}
}

// Kernel is an immutable transition value. Apply returns a new Kernel only
// when a command is accepted; replay and rejection return the original value.
type Kernel struct{ data *kernelData }

func NewKernel() Kernel { return Kernel{} }

func (kernel Kernel) State() (TurnState, bool) {
	if kernel.data == nil {
		return TurnState{}, false
	}
	return kernel.data.state, true
}

func (kernel Kernel) Apply(ctx context.Context, command Command) (Kernel, Outcome, error) {
	if err := ctx.Err(); err != nil {
		return kernel, Outcome{}, err
	}
	if err := command.validate(); err != nil {
		return kernel, Outcome{}, err
	}
	if kernel.data != nil && (command.Identity.SessionID != kernel.data.identity.SessionID || command.Identity.TurnID != kernel.data.identity.TurnID) {
		return kernel, Outcome{}, fmt.Errorf("%w: command identity does not name this turn", ErrInvalidInput)
	}
	if stored, ok := kernel.operation(command.Identity.OperationID); ok {
		if stored.coordinator {
			if !command.CoordinatorAuthorized {
				return kernel, Outcome{}, ErrCoordinatorUnauthorized
			}
		} else if command.ActorGeneration != stored.actorGeneration {
			return kernel, rejected(DispositionStale, command, kernel.generation(), "generation_superseded"), nil
		} else if !command.Capability.Matches(stored.ownerDigest) {
			return kernel, rejected(DispositionRevoked, command, kernel.generation(), "capability_not_current"), nil
		}
		if stored.hash == command.RequestHash && stored.fingerprint == fingerprint(command) {
			receipt := stored.receipt
			return kernel, Outcome{Disposition: DispositionReplayed, Receipt: &receipt}, nil
		}
		return kernel, rejected(DispositionConflict, command, kernel.generation(), "operation_request_changed"), nil
	}
	if command.Operation.coordinatorOnly() && !command.CoordinatorAuthorized {
		return kernel, Outcome{}, ErrCoordinatorUnauthorized
	}
	if kernel.data == nil {
		if command.Operation != OperationClaim {
			return kernel, Outcome{}, fmt.Errorf("%w: %s requires an existing turn", ErrIllegalTransition, command.Operation)
		}
		return kernel.claim(command)
	}
	if command.Operation == OperationClaim {
		return kernel, Outcome{}, fmt.Errorf("%w: turn is already claimed", ErrIllegalTransition)
	}
	if command.Operation.coordinatorOnly() {
		if command.ActorGeneration != kernel.data.state.Generation {
			return kernel, rejected(DispositionStale, command, kernel.data.state.Generation, "generation_superseded"), nil
		}
	} else if rejection := kernel.authorizeExecutor(command); rejection != nil {
		return kernel, *rejection, nil
	}
	return kernel.applyAuthorized(command)
}

func (kernel Kernel) operation(id ID) (storedOperation, bool) {
	if kernel.data == nil {
		return storedOperation{}, false
	}
	operation, ok := kernel.data.operations[id]
	return operation, ok
}

func (kernel Kernel) generation() uint64 {
	if kernel.data == nil {
		return 0
	}
	return kernel.data.state.Generation
}

func (kernel Kernel) claim(command Command) (Kernel, Outcome, error) {
	if command.ActorGeneration != 1 || !command.NewCapability.valid {
		return kernel, Outcome{}, fmt.Errorf("%w: claim requires generation 1 and a new capability", ErrInvalidInput)
	}
	state := TurnState{Lifecycle: LifecycleClaimed, Generation: 1, OwnerCapabilityDigest: command.NewCapability.Digest(), Authority: AuthorityCurrent}
	data := &kernelData{
		identity: command.Identity, state: state, operations: make(map[ID]storedOperation),
		effects: make(map[ID]struct{}), revokedExecutors: make(map[uint64]ExecutorIdentity),
	}
	return accept(Kernel{data: data}, command, Receipt{Operation: command.Operation, Subject: ClaimSubject{}})
}

func (kernel Kernel) authorizeExecutor(command Command) *Outcome {
	state := kernel.data.state
	if command.ActorGeneration != state.Generation {
		outcome := rejected(DispositionStale, command, state.Generation, "generation_superseded")
		return &outcome
	}
	if state.Authority == AuthorityRevoked {
		disposition := DispositionRevoked
		reason := "authority_revoked"
		if state.Lifecycle == LifecycleCanceled {
			disposition = DispositionCanceled
			reason = "turn_canceled"
		}
		outcome := rejected(disposition, command, state.Generation, reason)
		return &outcome
	}
	if !command.Capability.Matches(state.OwnerCapabilityDigest) {
		outcome := rejected(DispositionRevoked, command, state.Generation, "capability_not_current")
		return &outcome
	}
	return nil
}

func (kernel Kernel) applyAuthorized(command Command) (Kernel, Outcome, error) {
	data := kernel.clone()
	state := data.state
	receipt := Receipt{Operation: command.Operation}
	switch command.Operation {
	case OperationBeginStart:
		if state.Lifecycle != LifecycleClaimed {
			return kernel, Outcome{}, illegal(command, state)
		}
		data.state.Lifecycle = LifecycleStarting
		receipt.Subject = StartSubject{}
	case OperationRegister:
		if state.Lifecycle != LifecycleStarting || command.Executor == nil || !command.Executor.valid() {
			return kernel, Outcome{}, illegal(command, state)
		}
		if data.executor.valid() && data.executor != *command.Executor {
			return kernel, rejected(DispositionConflict, command, state.Generation, "executor_identity_conflict"), nil
		}
		data.state.Lifecycle = LifecycleRunning
		data.executor = *command.Executor
		receipt.Subject = RegistrationSubject{Executor: *command.Executor}
	case OperationAttach:
		if (state.Lifecycle != LifecycleStarting && state.Lifecycle != LifecycleRunning && state.Lifecycle != LifecycleCompleting) ||
			command.Executor == nil || !command.Executor.valid() || (data.executor.valid() && data.executor != *command.Executor) {
			return kernel, Outcome{}, illegal(command, state)
		}
		if !data.executor.valid() && !command.ExecutorDiscovered {
			return kernel, Outcome{}, ErrExecutorNotDiscovered
		}
		data.executor = *command.Executor
		receipt.Subject = AttachmentSubject{Executor: *command.Executor}
	case OperationReplace:
		if state.Lifecycle.terminal() || !command.NewCapability.valid {
			return kernel, Outcome{}, illegal(command, state)
		}
		if data.executor.valid() {
			data.revokedExecutors[state.Generation] = data.executor
		}
		data.state = TurnState{Lifecycle: LifecycleStarting, Generation: state.Generation + 1, OwnerCapabilityDigest: command.NewCapability.Digest(), Authority: AuthorityCurrent}
		data.executor = ExecutorIdentity{}
		data.resultReceiptID = ""
		receipt.Subject = ReplacementSubject{PreviousGeneration: state.Generation, ReplacementGeneration: state.Generation + 1}
	case OperationObserveProgress:
		if state.Lifecycle != LifecycleRunning || !command.ProgressHash.valid() {
			return kernel, Outcome{}, illegal(command, state)
		}
		receipt.Subject = ProgressSubject{Hash: command.ProgressHash}
	case OperationPublishEffectReceipt:
		if state.Lifecycle != LifecycleRunning || command.Effect == nil || !command.Effect.valid() {
			return kernel, Outcome{}, illegal(command, state)
		}
		if _, exists := data.effects[command.Effect.EffectID]; exists {
			return kernel, rejected(DispositionConflict, command, state.Generation, "effect_id_already_recorded"), nil
		}
		data.effects[command.Effect.EffectID] = struct{}{}
		receipt.Subject = EffectSubject{Result: *command.Effect}
	case OperationPublishResult:
		if state.Lifecycle != LifecycleRunning || command.Result == nil || !command.Result.valid() {
			return kernel, Outcome{}, illegal(command, state)
		}
		data.state.Lifecycle = LifecycleCompleting
		data.resultReceiptID = command.ReceiptID
		receipt.Subject = ResultSubject{Result: *command.Result}
	case OperationComplete:
		if state.Lifecycle != LifecycleCompleting || command.ResultReceiptID != data.resultReceiptID {
			return kernel, Outcome{}, illegal(command, state)
		}
		data.state.Lifecycle = LifecycleSucceeded
		data.state.Authority = AuthorityRevoked
		receipt.Subject = CompletionSubject{ResultReceiptID: command.ResultReceiptID}
	case OperationCancel:
		if state.Lifecycle.terminal() || !ID(command.ReasonCode).valid() {
			return kernel, Outcome{}, illegal(command, state)
		}
		data.state.Lifecycle = LifecycleCanceled
		data.state.Authority = AuthorityRevoked
		receipt.Subject = CancellationSubject{ReasonCode: command.ReasonCode}
	case OperationMarkUnresolved:
		if state.Lifecycle.terminal() || !ID(command.ReasonCode).valid() {
			return kernel, Outcome{}, illegal(command, state)
		}
		data.state.Lifecycle = LifecycleUnresolved
		data.state.Authority = AuthorityRevoked
		receipt.Subject = UnresolvedSubject{ReasonCode: command.ReasonCode}
	case OperationRecordStopDelivery, OperationAcknowledgeStop:
		if command.Stop == nil || !command.Stop.valid() || !data.stopTargetIsRevoked(*command.Stop) {
			return kernel, Outcome{}, illegal(command, state)
		}
		want := StopDelivered
		if command.Operation == OperationAcknowledgeStop {
			want = StopAcknowledged
		}
		if command.StopStatus != want {
			return kernel, Outcome{}, illegal(command, state)
		}
		receipt.Subject = StopSubject{Target: *command.Stop, Status: command.StopStatus}
	default:
		return kernel, Outcome{}, fmt.Errorf("%w: unsupported operation %q", ErrInvalidInput, command.Operation)
	}
	return accept(Kernel{data: data}, command, receipt)
}

func (kernel Kernel) clone() *kernelData {
	data := *kernel.data
	data.operations = make(map[ID]storedOperation, len(kernel.data.operations)+1)
	for id, operation := range kernel.data.operations {
		data.operations[id] = operation
	}
	data.effects = make(map[ID]struct{}, len(kernel.data.effects)+1)
	for id := range kernel.data.effects {
		data.effects[id] = struct{}{}
	}
	data.revokedExecutors = make(map[uint64]ExecutorIdentity, len(kernel.data.revokedExecutors)+1)
	for generation, executor := range kernel.data.revokedExecutors {
		data.revokedExecutors[generation] = executor
	}
	return &data
}

func (data *kernelData) stopTargetIsRevoked(target StopTarget) bool {
	if target.Generation == data.state.Generation {
		return data.state.Authority == AuthorityRevoked && data.executor.valid() && data.executor == target.Executor
	}
	executor, ok := data.revokedExecutors[target.Generation]
	return ok && executor == target.Executor
}

func accept(kernel Kernel, command Command, receipt Receipt) (Kernel, Outcome, error) {
	receipt.ID = command.ReceiptID
	receipt.RequestHash = command.RequestHash
	receipt.RecordedAt = command.OccurredAt.Format("2006-01-02T15:04:05.999999999Z07:00")
	kernel.data.operations[command.Identity.OperationID] = storedOperation{
		hash: command.RequestHash, fingerprint: fingerprint(command), actorGeneration: command.ActorGeneration,
		ownerDigest: command.Capability.Digest(), coordinator: command.Operation.coordinatorOnly(), receipt: receipt,
	}
	copy := receipt
	return kernel, Outcome{Disposition: DispositionAccepted, Receipt: &copy}, nil
}

func fingerprint(command Command) commandFingerprint {
	result := commandFingerprint{
		operation: command.Operation, actorGeneration: command.ActorGeneration,
		newOwnerDigest: command.NewCapability.Digest(), progressHash: command.ProgressHash,
		resultReceiptID: command.ResultReceiptID, reasonCode: command.ReasonCode, stopStatus: command.StopStatus,
		executorDiscovered: command.ExecutorDiscovered,
	}
	if command.Executor != nil {
		result.executor, result.hasExecutor = *command.Executor, true
	}
	if command.Effect != nil {
		result.effect, result.hasEffect = *command.Effect, true
	}
	if command.Result != nil {
		result.result, result.hasResult = *command.Result, true
	}
	if command.Stop != nil {
		result.stop, result.hasStop = *command.Stop, true
	}
	return result
}

func rejected(disposition Disposition, command Command, generation uint64, reason string) Outcome {
	return Outcome{Disposition: disposition, Rejection: &Rejection{
		Type: disposition, RequestHash: command.RequestHash, ReasonCode: reason, CurrentGeneration: generation,
	}}
}

func illegal(command Command, state TurnState) error {
	return fmt.Errorf("%w: cannot %s from %s/%s", ErrIllegalTransition, command.Operation, state.Lifecycle, state.Authority)
}
