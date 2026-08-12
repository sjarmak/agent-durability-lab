package codingagent

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

var testTime = time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)

func TestKernelAppliesAllTransitionsAndPreservesOldValues(t *testing.T) {
	owner1 := mustCapability(t)
	owner2 := mustCapability(t)
	kernel := NewKernel()
	original := kernel

	kernel = applyAccepted(t, kernel, command(OperationClaim, "claim", 1, owner1).
		AsCoordinator().WithNewCapability(owner1))
	claimed := kernel
	if _, ok := original.State(); ok {
		t.Fatal("Apply mutated the original kernel")
	}
	kernel = applyAccepted(t, kernel, command(OperationBeginStart, "start", 1, owner1))
	claimedState, _ := claimed.State()
	if claimedState.Lifecycle != LifecycleClaimed {
		t.Fatalf("Apply mutated a non-empty prior kernel: %#v", claimedState)
	}
	identity := ProcessExecutor("pid:412", "procfs:9001")
	kernel, receipt := applyAcceptedReceipt(t, kernel, command(OperationRegister, "register", 1, owner1).WithExecutor(identity))
	registration, ok := receipt.Subject.(RegistrationSubject)
	if !ok || registration.Executor != identity {
		t.Fatalf("registration receipt lost executor: %#v", receipt)
	}
	kernel, receipt = applyAcceptedReceipt(t, kernel, command(OperationAttach, "attach", 1, owner1).WithExecutor(identity))
	attachment, ok := receipt.Subject.(AttachmentSubject)
	if !ok || attachment.Executor != identity {
		t.Fatalf("attachment receipt lost executor: %#v", receipt)
	}
	progressHash := Digest("sha256:" + repeat("8", 64))
	kernel, receipt = applyAcceptedReceipt(t, kernel, command(OperationObserveProgress, "progress", 1, owner1).WithProgress(progressHash))
	progress, ok := receipt.Subject.(ProgressSubject)
	if !ok || progress.Hash != progressHash {
		t.Fatalf("progress receipt lost hash: %#v", receipt)
	}
	effect := EffectResult{
		EffectID: ID("effect:tool-call-7"), DestinationNamespace: "destination:issues",
		Capability: AtomicIdempotencyKey, DestinationReceiptID: ID("destination-receipt:7"),
		Outcome: EffectCommitted,
	}
	kernel, receipt = applyAcceptedReceipt(t, kernel, command(OperationPublishEffectReceipt, "effect", 1, owner1).WithEffect(effect))
	effectSubject, ok := receipt.Subject.(EffectSubject)
	if !ok || effectSubject.Result != effect {
		t.Fatalf("effect receipt lost subject: %#v", receipt)
	}
	result := CandidateResult{Hash: Digest("sha256:" + repeat("9", 64)), SystemOfRecordCheck: "check:destination-state"}
	kernel, receipt = applyAcceptedReceipt(t, kernel, command(OperationPublishResult, "result", 1, owner1).WithResult(result))
	resultSubject, ok := receipt.Subject.(ResultSubject)
	if !ok || resultSubject.Result != result {
		t.Fatalf("result receipt lost subject: %#v", receipt)
	}
	kernel, receipt = applyAcceptedReceipt(t, kernel, command(OperationComplete, "complete", 1, owner1).WithResultReceipt(ID("receipt:result")))
	completion, ok := receipt.Subject.(CompletionSubject)
	if !ok || completion.ResultReceiptID != "receipt:result" {
		t.Fatalf("completion receipt lost result link: %#v", receipt)
	}
	state, ok := kernel.State()
	if !ok || state.Lifecycle != LifecycleSucceeded || state.Authority != AuthorityRevoked {
		t.Fatalf("unexpected terminal state: %#v, %v", state, ok)
	}

	replaced := NewKernel()
	replaced = applyAccepted(t, replaced, command(OperationClaim, "claim-r", 1, owner1).AsCoordinator().WithNewCapability(owner1))
	replaced = applyAccepted(t, replaced, command(OperationBeginStart, "start-r", 1, owner1))
	replaced = applyAccepted(t, replaced, command(OperationRegister, "register-r", 1, owner1).WithExecutor(identity))
	replaced, receipt = applyAcceptedReceipt(t, replaced, command(OperationReplace, "replace", 1, Capability{}).
		AsCoordinator().WithNewCapability(owner2))
	replacement, ok := receipt.Subject.(ReplacementSubject)
	if !ok || replacement.PreviousGeneration != 1 || replacement.ReplacementGeneration != 2 {
		t.Fatalf("replacement receipt lost generations: %#v", receipt)
	}
	oldStop := StopTarget{Generation: 1, Executor: identity}
	replaced, receipt = applyAcceptedReceipt(t, replaced, command(OperationRecordStopDelivery, "stop-old", 2, Capability{}).
		AsCoordinator().WithStop(oldStop, StopDelivered))
	oldStopSubject, ok := receipt.Subject.(StopSubject)
	if !ok || oldStopSubject.Target != oldStop || oldStopSubject.Status != StopDelivered {
		t.Fatalf("old-generation stop receipt lost target: %#v", receipt)
	}
	identity2 := ProcessExecutor("pid:512", "procfs:9002")
	replaced = applyAccepted(t, replaced, command(OperationRegister, "register-r2", 2, owner2).WithExecutor(identity2))
	replaced, receipt = applyAcceptedReceipt(t, replaced, command(OperationCancel, "cancel", 2, Capability{}).
		AsCoordinator().WithReason("operator_canceled"))
	cancellation, ok := receipt.Subject.(CancellationSubject)
	if !ok || cancellation.ReasonCode != "operator_canceled" {
		t.Fatalf("cancellation receipt lost reason: %#v", receipt)
	}
	currentStop := StopTarget{Generation: 2, Executor: identity2}
	replaced, receipt = applyAcceptedReceipt(t, replaced, command(OperationRecordStopDelivery, "stop", 2, Capability{}).
		AsCoordinator().WithStop(currentStop, StopDelivered))
	stopDelivery, ok := receipt.Subject.(StopSubject)
	if !ok || stopDelivery.Target != currentStop || stopDelivery.Status != StopDelivered {
		t.Fatalf("stop receipt lost target: %#v", receipt)
	}
	_, receipt = applyAcceptedReceipt(t, replaced, command(OperationAcknowledgeStop, "ack", 2, Capability{}).
		AsCoordinator().WithStop(currentStop, StopAcknowledged))
	stopAcknowledgement, ok := receipt.Subject.(StopSubject)
	if !ok || stopAcknowledgement.Target != currentStop || stopAcknowledgement.Status != StopAcknowledged {
		t.Fatalf("stop acknowledgement lost target: %#v", receipt)
	}

	unresolved := NewKernel()
	unresolved = applyAccepted(t, unresolved, command(OperationClaim, "claim-u", 1, owner1).AsCoordinator().WithNewCapability(owner1))
	unresolved, receipt = applyAcceptedReceipt(t, unresolved, command(OperationMarkUnresolved, "unresolved", 1, Capability{}).
		AsCoordinator().WithReason("provider_effect_ambiguous"))
	unresolvedSubject, ok := receipt.Subject.(UnresolvedSubject)
	if !ok || unresolvedSubject.ReasonCode != "provider_effect_ambiguous" {
		t.Fatalf("unresolved receipt lost reason: %#v", receipt)
	}
	state, _ = unresolved.State()
	if state.Lifecycle != LifecycleUnresolved || state.Authority != AuthorityRevoked {
		t.Fatalf("unexpected unresolved state: %#v", state)
	}
}

func TestKernelReplaysAndConflictsWithoutChangingState(t *testing.T) {
	owner := mustCapability(t)
	kernel := NewKernel()
	claim := command(OperationClaim, "same-operation", 1, owner).AsCoordinator().WithNewCapability(owner)
	var first Outcome
	var err error
	kernel, first, err = kernel.Apply(context.Background(), claim)
	if err != nil || first.Disposition != DispositionAccepted {
		t.Fatalf("first apply: %#v, %v", first, err)
	}
	replayedKernel, replayed, err := kernel.Apply(context.Background(), claim)
	if err != nil || replayed.Disposition != DispositionReplayed || replayed.Receipt == nil || first.Receipt == nil || *replayed.Receipt != *first.Receipt {
		t.Fatalf("replay: %#v, %v", replayed, err)
	}
	if replayedKernel != kernel {
		t.Fatal("replay changed kernel value")
	}
	changed := claim
	changed.RequestHash = Digest("sha256:" + repeat("3", 64))
	conflictKernel, conflict, err := kernel.Apply(context.Background(), changed)
	if err != nil || conflict.Disposition != DispositionConflict || conflict.Rejection == nil {
		t.Fatalf("conflict: %#v, %v", conflict, err)
	}
	if conflictKernel != kernel {
		t.Fatal("conflict changed kernel value")
	}
	crossIdentity := claim
	crossIdentity.Identity.SessionID = "session:other"
	if _, _, err := kernel.Apply(context.Background(), crossIdentity); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("cross-turn replay was not rejected: %v", err)
	}
	unauthorizedReplay := claim
	unauthorizedReplay.CoordinatorAuthorized = false
	if _, _, err := kernel.Apply(context.Background(), unauthorizedReplay); !errors.Is(err, ErrCoordinatorUnauthorized) {
		t.Fatalf("unauthorized coordinator replay: %v", err)
	}
}

func TestExecutorReplayRequiresOriginalAuthorityAndExactContent(t *testing.T) {
	owner := mustCapability(t)
	kernel := NewKernel()
	kernel = applyAccepted(t, kernel, command(OperationClaim, "claim-auth", 1, owner).AsCoordinator().WithNewCapability(owner))
	start := command(OperationBeginStart, "start-auth", 1, owner)
	kernel = applyAccepted(t, kernel, start)
	withoutCapability := start
	withoutCapability.Capability = Capability{}
	if _, outcome, err := kernel.Apply(context.Background(), withoutCapability); err != nil || outcome.Disposition != DispositionRevoked {
		t.Fatalf("unauthorized executor replay: %#v, %v", outcome, err)
	}
	changed := start
	changed.Operation = OperationAttach
	changed.Executor = ptr(ProcessExecutor("pid:other", "procfs:other"))
	if _, outcome, err := kernel.Apply(context.Background(), changed); err != nil || outcome.Disposition != DispositionConflict {
		t.Fatalf("changed-content replay: %#v, %v", outcome, err)
	}
}

func TestHistoricalOwnerCanReadOnlyItsExactReplayReceipt(t *testing.T) {
	owner1 := mustCapability(t)
	owner2 := mustCapability(t)
	kernel := NewKernel()
	kernel = applyAccepted(t, kernel, command(OperationClaim, "claim-history", 1, owner1).AsCoordinator().WithNewCapability(owner1))
	start := command(OperationBeginStart, "start-history", 1, owner1)
	kernel = applyAccepted(t, kernel, start)
	kernel = applyAccepted(t, kernel, command(OperationReplace, "replace-history", 1, Capability{}).AsCoordinator().WithNewCapability(owner2))
	unchanged, replay, err := kernel.Apply(context.Background(), start)
	if err != nil || replay.Disposition != DispositionReplayed || unchanged != kernel {
		t.Fatalf("exact historical replay: %#v, %v", replay, err)
	}
	newOperation := command(OperationBeginStart, "new-history", 1, owner1)
	if _, rejected, err := kernel.Apply(context.Background(), newOperation); err != nil || rejected.Disposition != DispositionStale {
		t.Fatalf("historical owner started new operation: %#v, %v", rejected, err)
	}
}

func TestRegisterCannotReplaceAttachedExecutor(t *testing.T) {
	owner := mustCapability(t)
	kernel := NewKernel()
	kernel = applyAccepted(t, kernel, command(OperationClaim, "claim-a", 1, owner).AsCoordinator().WithNewCapability(owner))
	kernel = applyAccepted(t, kernel, command(OperationBeginStart, "start-a", 1, owner))
	if _, _, err := kernel.Apply(context.Background(), command(OperationAttach, "attach-unverified", 1, owner).WithExecutor(ProcessExecutor("pid:1", "procfs:1"))); !errors.Is(err, ErrExecutorNotDiscovered) {
		t.Fatalf("attach accepted unverified discovery: %v", err)
	}
	kernel = applyAccepted(t, kernel, command(OperationAttach, "attach-a", 1, owner).WithDiscoveredExecutor(ProcessExecutor("pid:1", "procfs:1")))
	unchanged, outcome, err := kernel.Apply(context.Background(), command(OperationRegister, "register-a", 1, owner).WithExecutor(ProcessExecutor("pid:2", "procfs:2")))
	if err != nil || outcome.Disposition != DispositionConflict || unchanged != kernel {
		t.Fatalf("register replaced attached executor: %#v, %v", outcome, err)
	}
}

func TestWrongCurrentCapabilityIsRevoked(t *testing.T) {
	owner := mustCapability(t)
	kernel := NewKernel()
	kernel = applyAccepted(t, kernel, command(OperationClaim, "claim-w", 1, owner).AsCoordinator().WithNewCapability(owner))
	_, outcome, err := kernel.Apply(context.Background(), command(OperationBeginStart, "start-w", 1, mustCapability(t)))
	if err != nil || outcome.Disposition != DispositionRevoked {
		t.Fatalf("wrong current capability: %#v, %v", outcome, err)
	}
}

func TestKernelRejectsStaleRevokedCanceledAndIllegalTransitions(t *testing.T) {
	owner1 := mustCapability(t)
	owner2 := mustCapability(t)
	kernel := NewKernel()
	kernel = applyAccepted(t, kernel, command(OperationClaim, "claim", 1, owner1).AsCoordinator().WithNewCapability(owner1))
	if _, _, err := kernel.Apply(context.Background(), command(OperationRegister, "illegal", 1, owner1).WithExecutor(ProcessExecutor("pid:1", "procfs:1"))); !errors.Is(err, ErrIllegalTransition) {
		t.Fatalf("register from claimed: %v", err)
	}
	kernel = applyAccepted(t, kernel, command(OperationReplace, "replace", 1, Capability{}).AsCoordinator().WithNewCapability(owner2))
	unchanged, stale, err := kernel.Apply(context.Background(), command(OperationAttach, "stale", 1, owner1).WithExecutor(ProcessExecutor("pid:1", "procfs:1")))
	if err != nil || stale.Disposition != DispositionStale || unchanged != kernel {
		t.Fatalf("stale owner: %#v, %v", stale, err)
	}
	kernel = applyAccepted(t, kernel, command(OperationCancel, "cancel", 2, Capability{}).AsCoordinator().WithReason("operator_canceled"))
	_, canceled, err := kernel.Apply(context.Background(), command(OperationAttach, "canceled", 2, owner2).WithExecutor(ProcessExecutor("pid:1", "procfs:1")))
	if err != nil || canceled.Disposition != DispositionCanceled {
		t.Fatalf("canceled owner: %#v, %v", canceled, err)
	}
	_, revoked, err := kernel.Apply(context.Background(), command(OperationAttach, "revoked", 2, mustCapability(t)).WithExecutor(ProcessExecutor("pid:1", "procfs:1")))
	if err != nil || revoked.Disposition != DispositionCanceled {
		t.Fatalf("terminal revocation: %#v, %v", revoked, err)
	}
}

func TestKernelValidatesBoundaryAndContext(t *testing.T) {
	owner := mustCapability(t)
	kernel := NewKernel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := kernel.Apply(ctx, command(OperationClaim, "claim", 1, owner).AsCoordinator().WithNewCapability(owner)); !errors.Is(err, context.Canceled) {
		t.Fatalf("context cancellation: %v", err)
	}
	unauthorized := command(OperationClaim, "claim", 1, owner).WithNewCapability(owner)
	if _, _, err := kernel.Apply(context.Background(), unauthorized); !errors.Is(err, ErrCoordinatorUnauthorized) {
		t.Fatalf("unauthorized coordinator: %v", err)
	}
	bad := command(OperationClaim, "claim", 1, owner).AsCoordinator().WithNewCapability(owner)
	bad.Identity.SessionID = ""
	if _, _, err := kernel.Apply(context.Background(), bad); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("invalid identity: %v", err)
	}
}

func TestKernelSnapshotSupportsConcurrentIndependentDecisions(t *testing.T) {
	owner := mustCapability(t)
	kernel := NewKernel()
	kernel = applyAccepted(t, kernel, command(OperationClaim, "claim-c", 1, owner).AsCoordinator().WithNewCapability(owner))
	kernel = applyAccepted(t, kernel, command(OperationBeginStart, "start-c", 1, owner))
	kernel = applyAccepted(t, kernel, command(OperationRegister, "register-c", 1, owner).WithExecutor(ProcessExecutor("pid:1", "procfs:1")))

	var wait sync.WaitGroup
	errors := make(chan error, 16)
	for index := range 16 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			request := command(OperationObserveProgress, fmt.Sprintf("progress-c-%d", index), 1, owner).
				WithProgress(Digest("sha256:" + repeat(fmt.Sprintf("%x", index), 64)))
			_, outcome, err := kernel.Apply(context.Background(), request)
			if err != nil || outcome.Disposition != DispositionAccepted {
				errors <- fmt.Errorf("decision %d: %#v, %w", index, outcome, err)
			}
		}()
	}
	wait.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
}

func command(operation Operation, suffix string, generation uint64, capability Capability) Command {
	return Command{
		Operation:   operation,
		Identity:    Identity{SessionID: ID("session:test"), TurnID: ID("turn:test"), OperationID: ID("operation:" + suffix)},
		RequestHash: Digest("sha256:" + repeat("2", 64)), ActorGeneration: generation,
		Capability: capability, ReceiptID: ID("receipt:" + suffix), OccurredAt: testTime,
	}
}

func applyAccepted(t *testing.T, kernel Kernel, command Command) Kernel {
	t.Helper()
	next, outcome, err := kernel.Apply(context.Background(), command)
	if err != nil || outcome.Disposition != DispositionAccepted || outcome.Receipt == nil {
		t.Fatalf("apply %s: outcome=%#v err=%v", command.Operation, outcome, err)
	}
	return next
}

func applyAcceptedReceipt(t *testing.T, kernel Kernel, command Command) (Kernel, Receipt) {
	t.Helper()
	next, outcome, err := kernel.Apply(context.Background(), command)
	if err != nil || outcome.Disposition != DispositionAccepted || outcome.Receipt == nil {
		t.Fatalf("apply %s: outcome=%#v err=%v", command.Operation, outcome, err)
	}
	return next, *outcome.Receipt
}

func ptr[T any](value T) *T { return &value }

func mustCapability(t *testing.T) Capability {
	t.Helper()
	capability, err := NewCapability()
	if err != nil {
		t.Fatal(err)
	}
	return capability
}

func repeat(value string, count int) string {
	result := ""
	for range count {
		result += value
	}
	return result
}
